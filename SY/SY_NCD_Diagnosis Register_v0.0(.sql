WITH initial AS (
    SELECT 
        patient_id, encounter_id AS initial_encounter_id, visit_location AS initial_visit_location,
        date AS initial_visit_date, DENSE_RANK () OVER (PARTITION BY patient_id ORDER BY date) AS initial_visit_order,
        LEAD (date) OVER (PARTITION BY patient_id ORDER BY date) AS next_initial_visit_date
    FROM "public"."ncd_initial_visit"
),
cohort AS (
    SELECT
        i.patient_id, i.initial_encounter_id, i.initial_visit_location, i.initial_visit_date,
        CASE WHEN i.initial_visit_order > 1 THEN 'Yes' END readmission,
        d.encounter_id AS discharge_encounter_id, d.discharge_date, d.patient_outcome
    FROM initial i
    LEFT JOIN (
        SELECT 
            patient_id, encounter_id, COALESCE(discharge_date::date, date::date) AS discharge_date, patient_outcome
        FROM "public"."ncd_discharge_visit"
    ) d 
    ON i.patient_id = d.patient_id 
    AND d.discharge_date >= i.initial_visit_date 
    AND (d.discharge_date < i.next_initial_visit_date OR i.next_initial_visit_date IS NULL)
),
cohort_diagnosis AS (
    SELECT
        c.patient_id, c.initial_encounter_id, d.date_created, d.diagnosis
    FROM public.diagnosis d
    LEFT JOIN cohort c 
    ON d.patient_id = c.patient_id 
    AND c.initial_visit_date <= d.date_created 
    AND COALESCE(c.discharge_date::date, CURRENT_DATE) >= d.date_created
),
cohort_diagnosis_last AS (
    SELECT
        patient_id, initial_encounter_id, diagnosis, date_created
    FROM (
        SELECT
            cdg.*, ROW_NUMBER() OVER (PARTITION BY patient_id, initial_encounter_id, diagnosis_group ORDER BY date_created DESC) AS rn
        FROM (
            SELECT
                cd.*,
                CASE
                    WHEN diagnosis IN ('Chronic kidney disease', 'Cardiovascular disease', 'Asthma', 'Chronic obstructive pulmonary disease', 'Hypertension', 'Other') THEN 'Group1'
                    WHEN diagnosis IN ('Diabetes mellitus, type 1', 'Diabetes mellitus, type 2') THEN 'Group2'
                    WHEN diagnosis IN ('Focal epilepsy', 'Generalised epilepsy', 'Unclassified epilepsy') THEN 'Group3'
                    WHEN diagnosis IN ('Hypothyroidism', 'Hyperthyroidism') THEN 'Group4'
                    ELSE 'Other'
                END AS diagnosis_group
            FROM cohort_diagnosis cd
        ) cdg
    ) foo
    WHERE diagnosis IS NOT NULL
),
Last_Visit_Location AS (
    SELECT DISTINCT ON (combined.patient_id)
        combined.patient_id, combined.visit_location, encounter_id
    FROM (
        SELECT 
            ncd_disc.patient_id, ncd_disc.visit_location, ncd_disc.date, encounter_id
        FROM public.ncd_discharge_visit ncd_disc
        WHERE ncd_disc.visit_location IS NOT NULL
        UNION ALL
        SELECT 
            ncd_fup.patient_id, ncd_fup.visit_location, ncd_fup.date, encounter_id
        FROM public.ncd_follow_up_visit ncd_fup
        WHERE ncd_fup.visit_location IS NOT NULL
        UNION ALL
        SELECT 
            ncd_init.patient_id, ncd_init.visit_location, ncd_init.date, encounter_id
        FROM public.ncd_initial_visit ncd_init
        WHERE ncd_init.visit_location IS NOT NULL
    ) AS combined
    ORDER BY combined.patient_id, combined.date, encounter_id DESC
),
Aggregated_Diagnoses AS (
    SELECT 
        diag.patient_id, initial_encounter_id,
        STRING_AGG(diag.diagnosis, ', ') AS aggregated_diagnoses
    FROM cohort_diagnosis_last diag
    GROUP BY initial_encounter_id, diag.patient_id
)

SELECT
    person_details_default.person_id AS patient_id,
    patient_identifier."Patient_Identifier" AS "Patient_Identifier", "public"."person_attributes"."Patient_code" AS "Patient Code",
    cohort.initial_encounter_id,
    person_details_default.gender AS gender,
    person_details_default.age AS age,
    CASE 
        WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, TO_DATE(person_details_default.birthyear::TEXT, 'YYYY'))) < 15 THEN '0-14 Years'
        WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, TO_DATE(person_details_default.birthyear::TEXT, 'YYYY'))) BETWEEN 15 AND 44 THEN '15-44 Years'
        WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, TO_DATE(person_details_default.birthyear::TEXT, 'YYYY'))) BETWEEN 45 AND 65 THEN '45-65 Years'
        WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, TO_DATE(person_details_default.birthyear::TEXT, 'YYYY'))) > 65 THEN '65+ Years'
        ELSE NULL
    END AS "Age Group",
    person_attributes."City_Village_Camp" AS "City_Village_Camp",
    cohort.initial_visit_date AS Enrollment_Date,
    cohort_diagnosis_last.diagnosis AS Diagnoses,
    Last_Visit_Location.visit_location AS "Last Visit Location",
    CASE 
        WHEN cohort.discharge_date IS NULL THEN 'Yes'
        ELSE NULL 
    END AS "In_Cohort?",
    cohort.discharge_date AS Date_of_Discharge,
    CASE 
        WHEN cohort.discharge_date IS NOT NULL AND cohort.discharge_date > cohort.initial_visit_date THEN cohort.patient_outcome
        ELSE NULL 
    END AS Patient_Outcome,
    cohort.readmission AS Readmission
    
FROM cohort_diagnosis_last

LEFT JOIN Aggregated_Diagnoses Agg_diag 
    ON cohort_diagnosis_last.patient_id = Agg_diag.patient_id 
    AND cohort_diagnosis_last.initial_encounter_id = Agg_diag.initial_encounter_id
LEFT JOIN Last_Visit_Location 
    ON cohort_diagnosis_last.initial_encounter_id = Last_Visit_Location.encounter_id
LEFT JOIN public.person_details_default  
    ON cohort_diagnosis_last.patient_id = person_details_default.person_id
LEFT OUTER JOIN public.patient_identifier 
    ON public.patient_identifier.patient_id = cohort_diagnosis_last.patient_id
LEFT JOIN public.person_attributes  
    ON cohort_diagnosis_last.patient_id = person_attributes.person_id
LEFT JOIN cohort 
    ON cohort_diagnosis_last.initial_encounter_id = cohort.initial_encounter_id

WHERE cohort_diagnosis_last.patient_id IS NOT NULL
