WITH initial AS (
	SELECT 
		patient_id, encounter_id AS initial_encounter_id, visit_location AS initial_visit_location,
		date AS initial_visit_date, DENSE_RANK () OVER (PARTITION BY patient_id ORDER BY date) AS initial_visit_order,
		LEAD (date) OVER (PARTITION BY patient_id ORDER BY date) AS next_initial_visit_date
	FROM  "public"."ncd_initial_visit"),
cohort AS (
	SELECT
		i.patient_id, i.initial_encounter_id, i.initial_visit_location, i.initial_visit_date,
		CASE WHEN i.initial_visit_order > 1 THEN 'Yes' END readmission, d.encounter_id AS discharge_encounter_id, d.discharge_date, d.patient_outcome AS patient_outcome
	FROM initial i
	LEFT JOIN (SELECT patient_id, encounter_id, COALESCE(discharge_date::date, date::date) AS discharge_date, patient_outcome FROM "public"."ncd_discharge_visit") d 
		ON i.patient_id = d.patient_id AND d.discharge_date >= i.initial_visit_date AND (d.discharge_date < i.next_initial_visit_date OR i.next_initial_visit_date IS NULL)),

First_initial AS (
    SELECT DISTINCT ON (ncd_init.patient_id)
        ncd_init.patient_id,  
        ncd_init.date,
        ncd_init.obs_datetime,   
        ncd_init.date_created, 
        ncd_init.encounter_id
    FROM public.ncd_initial_visit ncd_init
    WHERE ncd_init.date IS NOT NULL
    ORDER BY ncd_init.patient_id, ncd_init.date ASC
),
Last_Vitals_Labs_Date AS (
    SELECT
        patient_id,
        MAX(CASE WHEN hba1c IS NOT NULL THEN date_created ELSE NULL END) AS last_hba1c_date,
        MAX(CASE WHEN hba1c IS NOT NULL THEN hba1c ELSE NULL END) AS last_hba1c,
        MAX(CASE WHEN fasting_blood_glucose IS NOT NULL THEN fasting_blood_glucose ELSE NULL END) AS last_fbg,
        MAX(CASE WHEN random_blood_glucose IS NOT NULL THEN random_blood_glucose ELSE NULL END) AS last_rbg,
        MAX(CASE WHEN urine_protein IS NOT NULL THEN urine_protein ELSE NULL END) AS last_urine_protein,
        MAX(CASE WHEN systolic_blood_pressure IS NOT NULL THEN systolic_blood_pressure ELSE NULL END) AS last_systolic_bp,
        MAX(CASE WHEN diastolic_blood_pressure IS NOT NULL THEN diastolic_blood_pressure ELSE NULL END) AS last_diastolic_bp,
        MAX(CASE WHEN bmi IS NOT NULL THEN bmi ELSE NULL END) AS last_BMI,
        MAX(CASE WHEN bmi IS NOT NULL THEN date_of_vital_signs ELSE NULL END) AS last_BMI_date 
    FROM public.vitals_and_laboratory_information
    GROUP BY patient_id
),
Last_initial AS (
    SELECT DISTINCT ON (ncd_init.patient_id)
        ncd_init.patient_id,  
        ncd_init.date,
        ncd_init.obs_datetime,   
        ncd_init.date_created, 
        ncd_init.encounter_id,
        ncd_init.currently_pregnant, 
        ncd_init.missed_ncd_medication_doses_in_last_7_days, 
        ncd_init.eye_exam_performed, 
        ncd_init.foot_exam_performed, 
        ncd_init.any_seizures_since_last_visit,
        CASE 
            WHEN ncd_init.exacerbation_per_week IS NOT NULL 
                 AND ncd_init.exacerbation_per_week > 0 
            THEN 'Yes' 
        END AS initial_exacerbation_per_week
    FROM public.ncd_initial_visit ncd_init
    WHERE ncd_init.date IS NOT NULL
    ORDER BY ncd_init.patient_id, ncd_init.date DESC
),
Last_FUP AS (
    SELECT DISTINCT ON (ncd_fup.patient_id)
        ncd_fup.patient_id,  
        ncd_fup.date,
        ncd_fup.obs_datetime,   
        ncd_fup.date_created, 
        ncd_fup.currently_pregnant, 
        ncd_fup.missed_ncd_medication_doses_in_last_7_days, 
        ncd_fup.eye_exam_performed, 
        ncd_fup.foot_exam_performed, 
        ncd_fup.any_seizures_since_last_consultation,
        CASE 
            WHEN ncd_fup.exacerbation_per_week IS NOT NULL 
                 AND ncd_fup.exacerbation_per_week > 0 
            THEN 'Yes' 
        END AS FUP_exacerbation_per_week
    FROM public.ncd_follow_up_visit ncd_fup
    WHERE ncd_fup.date IS NOT NULL
    ORDER BY ncd_fup.patient_id, ncd_fup.date DESC
),
Initial_diagnosis AS (
    SELECT DISTINCT ON (diag.patient_id)
        diag.patient_id,    
        diag.obs_datetime,
        diag.diagnosis
    FROM public.diagnosis diag
    WHERE diag.date_created IS NOT NULL
    ORDER BY diag.patient_id, diag.date_created ASC
),
Last_discharge AS (
    SELECT DISTINCT ON (ncd_disc.patient_id)
        ncd_disc.patient_id,    
        ncd_disc.date_created,
        ncd_disc.date, 
        ncd_disc.patient_outcome, 
        ncd_disc.currently_pregnant, 
        ncd_disc.missed_ncd_medication_doses_in_last_7_days, 
        ncd_disc.eye_exam_performed, 
        ncd_disc.foot_exam_performed, 
        ncd_disc.any_seizures_since_last_consultation,
        CASE 
            WHEN ncd_disc.exacerbation_per_week IS NOT NULL 
                 AND ncd_disc.exacerbation_per_week > 0 
            THEN 'Yes' 
        END AS Discharge_exacerbation_per_week
    FROM public.ncd_discharge_visit ncd_disc
    WHERE ncd_disc.date IS NOT NULL
    ORDER BY ncd_disc.patient_id, ncd_disc.date ASC
),
Aggregated_Diagnoses AS (
    SELECT 
        diag.patient_id, 
        STRING_AGG(diag.diagnosis, ', ') AS aggregated_diagnoses
    FROM public.diagnosis diag
    GROUP BY diag.patient_id
),
Last_Visit_Location AS (
    SELECT DISTINCT ON (combined.patient_id)
        combined.patient_id,
        combined.visit_location
    FROM (
        SELECT 
            ncd_disc.patient_id, 
            ncd_disc.visit_location, 
            ncd_disc.date 
        FROM public.ncd_discharge_visit ncd_disc
        WHERE ncd_disc.visit_location IS NOT NULL
        UNION ALL
        SELECT 
            ncd_fup.patient_id, 
            ncd_fup.visit_location, 
            ncd_fup.date 
        FROM public.ncd_follow_up_visit ncd_fup
        WHERE ncd_fup.visit_location IS NOT NULL
        UNION ALL
        SELECT 
            ncd_init.patient_id, 
            ncd_init.visit_location, 
            ncd_init.date 
        FROM public.ncd_initial_visit ncd_init
        WHERE ncd_init.visit_location IS NOT NULL
    ) AS combined
    ORDER BY combined.patient_id, combined.date DESC
),

cohort_diagnosis AS (
	SELECT
		c.patient_id, c.initial_encounter_id, n.date, d.diagnosis AS diagnosis
	FROM public.diagnosis d 
	LEFT JOIN public.ncd_initial_visit n USING(encounter_id)
	LEFT JOIN cohort c ON d.patient_id = c.patient_id AND c.initial_visit_date <= n.date AND COALESCE(c.discharge_date::date, CURRENT_DATE) >= n.date),

cohort_diagnosis_last AS (
    SELECT
        patient_id, initial_encounter_id, diagnosis, date
    FROM (
        SELECT
            cdg.*,
            ROW_NUMBER() OVER (PARTITION BY patient_id, initial_encounter_id, diagnosis_group ORDER BY date DESC) AS rn
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
            FROM cohort_diagnosis cd) cdg) foo
    WHERE diagnosis IS NOT NULL),
    
ncd_diagnosis_pivot AS (
    SELECT 
        patient_id,initial_encounter_id,
        MAX(CASE WHEN diagnosis = 'Asthma' THEN 1 ELSE NULL END) AS asthma,
        MAX(CASE WHEN diagnosis = 'Chronic kidney disease' THEN 1 ELSE NULL END) AS chronic_kidney_disease,
        MAX(CASE WHEN diagnosis = 'Cardiovascular disease' THEN 1 ELSE NULL END) AS cardiovascular_disease,
        MAX(CASE WHEN diagnosis = 'Chronic obstructive pulmonary disease' THEN 1 ELSE NULL END) AS copd,
        MAX(CASE WHEN diagnosis = 'Diabetes mellitus, type 1' THEN 1 ELSE NULL END) AS diabetes_type1,
        MAX(CASE WHEN diagnosis = 'Diabetes mellitus, type 2' THEN 1 ELSE NULL END) AS diabetes_type2,
        MAX(CASE WHEN diagnosis = 'Hypertension' THEN 1 ELSE NULL END) AS hypertension,
        MAX(CASE WHEN diagnosis = 'Hypothyroidism' THEN 1 ELSE NULL END) AS hypothyroidism,
        MAX(CASE WHEN diagnosis = 'Hyperthyroidism' THEN 1 ELSE NULL END) AS hyperthyroidism,
        MAX(CASE WHEN diagnosis = 'Focal epilepsy' THEN 1 ELSE NULL END) AS focal_epilepsy,
        MAX(CASE WHEN diagnosis = 'Generalised epilepsy' THEN 1 ELSE NULL END) AS generalised_epilepsy,
        MAX(CASE WHEN diagnosis = 'Unclassified epilepsy' THEN 1 ELSE NULL END) AS unclassified_epilepsy,
        MAX(CASE WHEN diagnosis = 'Other' THEN 1 ELSE NULL END) AS other_ncd
    FROM cohort_diagnosis_last
    GROUP BY initial_encounter_id, patient_id
),
ncd_risk_factors_pivot AS (
    SELECT 
        patient_id,
        MAX(CASE WHEN risk_factor_noted = 'Occupational exposure' THEN 1 ELSE NULL END) AS occupational_exposure,
        MAX(CASE WHEN risk_factor_noted = 'Traditional medicine' THEN 1 ELSE NULL END) AS traditional_medicine,
        MAX(CASE WHEN risk_factor_noted = 'Second-hand smoking' THEN 1 ELSE NULL END) AS second_hand_smoking,
        MAX(CASE WHEN risk_factor_noted = 'Smoker' THEN 1 ELSE NULL END) AS smoker,
        MAX(CASE WHEN risk_factor_noted = 'Kitchen smoke' THEN 1 ELSE NULL END) AS kitchen_smoke,
        MAX(CASE WHEN risk_factor_noted = 'Alcohol use' THEN 1 ELSE NULL END) AS alcohol_use,
        MAX(CASE WHEN risk_factor_noted = 'Other' THEN 1 ELSE NULL END) AS other_risk
    FROM public."risk_factor_noted"
    GROUP BY patient_id
),
-- To get the last foot exam date
Last_Foot_Exam_Date AS (
    SELECT
        patient_id,
        MAX(last_exam_date) AS last_foot_exam_date
    FROM (
        -- Foot exams from initial visits
        SELECT 
            patient_id, 
            date AS last_exam_date
        FROM public.ncd_initial_visit
        WHERE foot_exam_performed = 'Yes' AND date IS NOT NULL

        UNION ALL

        -- Foot exams from follow-up visits
        SELECT 
            patient_id, 
            date AS last_exam_date
        FROM public.ncd_follow_up_visit
        WHERE foot_exam_performed = 'Yes' AND date IS NOT NULL

        UNION ALL

        -- Foot exams from discharge visits
        SELECT 
            patient_id, 
            date AS last_exam_date
        FROM public.ncd_discharge_visit
        WHERE foot_exam_performed = 'Yes' AND date IS NOT NULL
    ) AS foot_exams
    GROUP BY patient_id
),
-- The last missed medication doses date
Last_Missed_Med_Doses AS (
    SELECT
        patient_id,
        MAX(missed_med_doses_date) AS last_missed_med_doses_date
    FROM (
        -- Missed doses from initial visits
        SELECT 
            patient_id, 
            date AS missed_med_doses_date
        FROM public.ncd_initial_visit
        WHERE missed_ncd_medication_doses_in_last_7_days = 'Yes' AND date IS NOT NULL

        UNION ALL

        -- Missed doses from follow-up visits
        SELECT 
            patient_id, 
            date AS missed_med_doses_date
        FROM public.ncd_follow_up_visit
        WHERE missed_ncd_medication_doses_in_last_7_days = 'Yes' AND date IS NOT NULL

        UNION ALL

        -- Missed doses from discharge visits
        SELECT 
            patient_id, 
            date AS missed_med_doses_date
        FROM public.ncd_discharge_visit
        WHERE missed_ncd_medication_doses_in_last_7_days = 'Yes' AND date IS NOT NULL
    ) AS missed_doses
    GROUP BY patient_id
),
Last_Eye_Exam_Date AS (
    SELECT
        patient_id,
        MAX(last_exam_date) AS last_eye_exam_date
    FROM (
        -- Eye exams from initial visits
        SELECT 
            patient_id, 
            date AS last_exam_date
        FROM public.ncd_initial_visit
        WHERE eye_exam_performed = 'Yes' AND date IS NOT NULL

        UNION ALL

        -- Eye exams from follow-up visits
        SELECT 
            patient_id, 
            date AS last_exam_date
        FROM public.ncd_follow_up_visit
        WHERE eye_exam_performed = 'Yes' AND date IS NOT NULL

        UNION ALL

        -- Eye exams from discharge visits
        SELECT 
            patient_id, 
            date AS last_exam_date
        FROM public.ncd_discharge_visit
        WHERE eye_exam_performed = 'Yes' AND date IS NOT NULL
    ) AS eye_exams
    GROUP BY patient_id
),

Last_Seizure_Date AS (
    SELECT
        patient_id,
        MAX(last_seizure_date) AS last_seizure_date
    FROM (
        -- Seizures from initial visits
        SELECT 
            patient_id, 
            date AS last_seizure_date
        FROM public.ncd_initial_visit
        WHERE any_seizures_since_last_visit = 'Yes' AND date IS NOT NULL

        UNION ALL

        -- Seizures from follow-up visits
        SELECT 
            patient_id, 
            date AS last_seizure_date
        FROM public.ncd_follow_up_visit
        WHERE any_seizures_since_last_consultation = 'Yes' AND date IS NOT NULL

        UNION ALL

        -- Seizures from discharge visits
        SELECT 
            patient_id, 
            date AS last_seizure_date
        FROM public.ncd_discharge_visit
        WHERE any_seizures_since_last_consultation = 'Yes' AND date IS NOT NULL
    ) AS seizures
    GROUP BY patient_id
)

SELECT
    "public"."person_details_default".person_id AS patient_id,  
    "public"."patient_identifier"."Patient_Identifier" AS "Patient_Identifier",
    "public"."person_details_default".gender AS gender,
    "public"."person_details_default".age AS age,
    CASE 
        WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, TO_DATE("public"."person_details_default".birthyear::TEXT, 'YYYY'))) < 15 THEN '0-14 Years'
        WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, TO_DATE("public"."person_details_default".birthyear::TEXT, 'YYYY'))) BETWEEN 15 AND 44 THEN '15-44 Years'
        WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, TO_DATE("public"."person_details_default".birthyear::TEXT, 'YYYY'))) BETWEEN 45 AND 65 THEN '45-65 Years'
        WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, TO_DATE("public"."person_details_default".birthyear::TEXT, 'YYYY'))) > 65 THEN '65+ Years'
        ELSE NULL
    END AS "Age Group",
    "public"."person_attributes"."Legal_status" AS "Legal_status",
    "public"."person_attributes"."Education_level" AS "Education_level",
    "public"."person_attributes"."Personal_Situation" AS "Personal_Situation",
    "public"."person_attributes"."Patient_code" AS "Patient_code",
    "public"."person_attributes"."City_Village_Camp" AS "City_Village_Camp",
    cohort.initial_visit_date AS Enrollment_Date, 
    Last_FUP.date AS Date_of_last_FUP, 
    Agg_diag.aggregated_diagnoses AS Diagnoses,
    Last_discharge.date AS Date_of_Discharge,
    Last_Visit_Location.visit_location AS "Last Visit Location",
    CASE 
        WHEN Last_discharge.date IS NULL THEN 'Yes'
        WHEN Last_discharge.date IS NOT NULL AND Last_discharge.date > First_initial.date THEN 'No'
        WHEN Last_discharge.date IS NOT NULL AND Last_discharge.date < First_initial.date THEN 'Yes'
        ELSE NULL 
    END AS "In_Cohort?",
    CASE 
        WHEN EXTRACT(MONTH FROM AGE(CURRENT_DATE, Last_FUP.date)) >= 6
             AND Last_discharge.date IS NULL 
        THEN 'Yes'
        ELSE 'No'
    END AS "Last FUP 6+ months",
    
    CASE 
        WHEN Last_discharge.date IS NOT NULL AND Last_discharge.date > First_initial.date THEN Last_discharge.patient_outcome
        ELSE NULL 
    END AS Patient_Outcome,
    
    -- Pivoted Diagnosis 
    COALESCE(ncd_pivot.asthma, NULL) AS asthma,
    COALESCE(ncd_pivot.chronic_kidney_disease, NULL) AS chronic_kidney_disease,
    COALESCE(ncd_pivot.cardiovascular_disease, NULL) AS cardiovascular_disease,
    COALESCE(ncd_pivot.copd, NULL) AS copd,
    COALESCE(ncd_pivot.diabetes_type1, NULL) AS diabetes_type1,
    COALESCE(ncd_pivot.diabetes_type2, NULL) AS diabetes_type2,
    COALESCE(ncd_pivot.hypertension, NULL) AS hypertension,
    COALESCE(ncd_pivot.hypothyroidism, NULL) AS hypothyroidism,
    COALESCE(ncd_pivot.hyperthyroidism, NULL) AS hyperthyroidism,
    COALESCE(ncd_pivot.focal_epilepsy, NULL) AS focal_epilepsy,
    COALESCE(ncd_pivot.generalised_epilepsy, NULL) AS generalised_epilepsy,
    COALESCE(ncd_pivot.unclassified_epilepsy, NULL) AS unclassified_epilepsy,
    COALESCE(ncd_pivot.other_ncd, NULL) AS other_ncd,
    
    -- Pivoted Risk Factors
    COALESCE(ncd_risk_pivot.occupational_exposure, NULL) AS occupational_exposure,
    COALESCE(ncd_risk_pivot.traditional_medicine, NULL) AS traditional_medicine,
    COALESCE(ncd_risk_pivot.second_hand_smoking, NULL) AS second_hand_smoking,
    COALESCE(ncd_risk_pivot.smoker, NULL) AS smoker,
    COALESCE(ncd_risk_pivot.kitchen_smoke, NULL) AS kitchen_smoke,
    COALESCE(ncd_risk_pivot.alcohol_use, NULL) AS alcohol_use,
    COALESCE(ncd_risk_pivot.other_risk, NULL) AS other_risk,
    CASE 
        WHEN ncd_pivot.diabetes_type1 IS NOT NULL 
             OR ncd_pivot.diabetes_type2 IS NOT NULL 
        THEN 1 
    END AS Diabetes_any,
    Last_Foot_Exam_Date.last_foot_exam_date AS "Last Foot Exam Date",
    Last_Missed_Med_Doses.last_missed_med_doses_date AS "Last Missed Medication Doses Date",
    Last_Eye_Exam_Date.last_eye_exam_date AS "Last Eye Exam Date",
    Last_Seizure_Date.last_seizure_date AS "Last Seizure Date",
    Last_Vitals_Labs_Date.last_hba1c_date AS "Last HbA1c Date",
    Last_Vitals_Labs_Date.last_urine_protein AS "Last Urine Protein",
    Last_Vitals_Labs_Date.last_systolic_bp AS "Last Systolic BP",
    Last_Vitals_Labs_Date.last_diastolic_bp AS "Last Diastolic BP",
    Last_Vitals_Labs_Date.last_hba1c AS "Last HbA1c", Last_Vitals_Labs_Date.last_fbg AS "Last Fasting Blood Glucose",
    Last_Vitals_Labs_Date.last_BMI AS "Last BMI", Last_Vitals_Labs_Date.last_BMI_date AS "Last BMI Date",
    CASE WHEN ((DATE_PART('year', CURRENT_DATE) - DATE_PART('year', Last_initial.date)) * 12 + (DATE_PART('month', CURRENT_DATE) - DATE_PART('month', Last_initial.date))) >= 6 AND Last_discharge.date IS NULL THEN 'Yes' END AS in_cohort_6m,
	CASE WHEN Last_Vitals_Labs_Date.last_systolic_bp <= 140 AND Last_Vitals_Labs_Date.last_diastolic_bp <= 90 THEN 'Yes' WHEN Last_Vitals_Labs_Date.last_systolic_bp > 140 OR Last_Vitals_Labs_Date.last_diastolic_bp > 90 THEN 'No'
	END AS blood_pressure_control,
	CASE WHEN Last_Vitals_Labs_Date.last_hba1c < 8 THEN 'Yes' WHEN Last_Vitals_Labs_Date.last_hba1c >= 8 THEN 'No' WHEN Last_Vitals_Labs_Date.last_hba1c IS NULL AND Last_Vitals_Labs_Date.last_fbg < 150 THEN 'Yes' WHEN Last_Vitals_Labs_Date.last_hba1c IS NULL AND Last_Vitals_Labs_Date.last_fbg >= 150 THEN 'No' END AS diabetes_control,
	cohort.readmission AS Readmission

FROM
    cohort

LEFT OUTER JOIN public.patient_identifier 
    ON public.patient_identifier.patient_id = cohort.patient_id

LEFT OUTER JOIN First_initial 
    ON First_initial.patient_id = cohort.patient_id

LEFT OUTER JOIN Last_FUP 
    ON Last_FUP.patient_id = cohort.patient_id

LEFT OUTER JOIN Initial_diagnosis diag 
    ON diag.patient_id = cohort.patient_id

LEFT OUTER JOIN Last_discharge 
    ON Last_discharge.patient_id = cohort.patient_id

LEFT OUTER JOIN Aggregated_Diagnoses Agg_diag 
    ON Agg_diag.patient_id = cohort.patient_id

LEFT OUTER JOIN Last_initial 
    ON Last_initial.patient_id = cohort.patient_id

LEFT OUTER JOIN Last_Visit_Location 
    ON Last_Visit_Location.patient_id = cohort.patient_id

LEFT OUTER JOIN ncd_diagnosis_pivot ncd_pivot
    ON ncd_pivot.patient_id = cohort.patient_id

LEFT OUTER JOIN ncd_risk_factors_pivot ncd_risk_pivot
    ON ncd_risk_pivot.patient_id = cohort.patient_id

LEFT OUTER JOIN Last_Foot_Exam_Date 
    ON Last_Foot_Exam_Date.patient_id = cohort.patient_id

LEFT OUTER JOIN Last_Missed_Med_Doses 
    ON Last_Missed_Med_Doses.patient_id = cohort.patient_id
    
LEFT OUTER JOIN Last_Eye_Exam_Date
    ON Last_Eye_Exam_Date.patient_id = cohort.patient_id
    
LEFT OUTER JOIN Last_Seizure_Date 
    ON Last_Seizure_Date.patient_id = cohort.patient_id
    
LEFT OUTER JOIN Last_Vitals_Labs_Date 
    ON Last_Vitals_Labs_Date.patient_id = cohort.patient_id
    
LEFT OUTER JOIN initial 
    ON initial.patient_id = cohort.patient_id
    
LEFT OUTER JOIN public.person_details_default
    ON public.person_details_default.person_id = cohort.patient_id
	
LEFT OUTER JOIN public.person_attributes
    ON person_attributes.person_id = cohort.patient_id
    
WHERE 
    "public"."patient_identifier"."Patient_Identifier" IS NOT NULL;
