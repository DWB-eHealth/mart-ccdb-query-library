WITH initial AS (
	SELECT 
		patient_id, encounter_id AS initial_encounter_id, visit_location AS initial_visit_location,
		date AS initial_visit_date, DENSE_RANK () OVER (PARTITION BY patient_id ORDER BY date) AS initial_visit_order,
		LEAD (date) OVER (PARTITION BY patient_id ORDER BY date) AS next_initial_visit_date
	FROM  public.ncd_initial_visit),
cohort AS (
	SELECT
		i.patient_id, i.initial_encounter_id, i.initial_visit_location, i.initial_visit_date,
		CASE WHEN i.initial_visit_order > 1 THEN 'Yes' END readmission, d.encounter_id AS discharge_encounter_id, d.discharge_date, d.patient_outcome AS patient_outcome
	FROM initial i
	LEFT JOIN (SELECT patient_id, encounter_id, COALESCE(discharge_date::date, date::date) AS discharge_date, patient_outcome FROM public.ncd_discharge_visit) d 
		ON i.patient_id = d.patient_id AND d.discharge_date >= i.initial_visit_date AND (d.discharge_date < i.next_initial_visit_date OR i.next_initial_visit_date IS NULL)),
		
Last_hba1c AS (
	SELECT 
		DISTINCT ON (c.patient_id, c.initial_encounter_id) c.patient_id,
		c.initial_encounter_id,
		c.initial_visit_date, 
		c.discharge_encounter_id,
		c.discharge_date, 
		COALESCE(vli.date_of_sample_collection, vli.date_created) AS last_hba1c_date, 
		vli.hba1c AS last_hba1c
	FROM cohort c
	LEFT OUTER JOIN vitals_and_laboratory_information vli
		ON c.patient_id = vli.patient_id AND c.initial_visit_date <= COALESCE(vli.date_of_sample_collection, vli.date_created) 
		AND COALESCE(c.discharge_date, CURRENT_DATE) >= COALESCE(vli.date_of_sample_collection, vli.date_created)
	WHERE COALESCE(vli.date_created, vli.date_of_sample_collection) IS NOT NULL AND vli.hba1c IS NOT NULL
	ORDER BY c.patient_id, c.initial_encounter_id, vli.patient_id, COALESCE(vli.date_of_sample_collection, vli.date_created) DESC),
	
	last_bp AS (
	SELECT 
		DISTINCT ON (c.patient_id, c.initial_encounter_id) c.patient_id,
		c.initial_encounter_id,
		c.initial_visit_date, 
		c.discharge_encounter_id,
		c.discharge_date, 
		COALESCE(vli.date_of_vital_signs, vli.date_of_sample_collection) AS last_bp_date,
		vli.systolic_blood_pressure,
		vli.diastolic_blood_pressure
	FROM cohort c
	LEFT OUTER JOIN vitals_and_laboratory_information vli
		ON c.patient_id = vli.patient_id AND c.initial_visit_date <= COALESCE(vli.date_of_vital_signs, vli.date_of_sample_collection)
		AND COALESCE(c.discharge_date, CURRENT_DATE) >= COALESCE(vli.date_of_vital_signs, vli.date_of_sample_collection) 
	WHERE COALESCE(vli.date_of_vital_signs, vli.date_of_sample_collection) IS NOT NULL AND vli.systolic_blood_pressure IS NOT NULL AND vli.diastolic_blood_pressure IS NOT NULL
	ORDER BY c.patient_id, c.initial_encounter_id, vli.patient_id, COALESCE(vli.date_of_vital_signs, vli.date_of_sample_collection) DESC
	),
	
	last_fbg AS (
	SELECT 
		DISTINCT ON (c.patient_id, c.initial_encounter_id) c.patient_id,
		c.initial_encounter_id,
		c.initial_visit_date, 
		c.discharge_encounter_id,
		c.discharge_date, 
		COALESCE(vli.date_of_sample_collection, vli.date_created) AS last_fbg_date,
		vli.fasting_blood_glucose AS last_fbg
	FROM cohort c
	LEFT OUTER JOIN vitals_and_laboratory_information vli
		ON c.patient_id = vli.patient_id AND c.initial_visit_date <= COALESCE(vli.date_of_sample_collection, vli.date_created) 
		AND COALESCE(c.discharge_date, CURRENT_DATE) >= COALESCE(vli.date_of_sample_collection, vli.date_created)
	WHERE COALESCE(vli.date_created, vli.date_of_sample_collection) IS NOT NULL AND vli.fasting_blood_glucose IS NOT NULL
	ORDER BY c.patient_id, c.initial_encounter_id, vli.patient_id, COALESCE(vli.date_of_sample_collection, vli.date_created) DESC),
	
last_urine_protein AS (
	SELECT 
		DISTINCT ON (c.patient_id, c.initial_encounter_id) c.patient_id,
		c.initial_encounter_id,
		c.initial_visit_date, 
		c.discharge_encounter_id,
		c.discharge_date, 
		COALESCE(vli.date_of_sample_collection, vli.date_created) AS last_urine_protein_date, 
		vli.urine_protein AS last_urine_protein
	FROM cohort c
	LEFT OUTER JOIN vitals_and_laboratory_information vli
		ON c.patient_id = vli.patient_id AND c.initial_visit_date <= COALESCE(vli.date_of_sample_collection, vli.date_created) AND COALESCE(c.discharge_date, CURRENT_DATE) >= COALESCE(vli.date_of_sample_collection, vli.date_created)
	WHERE COALESCE(vli.date_created, vli.date_of_sample_collection) IS NOT NULL AND vli.urine_protein IS NOT NULL
	ORDER BY c.patient_id, c.initial_encounter_id, vli.patient_id, COALESCE(vli.date_of_sample_collection, vli.date_created) DESC),
	
	last_bmi AS (
	SELECT 
		DISTINCT ON (c.patient_id, c.initial_encounter_id) c.patient_id,
		c.initial_encounter_id,
		c.initial_visit_date, 
		c.discharge_encounter_id,
		c.discharge_date, 
		COALESCE(vli.date_of_vital_signs, vli.date_created) AS last_bmi_date,
		vli.bmi AS last_bmi
	FROM cohort c
	LEFT OUTER JOIN vitals_and_laboratory_information vli
		ON c.patient_id = vli.patient_id AND c.initial_visit_date <= COALESCE(vli.date_of_vital_signs, vli.date_created) AND COALESCE(c.discharge_date, CURRENT_DATE) >= COALESCE(vli.date_of_vital_signs, vli.date_created) 
	WHERE COALESCE(vli.date_of_vital_signs, vli.date_created) IS NOT NULL AND vli.bmi IS NOT NULL
	ORDER BY c.patient_id, c.initial_encounter_id, vli.patient_id, COALESCE(vli.date_of_vital_signs, vli.date_created) DESC),
	
Last_FUP AS (
  SELECT
  DISTINCT ON (c.patient_id, c.initial_encounter_id) c.patient_id,  
        c.initial_encounter_id,
		c.initial_visit_date, 
		c.discharge_encounter_id,
		c.discharge_date,
		ncd_fup.date,
        ncd_fup.currently_pregnant, 
        CASE WHEN ncd_fup.missed_ncd_medication_doses_in_last_7_days = 'Yes' THEN 'Yes' ELSE NULL END AS missed_ncd_medication_doses_in_last_7_days,
        CASE WHEN ncd_fup.missed_ncd_medication_doses_in_last_7_days = 'Yes' THEN ncd_fup.date ELSE NULL END AS missed_ncd_medication_doses_date,
        CASE WHEN ncd_fup.any_seizures_since_last_consultation = 'Yes' THEN 'Yes' ELSE NULL END AS any_seizures_since_last_consultation, 
        CASE WHEN ncd_fup.any_seizures_since_last_consultation = 'Yes' THEN ncd_fup.date ELSE NULL END AS Last_seizure_date,
        CASE WHEN ncd_fup.exacerbation_per_week IS NOT NULL AND ncd_fup.exacerbation_per_week > 0 THEN ncd_fup.date END AS exacerbations_date,
        CASE WHEN ncd_fup.exacerbation_per_week IS NOT NULL AND ncd_fup.exacerbation_per_week > 0 THEN 'Yes' END AS exacerbations_last_visit,
        CASE WHEN ncd_fup.exacerbation_per_week IS NOT NULL AND ncd_fup.exacerbation_per_week > 0 THEN ncd_fup.exacerbation_per_week ELSE NULL END AS exacerbation_per_week
	FROM cohort c
	LEFT OUTER JOIN public.ncd_follow_up_visit ncd_fup
		ON c.patient_id = ncd_fup.patient_id AND c.initial_visit_date <= COALESCE(ncd_fup.date)
		AND COALESCE(c.discharge_date, CURRENT_DATE) >= COALESCE(ncd_fup.date) 
	WHERE COALESCE(ncd_fup.date) IS NOT NULL
	ORDER BY c.patient_id, c.initial_encounter_id, ncd_fup.patient_id, COALESCE(ncd_fup.date) DESC
),

last_form_location AS (	
	SELECT 
		DISTINCT ON (c.patient_id, c.initial_encounter_id, c.initial_visit_date, c.discharge_date) c.initial_encounter_id,
		nvsl.date_created AS last_form_date,
		nvsl.visit_location AS last_form_location
	FROM cohort c
	LEFT OUTER JOIN (SELECT patient_id, date_created, visit_location FROM public.ncd_initial_visit UNION SELECT patient_id, date_created AS date,
	visit_location AS visit_location FROM public.ncd_initial_visit) nvsl
		ON c.patient_id = nvsl.patient_id AND c.initial_visit_date <= nvsl.date_created::date AND COALESCE(c.discharge_date, CURRENT_DATE) >= nvsl.date_created::date
	WHERE nvsl.visit_location IS NOT NULL
	GROUP BY c.patient_id, c.initial_encounter_id, c.initial_visit_date, c.discharge_date, nvsl.date_created, nvsl.visit_location
	ORDER BY c.patient_id, c.initial_encounter_id, c.initial_visit_date, c.discharge_date, nvsl.date_created DESC),

cohort_diagnosis AS (
	SELECT
		c.patient_id, c.initial_encounter_id, d.date_created, d.diagnosis AS diagnosis
	FROM public.diagnosis d 
	LEFT JOIN cohort c ON d.patient_id = c.patient_id AND c.initial_visit_date <= d.date_created AND COALESCE(c.discharge_date::date, CURRENT_DATE) >= d.date_created),

cohort_diagnosis_last AS (
    SELECT
        patient_id, initial_encounter_id, diagnosis, date_created
    FROM (
        SELECT
            cdg.*,
            ROW_NUMBER() OVER (PARTITION BY patient_id, initial_encounter_id, diagnosis_group ORDER BY date_created DESC) AS rn
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

Aggregated_Diagnoses AS (
    SELECT 
        diag.patient_id, initial_encounter_id,
        STRING_AGG(diag.diagnosis, ', ') AS aggregated_diagnoses
    FROM cohort_diagnosis_last diag
    GROUP BY initial_encounter_id, diag.patient_id
),
ncd_risk_factors_pivot AS (
    SELECT 
        patient_id, encounter_id,
        MAX(CASE WHEN risk_factor_noted = 'Occupational exposure' THEN 1 ELSE NULL END) AS occupational_exposure,
        MAX(CASE WHEN risk_factor_noted = 'Traditional medicine' THEN 1 ELSE NULL END) AS traditional_medicine,
        MAX(CASE WHEN risk_factor_noted = 'Second-hand smoking' THEN 1 ELSE NULL END) AS second_hand_smoking,
        MAX(CASE WHEN risk_factor_noted = 'Smoker' THEN 1 ELSE NULL END) AS smoker,
        MAX(CASE WHEN risk_factor_noted = 'Kitchen smoke' THEN 1 ELSE NULL END) AS kitchen_smoke,
        MAX(CASE WHEN risk_factor_noted = 'Alcohol use' THEN 1 ELSE NULL END) AS alcohol_use,
        MAX(CASE WHEN risk_factor_noted = 'Other' THEN 1 ELSE NULL END) AS other_risk
    FROM public."risk_factor_noted"
    GROUP BY encounter_id, patient_id
),

eye_exams AS (
	SELECT
        patient_id, encounter_id,
        MAX(last_exam_date) AS date
    FROM (
        -- Eye exams from initial visits
        SELECT 
            patient_id, encounter_id,
            date AS last_exam_date
        FROM public.ncd_initial_visit
        WHERE eye_exam_performed = 'Yes' AND date IS NOT NULL

        UNION ALL

        -- Eye exams from follow-up visits
        SELECT 
            patient_id, encounter_id,
            date AS last_exam_date
        FROM public.ncd_follow_up_visit
        WHERE eye_exam_performed = 'Yes' AND date IS NOT NULL

        UNION ALL

        -- Eye exams from discharge visits
        SELECT 
            patient_id, encounter_id,
            date AS last_exam_date
        FROM public.ncd_discharge_visit
        WHERE eye_exam_performed = 'Yes' AND date IS NOT NULL
    ) AS eye_exams
    GROUP BY encounter_id, patient_id 
),

last_eye_exam AS (
    SELECT 
        DISTINCT ON (c.patient_id, c.initial_encounter_id) c.patient_id,
        c.initial_encounter_id,
        c.initial_visit_date, 
        c.discharge_encounter_id,
        c.discharge_date, 
        e.date::date AS last_eye_exam_date,
        iv.visit_location AS initial_visit_location,
        fuv.visit_location AS follow_up_visit_location,
        dv.visit_location AS discharge_visit_location
    FROM cohort c
    LEFT OUTER JOIN eye_exams e 
        ON c.patient_id = e.patient_id 
        AND c.initial_visit_date <= e.date::date 
        AND COALESCE(c.discharge_date, CURRENT_DATE) >= e.date::date
    LEFT JOIN public.ncd_initial_visit iv
        ON c.patient_id = iv.patient_id
        AND c.initial_visit_date = iv.date
    LEFT JOIN public.ncd_follow_up_visit fuv
        ON c.patient_id = fuv.patient_id
        AND fuv.date BETWEEN c.initial_visit_date AND COALESCE(c.discharge_date, CURRENT_DATE)
    LEFT JOIN public.ncd_discharge_visit dv
        ON c.patient_id = dv.patient_id
        AND dv.date BETWEEN c.initial_visit_date AND COALESCE(c.discharge_date, CURRENT_DATE)
    ORDER BY c.patient_id, c.initial_encounter_id, e.patient_id, e.date::date DESC
),
	
	foot_exams AS (
	SELECT
        patient_id, encounter_id,
        MAX(last_exam_date) AS date
    FROM (
        -- Foot exams from initial visits
        SELECT 
            patient_id, encounter_id,
            date AS last_exam_date
        FROM public.ncd_initial_visit
        WHERE foot_exam_performed = 'Yes' AND date IS NOT NULL

        UNION ALL

        -- Foot exams from follow-up visits
        SELECT 
            patient_id, encounter_id,
            date AS last_exam_date
        FROM public.ncd_follow_up_visit
        WHERE foot_exam_performed = 'Yes' AND date IS NOT NULL

        UNION ALL

        -- Foot exams from discharge visits
        SELECT 
            patient_id, encounter_id,
            date AS last_exam_date
        FROM public.ncd_discharge_visit
        WHERE foot_exam_performed = 'Yes' AND date IS NOT NULL
    ) AS foot_exams
    GROUP BY encounter_id, patient_id
),

last_foot_exam AS (
    SELECT 
        DISTINCT ON (c.patient_id, c.initial_encounter_id) c.patient_id,
        c.initial_encounter_id,
        c.initial_visit_date, 
        c.discharge_encounter_id,
        c.discharge_date, 
        f.date::date AS last_foot_exam_date,
        iv.visit_location AS initial_visit_location,
        fuv.visit_location AS follow_up_visit_location,
        dv.visit_location AS discharge_visit_location
    FROM cohort c
    LEFT OUTER JOIN foot_exams f 
        ON c.patient_id = f.patient_id 
        AND c.initial_visit_date <= f.date::date 
        AND COALESCE(c.discharge_date, CURRENT_DATE) >= f.date::date
    LEFT JOIN public.ncd_initial_visit iv
        ON c.patient_id = iv.patient_id
        AND c.initial_visit_date = iv.date
    LEFT JOIN public.ncd_follow_up_visit fuv
        ON c.patient_id = fuv.patient_id
        AND fuv.date BETWEEN c.initial_visit_date AND COALESCE(c.discharge_date, CURRENT_DATE)
    LEFT JOIN public.ncd_discharge_visit dv
        ON c.patient_id = dv.patient_id
        AND dv.date BETWEEN c.initial_visit_date AND COALESCE(c.discharge_date, CURRENT_DATE)
    ORDER BY c.patient_id, c.initial_encounter_id, f.patient_id, f.date::date DESC
)


--Main Query

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
    cohort.discharge_date AS Date_of_Discharge,
    last_form_location.last_form_location AS "Last Visit Location",
    CASE 
        WHEN cohort.discharge_date IS NULL THEN 'Yes'
        ELSE NULL 
    END AS "In_Cohort?",
    CASE 
        WHEN EXTRACT(MONTH FROM AGE(CURRENT_DATE, cohort.discharge_date)) >= 6
             AND cohort.discharge_date IS NULL 
        THEN 'Yes'
        ELSE 'No'
    END AS "Last FUP 6+ months",
    
    CASE 
        WHEN cohort.discharge_date IS NOT NULL AND cohort.discharge_date > cohort.initial_visit_date THEN cohort.patient_outcome
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
    last_foot_exam.last_foot_exam_date AS "Last Foot Exam Date",
    Last_FUP.missed_ncd_medication_doses_date AS "Last Missed Medication Doses Date",
    Last_FUP.missed_ncd_medication_doses_in_last_7_days AS "Last Missed Medication Doses",
    last_eye_exam.last_eye_exam_date AS "Last Eye Exam Date",
    Last_FUP.any_seizures_since_last_consultation AS "Any seizures since last visit",
    Last_FUP.Last_seizure_date AS "Last Seizure Date",
    Last_FUP.exacerbations_date AS "Last_exacerbation_date",
    Last_FUP.exacerbations_last_visit AS "exacerbations_last_visit",
    Last_FUP.exacerbation_per_week AS "exacerbation per week",
    Last_HbA1c.last_hba1c_date AS "Last HbA1c Date",
    last_urine_protein.last_urine_protein AS "Last Urine Protein",
    last_urine_protein.last_urine_protein_date AS "Last Urine Protein Date",
    last_bp.systolic_blood_pressure AS "Last Systolic BP",
    last_bp.diastolic_blood_pressure AS "Last Diastolic BP",
    last_bp.last_bp_date AS "Last BP Date",
    Last_HbA1c.last_hba1c AS "Last HbA1c", last_fbg.last_fbg AS "Last Fasting Blood Glucose",
    last_bmi.last_BMI AS "Last BMI", last_bmi.last_BMI_date AS "Last BMI Date",
    CASE WHEN ((DATE_PART('year', CURRENT_DATE) - DATE_PART('year', cohort.initial_visit_date)) * 12 + (DATE_PART('month', CURRENT_DATE) - DATE_PART('month', cohort.initial_visit_date))) >= 6 
    AND cohort.discharge_date IS NULL THEN 'Yes' END AS in_cohort_6m,
	CASE WHEN last_bp.systolic_blood_pressure <= 140 AND last_bp.diastolic_blood_pressure <= 90 THEN 'Yes' WHEN last_bp.systolic_blood_pressure > 140 OR last_bp.diastolic_blood_pressure > 90 THEN 'No'
	END AS blood_pressure_control,
	CASE WHEN Last_HbA1c.last_hba1c < 8 THEN 'Yes' WHEN Last_HbA1c.last_hba1c >= 8 THEN 'No' WHEN Last_HbA1c.last_hba1c IS NULL AND last_fbg.last_fbg < 150 THEN 'Yes' WHEN Last_HbA1c.last_hba1c IS NULL AND last_fbg.last_fbg >= 150 THEN 'No' END AS diabetes_control,
	cohort.readmission AS Readmission, cohort.initial_encounter_id

FROM
    cohort

LEFT OUTER JOIN public.patient_identifier 
    ON public.patient_identifier.patient_id = cohort.patient_id

LEFT OUTER JOIN Last_FUP 
    ON Last_FUP.initial_encounter_id = cohort.initial_encounter_id

LEFT OUTER JOIN Aggregated_Diagnoses Agg_diag 
    ON Agg_diag.initial_encounter_id = cohort.initial_encounter_id

LEFT OUTER JOIN ncd_diagnosis_pivot ncd_pivot
    ON ncd_pivot.initial_encounter_id = cohort.initial_encounter_id

LEFT OUTER JOIN ncd_risk_factors_pivot ncd_risk_pivot
    ON ncd_risk_pivot.encounter_id = cohort.initial_encounter_id

LEFT OUTER JOIN last_foot_exam 
    ON last_foot_exam.initial_encounter_id = cohort.initial_encounter_id
    
LEFT OUTER JOIN initial 
    ON initial.initial_encounter_id = cohort.initial_encounter_id
    
LEFT OUTER JOIN public.person_details_default
    ON public.person_details_default.person_id = cohort.patient_id
	
LEFT OUTER JOIN public.person_attributes
    ON person_attributes.person_id = cohort.patient_id
    
LEFT OUTER JOIN last_bp 
    ON last_bp.initial_encounter_id = cohort.initial_encounter_id
    
LEFT OUTER JOIN Last_HbA1c 
    ON Last_HbA1c.initial_encounter_id = cohort.initial_encounter_id 
    
LEFT OUTER JOIN last_fbg 
    ON last_fbg.initial_encounter_id = cohort.initial_encounter_id
    
LEFT OUTER JOIN last_urine_protein 
    ON last_urine_protein.initial_encounter_id = cohort.initial_encounter_id
    
LEFT OUTER JOIN last_bmi 
    ON last_bmi.initial_encounter_id = cohort.initial_encounter_id
    
LEFT OUTER JOIN last_eye_exam 
    ON last_eye_exam.initial_encounter_id = cohort.initial_encounter_id

LEFT OUTER JOIN last_form_location 
    ON last_form_location.initial_encounter_id = cohort.initial_encounter_id
    
    
WHERE 
    "public"."patient_identifier"."Patient_Identifier" IS NOT NULL;
