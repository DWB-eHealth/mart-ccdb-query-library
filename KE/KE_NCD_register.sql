-- The first CTEs build the frame for patients entering and exiting the cohort. This frame is based on NCD forms with visit types of 'initial visit' and 'patient outcome'. The query takes all initial visit dates and matches patient outcome visit dates if the patient outcome visit date falls between the initial visit date and the next initial visit date (if present).
WITH initial AS (
	SELECT 
		patient_id, encounter_id AS initial_encounter_id, visit_location AS initial_visit_location, date_of_visit AS initial_visit_date, DENSE_RANK () OVER (PARTITION BY patient_id ORDER BY date_of_visit) AS initial_visit_order, LEAD (date_of_visit) OVER (PARTITION BY patient_id ORDER BY date_of_visit) AS next_initial_visit_date
	FROM ncd WHERE visit_type = 'Initial visit'),
cohort AS (
	SELECT
		i.patient_id, i.initial_encounter_id, i.initial_visit_location, i.initial_visit_date, CASE WHEN i.initial_visit_order > 1 THEN 'Yes' END readmission, d.encounter_id AS outcome_encounter_id, d.outcome_date2 AS outcome_date, d.patient_outcome_at_end_of_msf_care AS patient_outcome 
	FROM initial i
	LEFT JOIN (SELECT patient_id, encounter_id, COALESCE(patient_outcome_date::date, date_of_visit::date) AS outcome_date2, patient_outcome_at_end_of_msf_care FROM ncd WHERE visit_type = 'Patient outcome') d 
		ON i.patient_id = d.patient_id AND (d.outcome_date2 IS NULL OR (d.outcome_date2 >= i.initial_visit_date AND (d.outcome_date2 < i.next_initial_visit_date OR i.next_initial_visit_date IS NULL)))),
-- The NCD diagnosis CTEs extract all NCD diagnoses for patients reported between their initial visit and discharge visit. Diagnoses are only reported once. For specific disease groups, the second CTE extracts only the last reported diagnosis among the groups. These groups include types of diabetes, types of epilespy, and hyper-/hypothyroidism. The final CTE pivotes the diagnoses horizontally.
cohort_diagnosis AS (
	SELECT
		c.patient_id, c.initial_encounter_id, COALESCE(d.date_of_diagnosis, n.date_of_visit) AS date_of_diagnosis, d.ncd_cohort_diagnosis AS diagnosis
	FROM ncd_ncd_diagnoses d 
	LEFT JOIN ncd n USING(encounter_id)
	LEFT JOIN cohort c ON d.patient_id = c.patient_id AND c.initial_visit_date <= n.date_of_visit AND COALESCE(c.outcome_date::date, CURRENT_DATE) >= n.date_of_visit),
cohort_diagnosis_last AS (
    SELECT
        patient_id, initial_encounter_id, diagnosis, date_of_diagnosis
    FROM (
        SELECT
            cdg.*,
            ROW_NUMBER() OVER (PARTITION BY patient_id, initial_encounter_id, diagnosis_group ORDER BY date_of_diagnosis DESC) AS rn
        FROM (
            SELECT
                cd.*,
                CASE
                    WHEN diagnosis IN ('Asthma', 'Chronic obstructive pulmonary disease', 'Hypertension', 'Sickle cell disease') THEN 'Group1'
                    WHEN diagnosis IN ('Diabetes mellitus, type 1', 'Diabetes mellitus, type 2') THEN 'Group2'
                    WHEN diagnosis IN ('Focal epilepsy', 'Generalised epilepsy', 'Unclassified epilepsy') THEN 'Group3'
                    ELSE 'Other'
                END AS diagnosis_group
            FROM cohort_diagnosis cd) cdg) foo
    WHERE rn = 1),
ncd_diagnosis_pivot AS (
	SELECT 
		DISTINCT ON (initial_encounter_id, patient_id) initial_encounter_id, 
		patient_id,
		MAX (CASE WHEN diagnosis = 'Asthma' THEN date_of_diagnosis ELSE NULL END) AS asthma,
		MAX (CASE WHEN diagnosis = 'Sickle cell disease' THEN date_of_diagnosis ELSE NULL END) AS chronic_kidney_disease,
		MAX (CASE WHEN diagnosis = 'Chronic obstructive pulmonary disease' THEN date_of_diagnosis ELSE NULL END) AS copd,
		MAX (CASE WHEN diagnosis = 'Diabetes mellitus, type 1' THEN date_of_diagnosis ELSE NULL END) AS diabetes_type1,
		MAX (CASE WHEN diagnosis = 'Diabetes mellitus, type 2' THEN date_of_diagnosis ELSE NULL END) AS diabetes_type2,
		MAX (CASE WHEN diagnosis = 'Hypertension' THEN date_of_diagnosis ELSE NULL END) AS hypertension,
		MAX (CASE WHEN diagnosis = 'Focal epilepsy' THEN date_of_diagnosis ELSE NULL END) AS focal_epilepsy,
		MAX (CASE WHEN diagnosis = 'Generalised epilepsy' THEN date_of_diagnosis ELSE NULL END) AS generalised_epilepsy,
		MAX (CASE WHEN diagnosis = 'Unclassified epilepsy' THEN date_of_diagnosis ELSE NULL END) AS unclassified_epilepsy
	FROM cohort_diagnosis_last
	GROUP BY initial_encounter_id, patient_id),
ncd_diagnosis_list AS (
	SELECT initial_encounter_id, STRING_AGG(diagnosis, ', ') AS diagnosis_list
	FROM cohort_diagnosis_last
	GROUP BY initial_encounter_id),
-- The risk factor CTEs pivot risk factor data horizontally from the NCD form. Only the last risk factors are reported per cohort enrollment are present. 
current_lifestyle_pivot AS (
	SELECT 
		DISTINCT ON (n.encounter_id, n.patient_id, n.date_of_visit::date) n.encounter_id, 
		n.patient_id, 
		n.date_of_visit::date,
		MAX (CASE WHEN cl.current_lifestyle = 'Adequate physical activity' THEN 1 ELSE NULL END) AS adequate_physical_activity,
		MAX (CASE WHEN cl.current_lifestyle = 'Tobacco use' THEN 1 ELSE NULL END) AS tobacco_use,
		MAX (CASE WHEN cl.current_lifestyle = 'Healthy diet' THEN 1 ELSE NULL END) AS healthy_diet,
		MAX (CASE WHEN cl.current_lifestyle = 'Alcohol use' THEN 1 ELSE NULL END) AS alcohol_use
	FROM ncd n
	LEFT OUTER JOIN current_lifestyle cl 
		ON cl.encounter_id = n.encounter_id AND cl.current_lifestyle IS NOT NULL 
	WHERE cl.current_lifestyle IS NOT NULL 
	GROUP BY n.encounter_id, n.patient_id, n.date_of_visit::date),
last_current_lifestyle AS (
	SELECT 
		DISTINCT ON (c.patient_id, c.initial_encounter_id) c.patient_id,
		c.initial_encounter_id,
		c.initial_visit_date, 
		c.outcome_encounter_id,
		c.outcome_date, 
		clp.date_of_visit::date,
		clp.tobacco_use,
		clp.alcohol_use,
		clp.adequate_physical_activity,
		clp.healthy_diet
	FROM cohort c
	LEFT OUTER JOIN current_lifestyle_pivot clp 
		ON c.patient_id = clp.patient_id AND c.initial_visit_date <= clp.date_of_visit AND COALESCE(c.outcome_date, CURRENT_DATE) >= clp.date_of_visit
	WHERE clp.date_of_visit IS NOT NULL	
	GROUP BY c.patient_id, c.initial_encounter_id, c.initial_visit_date, c.outcome_encounter_id, c.outcome_date, clp.date_of_visit, clp.tobacco_use, clp.alcohol_use, clp.adequate_physical_activity,
		clp.healthy_diet
	ORDER BY c.patient_id, c.initial_encounter_id, c.initial_visit_date, clp.date_of_visit DESC),/*
-- The epilepsy history CTEs pivot past medical history data from the epilepsy details section horizontally from the NCD form. Only the last medical history is reported per cohort enrollment are present. 
epilepsy_history_pivot AS (
	SELECT 
		DISTINCT ON (n.encounter_id, n.patient_id, n.date::date) n.encounter_id, 
		n.patient_id, 
		n.date::date,
		MAX (CASE WHEN pmh.past_medical_history = 'Delayed milestones' THEN 1 ELSE NULL END) AS delayed_milestones,
		MAX (CASE WHEN pmh.past_medical_history = 'Cerebral malaria' THEN 1 ELSE NULL END) AS cerebral_malaria,
		MAX (CASE WHEN pmh.past_medical_history = 'Birth trauma' THEN 1 ELSE NULL END) AS birth_trauma,
		MAX (CASE WHEN pmh.past_medical_history = 'Neonatal sepsis' THEN 1 ELSE NULL END) AS neonatal_sepsis,
		MAX (CASE WHEN pmh.past_medical_history = 'Meningitis' THEN 1 ELSE NULL END) AS meningitis,
		MAX (CASE WHEN pmh.past_medical_history = 'Head Injury' THEN 1 ELSE NULL END) AS head_injury,
		MAX (CASE WHEN pmh.past_medical_history = 'Other' THEN 1 ELSE NULL END) AS other_epilepsy_history
	FROM ncd n
	LEFT OUTER JOIN past_medical_history pmh
		ON pmh.encounter_id = n.encounter_id AND pmh.past_medical_history IS NOT NULL 
	WHERE pmh.past_medical_history IS NOT NULL 
	GROUP BY n.encounter_id, n.patient_id, n.date::date),
last_epilepsy_history AS (
	SELECT 
		DISTINCT ON (c.patient_id, c.initial_encounter_id) c.patient_id,
		c.initial_encounter_id,
		c.initial_visit_date, 
		c.outcome_encounter_id,
		c.outcome_date, 
		ehp.date::date,
		ehp.delayed_milestones,
		ehp.cerebral_malaria,
		ehp.birth_trauma,
		ehp.neonatal_sepsis,
		ehp.meningitis,
		ehp.head_injury,
		ehp.other_epilepsy_history
	FROM cohort c
	LEFT OUTER JOIN epilepsy_history_pivot ehp 
		ON c.patient_id = ehp.patient_id AND c.initial_visit_date <= ehp.date AND COALESCE(c.outcome_date, CURRENT_DATE) >= ehp.date
	WHERE ehp.date IS NOT NULL	
	GROUP BY c.patient_id, c.initial_encounter_id, c.initial_visit_date, c.outcome_encounter_id, c.outcome_date, ehp.date, ehp.delayed_milestones, ehp.cerebral_malaria, ehp.birth_trauma, ehp.neonatal_sepsis, ehp.meningitis, ehp.head_injury, ehp.other_epilepsy_history
	ORDER BY c.patient_id, c.initial_encounter_id, c.initial_visit_date, ehp.date DESC),		
-- The hospitalised CTE checks there is a hospitlisation reported in visits taking place in the last 6 months. 
hospitalisation_last_6m AS (
	SELECT DISTINCT ON (c.patient_id, c.initial_encounter_id) c.patient_id,	c.initial_encounter_id, COUNT(n.hospitalised_since_last_visit) AS nb_hospitalised_last_6m, CASE WHEN n.hospitalised_since_last_visit IS NOT NULL THEN 'Yes' ELSE 'No' END AS hospitalised_last_6m
		FROM cohort c
		LEFT OUTER JOIN ncd n
			ON c.patient_id = n.patient_id AND c.initial_visit_date <= n.date::date AND COALESCE(c.outcome_date, CURRENT_DATE) >= n.date::date
		WHERE n.hospitalised_since_last_visit = 'Yes' and n.date <= current_date and n.date >= current_date - interval '6 months'
		GROUP BY c.patient_id, c.initial_encounter_id, n.hospitalised_since_last_visit),*/
-- The last foot exam CTE extracts the date of the last foot exam performed per cohort enrollment.
last_foot_exam AS (
	SELECT 
		DISTINCT ON (c.patient_id, c.initial_encounter_id) c.patient_id,
		c.initial_encounter_id,
		c.initial_visit_date, 
		c.outcome_encounter_id,
		c.outcome_date, 
		n.foot_assessment_outcome,
		n.date_of_visit::date AS last_foot_exam_date
	FROM cohort c
	LEFT OUTER JOIN ncd n
		ON c.patient_id = n.patient_id AND c.initial_visit_date <= n.date_of_visit::date AND COALESCE(c.outcome_date, CURRENT_DATE) >= n.date_of_visit::date
	WHERE n.foot_assessment_outcome != 'Not performed'
	ORDER BY c.patient_id, c.initial_encounter_id, n.patient_id, n.date_of_visit::date DESC),
-- The asthma severity CTE extracts the last asthma severity reported per cohort enrollment.
asthma_severity AS (
	SELECT 
		DISTINCT ON (c.patient_id, c.initial_encounter_id) c.patient_id,
		c.initial_encounter_id,
		c.initial_visit_date, 
		c.outcome_encounter_id,
		c.outcome_date, 
		n.date_of_visit::date,
		n.asthma_exacerbation_level
	FROM cohort c
	LEFT OUTER JOIN ncd n
		ON c.patient_id = n.patient_id AND c.initial_visit_date <= n.date_of_visit::date AND COALESCE(c.outcome_date, CURRENT_DATE) >= n.date_of_visit::date
	WHERE n.asthma_exacerbation_level IS NOT NULL
	ORDER BY c.patient_id, c.initial_encounter_id, n.patient_id, n.date_of_visit::date DESC),/*
-- The seizure onset CTE extracts the last age of seizure onset reported per cohort enrollment.
seizure_onset AS (
	SELECT 
		DISTINCT ON (c.patient_id, c.initial_encounter_id) c.patient_id,
		c.initial_encounter_id,
		c.initial_visit_date, 
		c.outcome_encounter_id,
		c.outcome_date, 
		n.date_of_visit::date,
		n.age_at_onset_of_seizure_in_years AS seizure_onset_age
	FROM cohort c
	LEFT OUTER JOIN ncd n
		ON c.patient_id = n.patient_id AND c.initial_visit_date <= n.date_of_visit::date AND COALESCE(c.outcome_date, CURRENT_DATE) >= n.date_of_visit::date
	WHERE n.age_at_onset_of_seizure_in_years IS NOT NULL
	ORDER BY c.patient_id, c.initial_encounter_id, n.patient_id, n.date_of_visit::date DESC),*/
-- The last NCD visit CTE extracts the last NCD visit data per cohort enrollment to look at if there are values reported for pregnancy, family planning, hospitalization, missed medication, seizures, or asthma/COPD exacerbations repoted at the last visit. 
last_ncd_form AS (
	SELECT 
		DISTINCT ON (c.patient_id, c.initial_encounter_id) c.patient_id,
		c.initial_encounter_id,
		c.initial_visit_date, 
		c.outcome_encounter_id,
		c.outcome_date, 
		n.date_of_visit::date AS last_ncd_form_date,
		DATE_PART('day',(now())-(n.date_of_visit::timestamp))::int AS days_since_last_visit,
		n.visit_type AS last_ncd_form_type,
		n.visit_location AS last_ncd_form_location,
		CASE WHEN n.currently_pregnant = 'Yes' THEN 'Yes' END AS pregnant_last_visit,
		--CASE WHEN n.family_planning_counseling = 'Yes' THEN 'Yes' END AS fp_last_visit,
		CASE WHEN n.hospitalised_since_last_visit = 'Yes' THEN 'Yes' END AS hospitalised_last_visit,
		--CASE WHEN n.missed_medication_doses_in_last_7_days = 'Yes' THEN 'Yes' END AS missed_medication_last_visit,
		CASE WHEN n.seizure_since_last_visit = 'Yes' THEN 'Yes' END AS seizures_last_visit--,
		--CASE WHEN n.exacerbation_per_week IS NOT NULL AND n.exacerbation_per_week > 0 THEN 'Yes' END AS exacerbations_last_visit,
		--n.exacerbation_per_week AS nb_exacerbations_last_visit
	FROM cohort c
	LEFT OUTER JOIN ncd n
		ON c.patient_id = n.patient_id AND c.initial_visit_date <= n.date_of_visit::date AND COALESCE(c.outcome_date, CURRENT_DATE) >= n.date_of_visit::date
	ORDER BY c.patient_id, c.initial_encounter_id, n.patient_id, n.date_of_visit::date DESC),
-- The last BP CTE extracts the last complete blood pressure measurements reported per cohort enrollment. Uses date reported on form. If no date is present, uses date of sample collection. If neither date or date of sample collection are present, results are not considered. 
last_bp AS (
	SELECT 
		DISTINCT ON (c.patient_id, c.initial_encounter_id) c.patient_id,
		c.initial_encounter_id,
		c.initial_visit_date, 
		c.outcome_encounter_id,
		c.outcome_date, 
		vli.date_vitals_taken AS last_bp_date,
		vli.systolic_blood_pressure,
		vli.diastolic_blood_pressure
	FROM cohort c
	LEFT OUTER JOIN vitals_and_laboratory_information vli
		ON c.patient_id = vli.patient_id AND c.initial_visit_date <= vli.date_vitals_taken AND COALESCE(c.outcome_date, CURRENT_DATE) >= vli.date_vitals_taken 
	WHERE vli.date_vitals_taken IS NOT NULL AND vli.systolic_blood_pressure IS NOT NULL AND vli.diastolic_blood_pressure IS NOT NULL
	ORDER BY c.patient_id, c.initial_encounter_id, vli.patient_id, vli.date_vitals_taken DESC),
-- The last BMI CTE extracts the last BMI measurement reported per cohort enrollment. Uses date reported on form. If no date is present, uses date of sample collection. If neither date or date of sample collection are present, results are not considered. 
last_bmi AS (
	SELECT 
		DISTINCT ON (c.patient_id, c.initial_encounter_id) c.patient_id,
		c.initial_encounter_id,
		c.initial_visit_date, 
		c.outcome_encounter_id,
		c.outcome_date, 
		vli.date_vitals_taken AS last_bmi_date,
		vli.bmi_kg_m2 AS last_bmi
	FROM cohort c
	LEFT OUTER JOIN vitals_and_laboratory_information vli
		ON c.patient_id = vli.patient_id AND c.initial_visit_date <= vli.date_vitals_taken AND COALESCE(c.outcome_date, CURRENT_DATE) >= vli.date_vitals_taken 
	WHERE vli.date_vitals_taken  IS NOT NULL AND vli.bmi_kg_m2 IS NOT NULL
	ORDER BY c.patient_id, c.initial_encounter_id, vli.patient_id, vli.date_vitals_taken  DESC),
-- The last fasting blood glucose CTE extracts the last fasting blood glucose measurement reported per cohort enrollment. Uses date of sample collection reported on form. If no date of sample collection is present, uses date of form. If neither date or date of sample collection are present, results are not considered. 
last_fbg AS (
	SELECT 
		DISTINCT ON (c.patient_id, c.initial_encounter_id) c.patient_id,
		c.initial_encounter_id,
		c.initial_visit_date, 
		c.outcome_encounter_id,
		c.outcome_date, 
		vli.date_of_sample_collection AS last_fbg_date,
		vli.fasting_blood_glucose AS last_fbg
	FROM cohort c
	LEFT OUTER JOIN vitals_and_laboratory_information vli
		ON c.patient_id = vli.patient_id AND c.initial_visit_date <= vli.date_of_sample_collection AND COALESCE(c.outcome_date, CURRENT_DATE) >= vli.date_of_sample_collection
	WHERE vli.date_of_sample_collection IS NOT NULL AND vli.fasting_blood_glucose IS NOT NULL
	ORDER BY c.patient_id, c.initial_encounter_id, vli.patient_id, vli.date_of_sample_collection DESC),
-- The last HbA1c CTE extracts the last fasting blood glucose measurement reported per cohort enrollment. Uses date of sample collection reported on form. If no date of sample collection is present, uses date of form. If neither date or date of sample collection are present, results are not considered. 
last_hba1c AS (
	SELECT 
		DISTINCT ON (c.patient_id, c.initial_encounter_id) c.patient_id,
		c.initial_encounter_id,
		c.initial_visit_date, 
		c.outcome_encounter_id,
		c.outcome_date, 
		vli.date_of_sample_collection AS last_hba1c_date, 
		vli.hba1c AS last_hba1c
	FROM cohort c
	LEFT OUTER JOIN vitals_and_laboratory_information vli
		ON c.patient_id = vli.patient_id AND c.initial_visit_date <= vli.date_of_sample_collection AND COALESCE(c.outcome_date, CURRENT_DATE) >= vli.date_of_sample_collection
	WHERE vli.date_of_sample_collection IS NOT NULL AND vli.hba1c IS NOT NULL
	ORDER BY c.patient_id, c.initial_encounter_id, vli.patient_id, vli.date_of_sample_collection DESC),/*
-- The last GFR CTE extracts the last GFR measurement reported per cohort enrollment. Uses date of sample collection reported on form. If no date of sample collection is present, uses date of form. If neither date or date of sample collection are present, results are not considered. 
last_gfr AS (
	SELECT 
		DISTINCT ON (c.patient_id, c.initial_encounter_id) c.patient_id,
		c.initial_encounter_id,
		c.initial_visit_date, 
		c.outcome_encounter_id,
		c.outcome_date, 
		vli.date_of_sample_collection AS last_gfr_date, 
		vli.gfr_ml_min_1_73m2 AS last_gfr
	FROM cohort c
	LEFT OUTER JOIN vitals_and_laboratory_information vli
		ON c.patient_id = vli.patient_id AND c.initial_visit_date <= vli.date_of_sample_collection AND COALESCE(c.outcome_date, CURRENT_DATE) >= vli.date_of_sample_collection
	WHERE vli.date_of_sample_collection IS NOT NULL AND vli.gfr_ml_min_1_73m2 IS NOT NULL
	ORDER BY c.patient_id, c.initial_encounter_id, vli.patient_id, vli.date_of_sample_collection DESC),*/
-- The last creatinine CTE extracts the last creatinine measurement reported per cohort enrollment. Uses date of sample collection reported on form. If no date of sample collection is present, uses date of form. If neither date or date of sample collection are present, results are not considered. 
last_creatinine AS (
	SELECT 
		DISTINCT ON (c.patient_id, c.initial_encounter_id) c.patient_id,
		c.initial_encounter_id,
		c.initial_visit_date, 
		c.outcome_encounter_id,
		c.outcome_date, 
		vli.date_of_sample_collection AS last_creatinine_date, 
		vli.creatinine AS last_creatinine
	FROM cohort c
	LEFT OUTER JOIN vitals_and_laboratory_information vli
		ON c.patient_id = vli.patient_id AND c.initial_visit_date <= vli.date_of_sample_collection AND COALESCE(c.outcome_date, CURRENT_DATE) >= vli.date_of_sample_collection
	WHERE vli.date_of_sample_collection IS NOT NULL AND vli.creatinine IS NOT NULL
	ORDER BY c.patient_id, c.initial_encounter_id, vli.patient_id, vli.date_of_sample_collection DESC),
-- The last urine protein CTE extracts the last urine protein result reported per cohort enrollment. Uses date of sample collection reported on form. If no date of sample collection is present, uses date of form. If neither date or date of sample collection are present, results are not considered. 
last_urine_protein AS (
	SELECT 
		DISTINCT ON (c.patient_id, c.initial_encounter_id) c.patient_id,
		c.initial_encounter_id,
		c.initial_visit_date, 
		c.outcome_encounter_id,
		c.outcome_date, 
		vli.date_of_sample_collection AS last_urine_protein_date, 
		vli.urine_protein AS last_urine_protein
	FROM cohort c
	LEFT OUTER JOIN vitals_and_laboratory_information vli
		ON c.patient_id = vli.patient_id AND c.initial_visit_date <= vli.date_of_sample_collection AND COALESCE(c.outcome_date, CURRENT_DATE) >= vli.date_of_sample_collection
	WHERE vli.date_of_sample_collection IS NOT NULL AND vli.urine_protein IS NOT NULL
	ORDER BY c.patient_id, c.initial_encounter_id, vli.patient_id, vli.date_of_sample_collection DESC),
-- The last HIV test CTE extracts the last HIV test result reported per cohort enrollment. Uses date of sample collection reported on form. If no date of sample collection is present, uses date of form. If neither date or date of sample collection are present, results are not considered. 
last_hiv AS (
	SELECT 
		DISTINCT ON (c.patient_id, c.initial_encounter_id) c.patient_id,
		c.initial_encounter_id,
		c.initial_visit_date, 
		c.outcome_encounter_id,
		c.outcome_date, 
		vli.date_of_hiv_test AS last_hiv_date, 
		vli.hiv_test_result AS last_hiv
	FROM cohort c
	LEFT OUTER JOIN vitals_and_laboratory_information vli
		ON c.patient_id = vli.patient_id AND c.initial_visit_date <= vli.date_of_hiv_test AND COALESCE(c.outcome_date, CURRENT_DATE) >= vli.date_of_hiv_test
	WHERE vli.date_of_hiv_test IS NOT NULL AND vli.hiv_test_result IS NOT NULL
	ORDER BY c.patient_id, c.initial_encounter_id, vli.patient_id, vli.date_of_hiv_test DESC),
last_cd4 AS (
	SELECT 
		DISTINCT ON (c.patient_id, c.initial_encounter_id) c.patient_id,
		c.initial_encounter_id,
		c.initial_visit_date, 
		c.outcome_encounter_id,
		c.outcome_date, 
		vli.date_of_cd4_sample_collection AS last_cd4_date, 
		vli.cd4_test AS last_cd4
	FROM cohort c
	LEFT OUTER JOIN vitals_and_laboratory_information vli
		ON c.patient_id = vli.patient_id AND c.initial_visit_date <= vli.date_of_cd4_sample_collection AND COALESCE(c.outcome_date, CURRENT_DATE) >= vli.date_of_cd4_sample_collection
	WHERE vli.date_of_cd4_sample_collection IS NOT NULL AND vli.cd4_test IS NOT NULL
	ORDER BY c.patient_id, c.initial_encounter_id, vli.patient_id, vli.date_of_cd4_sample_collection DESC)
-- Main query --
SELECT
	pi."Patient_Identifier",
	c.patient_id,
	c.initial_encounter_id,
	pa."patientFileNumber",
	pa."Patient_Unique_ID_CCC_No",
	pa."facilityForArt",
	pdd.age AS age_current,
	CASE 
		WHEN pdd.age::int <= 4 THEN '0-4'
		WHEN pdd.age::int >= 5 AND pdd.age::int <= 14 THEN '05-14'
		WHEN pdd.age::int >= 15 AND pdd.age::int <= 24 THEN '15-24'
		WHEN pdd.age::int >= 25 AND pdd.age::int <= 34 THEN '25-34'
		WHEN pdd.age::int >= 35 AND pdd.age::int <= 44 THEN '35-44'
		WHEN pdd.age::int >= 45 AND pdd.age::int <= 54 THEN '45-54'
		WHEN pdd.age::int >= 55 AND pdd.age::int <= 64 THEN '55-64'
		WHEN pdd.age::int >= 65 THEN '65+'
		ELSE NULL
	END AS age_group_current,
	EXTRACT(YEAR FROM age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy'))) AS age_admission,
	CASE 
		WHEN EXTRACT(YEAR FROM age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')))::int <= 4 THEN '0-4'
		WHEN EXTRACT(YEAR FROM age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')))::int >= 5 AND EXTRACT(YEAR FROM age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy'))) <= 14 THEN '05-14'
		WHEN EXTRACT(YEAR FROM age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')))::int >= 15 AND EXTRACT(YEAR FROM age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy'))) <= 24 THEN '15-24'
		WHEN EXTRACT(YEAR FROM age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')))::int >= 25 AND EXTRACT(YEAR FROM age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy'))) <= 34 THEN '25-34'
		WHEN EXTRACT(YEAR FROM age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')))::int >= 35 AND EXTRACT(YEAR FROM age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy'))) <= 44 THEN '35-44'
		WHEN EXTRACT(YEAR FROM age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')))::int >= 45 AND EXTRACT(YEAR FROM age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy'))) <= 54 THEN '45-54'
		WHEN EXTRACT(YEAR FROM age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')))::int >= 55 AND EXTRACT(YEAR FROM age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy'))) <= 64 THEN '55-64'
		WHEN EXTRACT(YEAR FROM age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')))::int >= 65 THEN '65+'
		ELSE NULL
	END AS age_group_admission,
	pdd.gender,
	--pa."patientCity" AS camp_location, 
	--pa."Legal_status",
	--pa."Civil_status",
	--pa."Education_level",
	pa."Occupation",
	--pa."Personal_Situation",
	--pa."Living_conditions",
	c.initial_visit_date AS enrollment_date,
	CASE WHEN c.outcome_date IS NULL THEN 'Yes' END AS in_cohort,
	CASE WHEN ((DATE_PART('year', CURRENT_DATE) - DATE_PART('year', c.initial_visit_date)) * 12 + (DATE_PART('month', CURRENT_DATE) - DATE_PART('month', c.initial_visit_date))) >= 6 AND c.outcome_date IS NULL THEN 'Yes' END AS in_cohort_6m,
	CASE WHEN ((DATE_PART('year', CURRENT_DATE) - DATE_PART('year', c.initial_visit_date)) * 12 + (DATE_PART('month', CURRENT_DATE) - DATE_PART('month', c.initial_visit_date))) >= 12 AND c.outcome_date IS NULL THEN 'Yes' END AS in_cohort_12m,
	c.readmission,
	c.initial_visit_location,
	lnf.last_ncd_form_location,
	lnf.last_ncd_form_date,
	lnf.last_ncd_form_type,	
	lnf.days_since_last_visit,
	c.outcome_date,
	c.patient_outcome,
	lnf.pregnant_last_visit,
	--lnf.fp_last_visit,
	lnf.hospitalised_last_visit,
	--lnf.missed_medication_last_visit,
	lnf.seizures_last_visit,
	--lnf.exacerbations_last_visit,
	--lnf.nb_exacerbations_last_visit,
	--h6m.nb_hospitalised_last_6m,
	--h6m.hospitalised_last_6m,
	lfe.last_foot_exam_date,
	asev.asthma_exacerbation_level,
	--so.seizure_onset_age,
	lbp.systolic_blood_pressure,
	lbp.diastolic_blood_pressure,
	CASE WHEN lbp.systolic_blood_pressure IS NOT NULL AND lbp.diastolic_blood_pressure IS NOT NULL THEN CONCAT(lbp.systolic_blood_pressure,'/',lbp.diastolic_blood_pressure) END AS blood_pressure,
	CASE WHEN lbp.systolic_blood_pressure <= 140 AND lbp.diastolic_blood_pressure <= 90 THEN 'Yes' WHEN lbp.systolic_blood_pressure > 140 OR lbp.diastolic_blood_pressure > 90 THEN 'No' END AS blood_pressure_control,
	lbp.last_bp_date,
	lbmi.last_bmi,
	lbmi.last_bmi_date,
	lfbg.last_fbg,
	lfbg.last_fbg_date,
	lbg.last_hba1c,
	CASE WHEN lbg.last_hba1c <= 6.5 THEN '0-6.5%' WHEN lbg.last_hba1c BETWEEN 6.6 AND 8 THEN '6.6-8.0%' WHEN lbg.last_hba1c > 8 THEN '>8%' END AS last_hba1c_grouping, 
	lbg.last_hba1c_date,
	CASE WHEN lbg.last_hba1c < 8 THEN 'Yes' WHEN lbg.last_hba1c >= 8 THEN 'No' WHEN lbg.last_hba1c IS NULL AND lfbg.last_fbg < 150 THEN 'Yes' WHEN lbg.last_hba1c IS NULL AND lfbg.last_fbg >= 150 THEN 'No' END AS diabetes_control,
	--lgfr.last_gfr,
	--lgfr.last_gfr_date,
	--CASE WHEN lgfr.last_gfr < 30 THEN 'Yes' WHEN lgfr.last_gfr >= 30 THEN 'No' END AS gfr_control,
	lc.last_creatinine,
	lc.last_creatinine_date,	
	lup.last_urine_protein,
	lup.last_urine_protein_date,
	lh.last_hiv,
	lh.last_hiv_date,
	last_cd4.last_cd4,
	last_cd4.last_cd4_date,
	ndx.asthma,
	ndx.copd,
	ndx.diabetes_type1,
	ndx.diabetes_type2,
	CASE WHEN ndx.diabetes_type1 IS NOT NULL OR ndx.diabetes_type2 IS NOT NULL THEN 1 END AS diabetes_any,
	ndx.hypertension,
	ndx.focal_epilepsy,
	ndx.generalised_epilepsy,
	ndx.unclassified_epilepsy,
	ndl.diagnosis_list,
	lrf.tobacco_use,
	lrf.alcohol_use,
	lrf.adequate_physical_activity,
	lrf.healthy_diet/*,
	leh.delayed_milestones,
	leh.cerebral_malaria,
	leh.birth_trauma,
	leh.neonatal_sepsis,
	leh.meningitis,
	leh.head_injury,
	leh.other_epilepsy_history*/
FROM cohort c
LEFT OUTER JOIN patient_identifier pi
	ON c.patient_id = pi.patient_id
LEFT OUTER JOIN person_attributes pa
	ON c.patient_id = pa.person_id
LEFT OUTER JOIN person_details_default pdd 
	ON c.patient_id = pdd.person_id
LEFT OUTER JOIN patient_encounter_details_default ped 
	ON c.initial_encounter_id = ped.encounter_id
LEFT OUTER JOIN ncd_diagnosis_pivot ndx
	ON c.initial_encounter_id = ndx.initial_encounter_id
LEFT OUTER JOIN ncd_diagnosis_list ndl
	ON c.initial_encounter_id = ndl.initial_encounter_id
LEFT OUTER JOIN last_current_lifestyle lrf
	ON c.initial_encounter_id = lrf.initial_encounter_id
/*LEFT OUTER JOIN last_epilepsy_history leh
	ON c.initial_encounter_id = leh.initial_encounter_id*/
LEFT OUTER JOIN last_ncd_form lnf
	ON c.initial_encounter_id = lnf.initial_encounter_id/*
LEFT OUTER JOIN hospitalisation_last_6m h6m
	ON c.initial_encounter_id = h6m.initial_encounter_id*/
LEFT OUTER JOIN last_foot_exam lfe
	ON c.initial_encounter_id = lfe.initial_encounter_id
LEFT OUTER JOIN asthma_severity asev
	ON c.initial_encounter_id = asev.initial_encounter_id/*
LEFT OUTER JOIN seizure_onset so
	ON c.initial_encounter_id = so.initial_encounter_id*/
LEFT OUTER JOIN last_bp lbp
	ON c.initial_encounter_id = lbp.initial_encounter_id
LEFT OUTER JOIN last_bmi lbmi
	ON c.initial_encounter_id = lbmi.initial_encounter_id
LEFT OUTER JOIN last_fbg lfbg
	ON c.initial_encounter_id = lfbg.initial_encounter_id
LEFT OUTER JOIN last_hba1c lbg
	ON c.initial_encounter_id = lbg.initial_encounter_id
--LEFT OUTER JOIN last_gfr lgfr
--	ON c.initial_encounter_id = lgfr.initial_encounter_id
LEFT OUTER JOIN last_creatinine lc
	ON c.initial_encounter_id = lc.initial_encounter_id
LEFT OUTER JOIN last_urine_protein lup
	ON c.initial_encounter_id = lup.initial_encounter_id
LEFT OUTER JOIN last_hiv lh
	ON c.initial_encounter_id = lh.initial_encounter_id
LEFT OUTER JOIN last_cd4
	ON c.initial_encounter_id = last_cd4.initial_encounter_id;