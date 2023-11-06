-- The first CTE build the frame for patients entering and exiting the cohort. This frame is based on NCD forms with visit types of 'initial visit' and 'discharge visit'. The query takes all initial visit dates and matches discharge visit dates if the discharge visit date falls between the initial visit date and the next initial visit date (if present).
WITH initial AS (
	SELECT 
		patient_id, encounter_id AS initial_encounter_id, visit_location AS initial_visit_location, date AS initial_visit_date, DENSE_RANK () OVER (PARTITION BY patient_id ORDER BY date) AS initial_visit_order, LEAD (date) OVER (PARTITION BY patient_id ORDER BY date) AS next_initial_visit_date
	FROM ncd WHERE visit_type = 'Initial visit'),
cohort AS (
	SELECT
		i.patient_id, i.initial_encounter_id, i.initial_visit_location, i.initial_visit_date, CASE WHEN i.initial_visit_order > 1 THEN 'Yes' END readmission, d.encounter_id AS discharge_encounter_id, d.date AS discharge_date, d.ncd_hep_b_patient_outcome AS patient_outcome
	FROM initial i
	LEFT JOIN (SELECT patient_id, date, encounter_id, ncd_hep_b_patient_outcome FROM ncd WHERE visit_type = 'Discharge visit') d 
		ON i.patient_id = d.patient_id AND d.date >= i.initial_visit_date AND (d.date < i.next_initial_visit_date OR i.next_initial_visit_date IS NULL)),
-- The diagnosis CTEs pivot diagnosis data horizontally from the NCD form. Only the last diagnoses are reported per cohort enrollment are present. 
ncd_diagnosis_pivot AS (
	SELECT 
		DISTINCT ON (n.encounter_id, n.patient_id, n.date::date) n.encounter_id, 
		n.patient_id, 
		n.date::date,
		MAX (CASE WHEN d.ncdiagnosis = 'Asthma' THEN 1 ELSE NULL END) AS asthma,
		MAX (CASE WHEN d.ncdiagnosis = 'Chronic kidney disease' THEN 1 ELSE NULL END) AS chronic_kidney_disease,
		MAX (CASE WHEN d.ncdiagnosis = 'Cardiovascular disease' THEN 1 ELSE NULL END) AS cardiovascular_disease,
		MAX (CASE WHEN d.ncdiagnosis = 'Chronic obstructive pulmonary disease' THEN 1 ELSE NULL END) AS copd,
		MAX (CASE WHEN d.ncdiagnosis = 'Diabetes mellitus, type 1' THEN 1 ELSE NULL END) AS diabetes_type1,
		MAX (CASE WHEN d.ncdiagnosis = 'Diabetes mellitus, type 2' THEN 1 ELSE NULL END) AS diabetes_type2,
		MAX (CASE WHEN d.ncdiagnosis = 'Hypertension' THEN 1 ELSE NULL END) AS hypertension,
		MAX (CASE WHEN d.ncdiagnosis = 'Hypothyroidism' THEN 1 ELSE NULL END) AS hypothyroidism,
		MAX (CASE WHEN d.ncdiagnosis = 'Hyperthyroidism' THEN 1 ELSE NULL END) AS hyperthyroidism,
		MAX (CASE WHEN d.ncdiagnosis = 'Focal epilepsy' THEN 1 ELSE NULL END) AS focal_epilepsy,
		MAX (CASE WHEN d.ncdiagnosis = 'Generalised epilepsy' THEN 1 ELSE NULL END) AS generalised_epilepsy,
		MAX (CASE WHEN d.ncdiagnosis = 'Unclassified epilepsy' THEN 1 ELSE NULL END) AS unclassified_epilepsy,
		MAX (CASE WHEN d.ncdiagnosis = 'Other' THEN 1 ELSE NULL END) AS other_ncd
	FROM ncd n
	LEFT OUTER JOIN ncdiagnosis d 
		ON d.encounter_id = n.encounter_id AND d.ncdiagnosis IS NOT NULL 
	WHERE d.ncdiagnosis IS NOT NULL 
	GROUP BY n.encounter_id, n.patient_id, n.date::date),
last_ncd_diagnosis AS (
	SELECT 
		DISTINCT ON (c.patient_id, c.initial_encounter_id) c.patient_id,
		c.initial_encounter_id,
		c.initial_visit_date, 
		c.discharge_encounter_id,
		c.discharge_date, 
		ndp.date::date,
		ndp.asthma,
		ndp.chronic_kidney_disease,
		ndp.cardiovascular_disease,
		ndp.copd,
		ndp.diabetes_type1,
		ndp.diabetes_type2,
		ndp.hypertension,
		ndp.hypothyroidism,
		ndp.hyperthyroidism,		
		ndp.focal_epilepsy,
		ndp.generalised_epilepsy,
		ndp.unclassified_epilepsy,
		ndp.other_ncd
	FROM cohort c
	LEFT OUTER JOIN ncd_diagnosis_pivot ndp 
		ON c.patient_id = ndp.patient_id AND c.initial_visit_date <= ndp.date AND CASE WHEN c.discharge_date IS NOT NULL THEN c.discharge_date ELSE current_date END >= ndp.date
	WHERE ndp.date IS NOT NULL	
	GROUP BY c.patient_id, c.initial_encounter_id, c.initial_visit_date, c.discharge_encounter_id, c.discharge_date, ndp.date, ndp.asthma, ndp.chronic_kidney_disease, ndp.cardiovascular_disease, ndp.copd, ndp.diabetes_type1, ndp.diabetes_type2, ndp.hypertension, ndp.hypothyroidism, ndp.hyperthyroidism, ndp.focal_epilepsy, ndp.generalised_epilepsy, ndp.unclassified_epilepsy, ndp.other_ncd 
	ORDER BY c.patient_id, c.initial_encounter_id, c.initial_visit_date, ndp.date DESC),
-- The risk factor CTEs pivot risk factor data horizontally from the NCD form. Only the last risk factors are reported per cohort enrollment are present. 
ncd_risk_factors_pivot AS (
	SELECT 
		DISTINCT ON (n.encounter_id, n.patient_id, n.date::date) n.encounter_id, 
		n.patient_id, 
		n.date::date,
		MAX (CASE WHEN rfn.risk_factor_noted = 'Occupational exposure' THEN 1 ELSE NULL END) AS occupational_exposure,
		MAX (CASE WHEN rfn.risk_factor_noted = 'Traditional medicine' THEN 1 ELSE NULL END) AS traditional_medicine,
		MAX (CASE WHEN rfn.risk_factor_noted = 'Second-hand smoking' THEN 1 ELSE NULL END) AS secondhand_smoking,
		MAX (CASE WHEN rfn.risk_factor_noted = 'Smoker' THEN 1 ELSE NULL END) AS smoker,
		MAX (CASE WHEN rfn.risk_factor_noted = 'Kitchen smoke' THEN 1 ELSE NULL END) AS kitchen_smoke,
		MAX (CASE WHEN rfn.risk_factor_noted = 'Alcohol use' THEN 1 ELSE NULL END) AS alcohol_use,
		MAX (CASE WHEN rfn.risk_factor_noted = 'Other' THEN 1 ELSE NULL END) AS other_risk_factor
	FROM ncd n
	LEFT OUTER JOIN risk_factor_noted rfn 
		ON rfn.encounter_id = n.encounter_id AND rfn.risk_factor_noted IS NOT NULL 
	WHERE rfn.risk_factor_noted IS NOT NULL 
	GROUP BY n.encounter_id, n.patient_id, n.date::date),
last_risk_factors AS (
	SELECT 
		DISTINCT ON (c.patient_id, c.initial_encounter_id) c.patient_id,
		c.initial_encounter_id,
		c.initial_visit_date, 
		c.discharge_encounter_id,
		c.discharge_date, 
		nrfp.date::date,
		nrfp.occupational_exposure,
		nrfp.traditional_medicine,
		nrfp.secondhand_smoking,
		nrfp.smoker,
		nrfp.kitchen_smoke,
		nrfp.alcohol_use,
		nrfp.other_risk_factor
	FROM cohort c
	LEFT OUTER JOIN ncd_risk_factors_pivot nrfp 
		ON c.patient_id = nrfp.patient_id AND c.initial_visit_date <= nrfp.date AND CASE WHEN c.discharge_date IS NOT NULL THEN c.discharge_date ELSE current_date END >= nrfp.date
	WHERE nrfp.date IS NOT NULL	
	GROUP BY c.patient_id, c.initial_encounter_id, c.initial_visit_date, c.discharge_encounter_id, c.discharge_date, nrfp.date, nrfp.occupational_exposure, nrfp.traditional_medicine, nrfp.secondhand_smoking, nrfp.smoker, nrfp.kitchen_smoke, nrfp.alcohol_use, nrfp.other_risk_factor
	ORDER BY c.patient_id, c.initial_encounter_id, c.initial_visit_date, nrfp.date DESC),
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
		c.discharge_encounter_id,
		c.discharge_date, 
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
		ON c.patient_id = ehp.patient_id AND c.initial_visit_date <= ehp.date AND CASE WHEN c.discharge_date IS NOT NULL THEN c.discharge_date ELSE current_date END >= ehp.date
	WHERE ehp.date IS NOT NULL	
	GROUP BY c.patient_id, c.initial_encounter_id, c.initial_visit_date, c.discharge_encounter_id, c.discharge_date, ehp.date, ehp.delayed_milestones, ehp.cerebral_malaria, ehp.birth_trauma, ehp.neonatal_sepsis, ehp.meningitis, ehp.head_injury, ehp.other_epilepsy_history
	ORDER BY c.patient_id, c.initial_encounter_id, c.initial_visit_date, ehp.date DESC),
-- The last NCD visit CTE extracts the last NCD visit data per cohort enrollment to look at if there are values reported for pregnancy, family planning, hospitalization, missed medication, seizures, or asthma/COPD exacerbations repoted at the last visit. 
last_ncd_visit AS (
SELECT 
		DISTINCT ON (c.patient_id, c.initial_encounter_id) c.patient_id,
		c.initial_encounter_id,
		c.initial_visit_date, 
		c.discharge_encounter_id,
		c.discharge_date, 
		n.date::date AS last_visit_date,
		n.visit_type AS last_visit_type,
		n.currently_pregnant AS pregnant_last_visit,
		n.family_planning_counseling AS fp_last_visit,
		n.hospitalised_since_last_visit AS hospitalised_last_visit,
		n.missed_medication_doses_in_last_7_days AS missed_medication_last_visit,
		n.seizures_since_last_visit AS seizures_last_visit,
		n.exacerbation_per_week AS exacerbations_last_visit
	FROM cohort c
	LEFT OUTER JOIN ncd n
		ON c.patient_id = n.patient_id AND c.initial_visit_date <= n.date::date AND CASE WHEN c.discharge_date IS NOT NULL THEN c.discharge_date ELSE current_date END >= n.date::date
	ORDER BY c.patient_id, c.initial_encounter_id, n.patient_id, n.date::date DESC),		
-- The last eye exam CTE extracts the date of the last eye exam performed per cohort enrollment.
last_eye_exam AS (
SELECT 
		DISTINCT ON (c.patient_id, c.initial_encounter_id) c.patient_id,
		c.initial_encounter_id,
		c.initial_visit_date, 
		c.discharge_encounter_id,
		c.discharge_date, 
		n.date::date AS last_eye_exam_date
	FROM cohort c
	LEFT OUTER JOIN ncd n
		ON c.patient_id = n.patient_id AND c.initial_visit_date <= n.date::date AND CASE WHEN c.discharge_date IS NOT NULL THEN c.discharge_date ELSE current_date END >= n.date::date
	WHERE n.eye_exam_performed IS NOT NULL
	ORDER BY c.patient_id, c.initial_encounter_id, n.patient_id, n.date::date DESC),
-- The last foot exam CTE extracts the date of the last eye exam performed per cohort enrollment.
last_foot_exam AS (
SELECT 
		DISTINCT ON (c.patient_id, c.initial_encounter_id) c.patient_id,
		c.initial_encounter_id,
		c.initial_visit_date, 
		c.discharge_encounter_id,
		c.discharge_date, 
		n.date::date AS last_foot_exam_date
	FROM cohort c
	LEFT OUTER JOIN ncd n
		ON c.patient_id = n.patient_id AND c.initial_visit_date <= n.date::date AND CASE WHEN c.discharge_date IS NOT NULL THEN c.discharge_date ELSE current_date END >= n.date::date
	WHERE n.foot_exam_performed IS NOT NULL
	ORDER BY c.patient_id, c.initial_encounter_id, n.patient_id, n.date::date DESC),
-- The asthma severity CTE extracts the last asthma severity reported per cohort enrollment.
asthma_severity AS (
SELECT 
		DISTINCT ON (c.patient_id, c.initial_encounter_id) c.patient_id,
		c.initial_encounter_id,
		c.initial_visit_date, 
		c.discharge_encounter_id,
		c.discharge_date, 
		n.date::date,
		n.asthma_severity
	FROM cohort c
	LEFT OUTER JOIN ncd n
		ON c.patient_id = n.patient_id AND c.initial_visit_date <= n.date::date AND CASE WHEN c.discharge_date IS NOT NULL THEN c.discharge_date ELSE current_date END >= n.date::date
	WHERE n.asthma_severity IS NOT NULL
	ORDER BY c.patient_id, c.initial_encounter_id, n.patient_id, n.date::date DESC),
-- The seizure onset CTE extracts the last age of seizure onset reported per cohort enrollment.
seizure_onset AS (
SELECT 
		DISTINCT ON (c.patient_id, c.initial_encounter_id) c.patient_id,
		c.initial_encounter_id,
		c.initial_visit_date, 
		c.discharge_encounter_id,
		c.discharge_date, 
		n.date::date,
		n.age_at_onset_of_seizure_in_years AS seizure_onset_age
	FROM cohort c
	LEFT OUTER JOIN ncd n
		ON c.patient_id = n.patient_id AND c.initial_visit_date <= n.date::date AND CASE WHEN c.discharge_date IS NOT NULL THEN c.discharge_date ELSE current_date END >= n.date::date
	WHERE n.age_at_onset_of_seizure_in_years IS NOT NULL
	ORDER BY c.patient_id, c.initial_encounter_id, n.patient_id, n.date::date DESC),
-- The last visit location CTE finds the last visit location reported in NCD forms.
last_visit_location AS (	
	SELECT 
		DISTINCT ON (c.patient_id, c.initial_encounter_id, c.initial_visit_date, c.discharge_date) c.initial_encounter_id,
		ncd.visit_location AS last_visit_location
	FROM cohort c
	LEFT OUTER JOIN ncd
		ON c.patient_id = ncd.patient_id AND c.initial_visit_date <= ncd.date::date AND CASE WHEN c.discharge_date IS NOT NULL THEN c.discharge_date ELSE current_date END >= ncd.date::date
	WHERE ncd.visit_location IS NOT NULL
	GROUP BY c.patient_id, c.initial_encounter_id, c.initial_visit_date, c.discharge_date, ncd.date, ncd.visit_location
	ORDER BY c.patient_id, c.initial_encounter_id, c.initial_visit_date, c.discharge_date, ncd.date DESC),
-- The last BP CTE extracts the last complete blood pressure measurements reported per cohort enrollment. Uses date reported on form. If no date is present, uses date of sample collection. If neither date or date of sample collection are present, results are not considered. 
last_bp AS (
	SELECT 
		DISTINCT ON (c.patient_id, c.initial_encounter_id) c.patient_id,
		c.initial_encounter_id,
		c.initial_visit_date, 
		c.discharge_encounter_id,
		c.discharge_date, 
		CASE WHEN vli.date IS NOT NULL THEN vli.date::date ELSE vli.date_of_sample_collection::date END AS last_bp_date,
		vli.systolic_blood_pressure,
		vli.diastolic_blood_pressure
	FROM cohort c
	LEFT OUTER JOIN vitals_and_laboratory_information vli
		ON c.patient_id = vli.patient_id AND c.initial_visit_date <= vli.date::date AND CASE WHEN c.discharge_date IS NOT NULL THEN c.discharge_date ELSE current_date END >= vli.date::date
	WHERE COALESCE(vli.date, vli.date_of_sample_collection) IS NOT NULL AND vli.systolic_blood_pressure IS NOT NULL AND vli.diastolic_blood_pressure IS NOT NULL
	ORDER BY c.patient_id, c.initial_encounter_id, vli.patient_id, vli.date::date DESC),
-- The last BMI CTE extracts the last BMI measurement reported per cohort enrollment. Uses date reported on form. If no date is present, uses date of sample collection. If neither date or date of sample collection are present, results are not considered. 
last_bmi AS (
	SELECT 
		DISTINCT ON (c.patient_id, c.initial_encounter_id) c.patient_id,
		c.initial_encounter_id,
		c.initial_visit_date, 
		c.discharge_encounter_id,
		c.discharge_date, 
		CASE WHEN vli.date IS NOT NULL THEN vli.date::date ELSE vli.date_of_sample_collection::date END AS last_bmi_date,
		vli.bmi_kg_m2 AS last_bmi
	FROM cohort c
	LEFT OUTER JOIN vitals_and_laboratory_information vli
		ON c.patient_id = vli.patient_id AND c.initial_visit_date <= vli.date::date AND CASE WHEN c.discharge_date IS NOT NULL THEN c.discharge_date ELSE current_date END >= vli.date::date
	WHERE COALESCE(vli.date, vli.date_of_sample_collection) IS NOT NULL AND vli.bmi_kg_m2 IS NOT NULL
	ORDER BY c.patient_id, c.initial_encounter_id, vli.patient_id, vli.date::date DESC),
-- The last fasting blood glucose CTE extracts the last fasting blood glucose measurement reported per cohort enrollment. Uses date of sample collection reported on form. If no date of sample collection is present, uses date of form. If neither date or date of sample collection are present, results are not considered. 
last_fbg AS (
	SELECT 
		DISTINCT ON (c.patient_id, c.initial_encounter_id) c.patient_id,
		c.initial_encounter_id,
		c.initial_visit_date, 
		c.discharge_encounter_id,
		c.discharge_date, 
		CASE WHEN vli.date_of_sample_collection IS NOT NULL THEN vli.date_of_sample_collection::date ELSE vli.date::date END AS last_fbg_date,
		vli.fasting_blood_glucose_mg_dl AS last_fbg
	FROM cohort c
	LEFT OUTER JOIN vitals_and_laboratory_information vli
		ON c.patient_id = vli.patient_id AND c.initial_visit_date <= vli.date::date AND CASE WHEN c.discharge_date IS NOT NULL THEN c.discharge_date ELSE current_date END >= vli.date::date
	WHERE COALESCE(vli.date, vli.date_of_sample_collection) IS NOT NULL AND vli.fasting_blood_glucose_mg_dl IS NOT NULL
	ORDER BY c.patient_id, c.initial_encounter_id, vli.patient_id, vli.date::date DESC),
-- The last HbA1c CTE extracts the last fasting blood glucose measurement reported per cohort enrollment. Uses date of sample collection reported on form. If no date of sample collection is present, uses date of form. If neither date or date of sample collection are present, results are not considered. 
last_hba1c AS (
	SELECT 
		DISTINCT ON (c.patient_id, c.initial_encounter_id) c.patient_id,
		c.initial_encounter_id,
		c.initial_visit_date, 
		c.discharge_encounter_id,
		c.discharge_date, 
		CASE WHEN vli.date_of_sample_collection IS NOT NULL THEN vli.date_of_sample_collection::date ELSE vli.date::date END AS last_hba1c_date, 
		vli.hba1c AS last_hba1c
	FROM cohort c
	LEFT OUTER JOIN vitals_and_laboratory_information vli
		ON c.patient_id = vli.patient_id AND c.initial_visit_date <= vli.date::date AND CASE WHEN c.discharge_date IS NOT NULL THEN c.discharge_date ELSE current_date END >= vli.date::date
	WHERE COALESCE(vli.date, vli.date_of_sample_collection) IS NOT NULL AND vli.hba1c IS NOT NULL
	ORDER BY c.patient_id, c.initial_encounter_id, vli.patient_id, vli.date::date DESC),
-- The last GFR CTE extracts the last GFR measurement reported per cohort enrollment. Uses date of sample collection reported on form. If no date of sample collection is present, uses date of form. If neither date or date of sample collection are present, results are not considered. 
last_gfr AS (
	SELECT 
		DISTINCT ON (c.patient_id, c.initial_encounter_id) c.patient_id,
		c.initial_encounter_id,
		c.initial_visit_date, 
		c.discharge_encounter_id,
		c.discharge_date, 
		CASE WHEN vli.date_of_sample_collection IS NOT NULL THEN vli.date_of_sample_collection::date ELSE vli.date::date END AS last_gfr_date, 
		vli.gfr_ml_min_1_73m2 AS last_gfr
	FROM cohort c
	LEFT OUTER JOIN vitals_and_laboratory_information vli
		ON c.patient_id = vli.patient_id AND c.initial_visit_date <= vli.date::date AND CASE WHEN c.discharge_date IS NOT NULL THEN c.discharge_date ELSE current_date END >= vli.date::date
	WHERE COALESCE(vli.date, vli.date_of_sample_collection) IS NOT NULL AND vli.gfr_ml_min_1_73m2 IS NOT NULL
	ORDER BY c.patient_id, c.initial_encounter_id, vli.patient_id, vli.date::date DESC),
-- The last creatinine CTE extracts the last creatinine measurement reported per cohort enrollment. Uses date of sample collection reported on form. If no date of sample collection is present, uses date of form. If neither date or date of sample collection are present, results are not considered. 
last_creatinine AS (
	SELECT 
		DISTINCT ON (c.patient_id, c.initial_encounter_id) c.patient_id,
		c.initial_encounter_id,
		c.initial_visit_date, 
		c.discharge_encounter_id,
		c.discharge_date, 
		CASE WHEN vli.date_of_sample_collection IS NOT NULL THEN vli.date_of_sample_collection::date ELSE vli.date::date END AS last_creatinine_date, 
		vli.creatinine_mg_dl AS last_creatinine
	FROM cohort c
	LEFT OUTER JOIN vitals_and_laboratory_information vli
		ON c.patient_id = vli.patient_id AND c.initial_visit_date <= vli.date::date AND CASE WHEN c.discharge_date IS NOT NULL THEN c.discharge_date ELSE current_date END >= vli.date::date
	WHERE COALESCE(vli.date, vli.date_of_sample_collection) IS NOT NULL AND vli.creatinine_mg_dl IS NOT NULL
	ORDER BY c.patient_id, c.initial_encounter_id, vli.patient_id, vli.date::date DESC)
-- Main query --
SELECT
	pi."Patient_Identifier",
	c.patient_id,
	c.initial_encounter_id,
	pa."Other_patient_identifier",
	pa."Previous_MSF_code",
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
	EXTRACT(YEAR FROM (SELECT age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')))) AS age_admission,
	CASE 
		WHEN EXTRACT(YEAR FROM (SELECT age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy'))))::int <= 4 THEN '0-4'
		WHEN EXTRACT(YEAR FROM (SELECT age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy'))))::int >= 5 AND EXTRACT(YEAR FROM (SELECT age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')))) <= 14 THEN '05-14'
		WHEN EXTRACT(YEAR FROM (SELECT age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy'))))::int >= 15 AND EXTRACT(YEAR FROM (SELECT age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')))) <= 24 THEN '15-24'
		WHEN EXTRACT(YEAR FROM (SELECT age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy'))))::int >= 25 AND EXTRACT(YEAR FROM (SELECT age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')))) <= 34 THEN '25-34'
		WHEN EXTRACT(YEAR FROM (SELECT age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy'))))::int >= 35 AND EXTRACT(YEAR FROM (SELECT age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')))) <= 44 THEN '35-44'
		WHEN EXTRACT(YEAR FROM (SELECT age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy'))))::int >= 45 AND EXTRACT(YEAR FROM (SELECT age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')))) <= 54 THEN '45-54'
		WHEN EXTRACT(YEAR FROM (SELECT age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy'))))::int >= 55 AND EXTRACT(YEAR FROM (SELECT age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')))) <= 64 THEN '55-64'
		WHEN EXTRACT(YEAR FROM (SELECT age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy'))))::int >= 65 THEN '65+'
		ELSE NULL
	END AS age_group_admission,
	pdd.gender,
	pa."patientCity" AS camp_location, 
	pa."Legal_status",
	pa."Civil_status",
	pa."Education_level",
	pa."Occupation",
	pa."Personal_Situation",
	pa."Living_conditions",
	c.initial_visit_date AS enrollment_date,
	CASE WHEN c.discharge_date IS NULL THEN 'Yes' END AS in_cohort,
	CASE WHEN ((DATE_PART('year', CURRENT_DATE) - DATE_PART('year', c.initial_visit_date)) * 12 + (DATE_PART('month', CURRENT_DATE) - DATE_PART('month', c.initial_visit_date))) >= 6 THEN 'Yes' END AS in_cohort_6m,
	CASE WHEN ((DATE_PART('year', CURRENT_DATE) - DATE_PART('year', c.initial_visit_date)) * 12 + (DATE_PART('month', CURRENT_DATE) - DATE_PART('month', c.initial_visit_date))) >= 12 THEN 'Yes' END AS in_cohort_12m,
	c.readmission,
	c.initial_visit_location,
	lvl.last_visit_location,
	c.discharge_date,
	c.patient_outcome,
	lnv.last_visit_date,
	lnv.last_visit_type,
	lnv.pregnant_last_visit,
	lnv.fp_last_visit,
	lnv.hospitalised_last_visit,
	lnv.missed_medication_last_visit,
	lnv.seizures_last_visit,
	lnv.exacerbations_last_visit,
	lee.last_eye_exam_date,
	lfe.last_foot_exam_date,
	asev.asthma_severity,
	so.seizure_onset_age,
	lbp.systolic_blood_pressure,
	lbp.diastolic_blood_pressure,
	CONCAT(lbp.systolic_blood_pressure,'/',lbp.diastolic_blood_pressure) AS blood_pressure,
	CASE WHEN lbp.systolic_blood_pressure <= 140 AND lbp.diastolic_blood_pressure <= 40 THEN 'Yes' END AS blood_pressure_controlled,
	lbp.last_bp_date,
	lbmi.last_bmi,
	lbmi.last_bmi_date,
	lfbg.last_fbg,
	lfbg.last_fbg_date,
	lbg.last_hba1c,
	CASE WHEN lbg.last_hba1c <= 6.5 THEN '0-6.5%' WHEN lbg.last_hba1c BETWEEN 6.6 AND 8 THEN '6.6-8.0%' WHEN lbg.last_hba1c > 8 THEN '>8%' END AS last_hba1c_grouping, 
	lbg.last_hba1c_date,
	CASE WHEN lbg.last_hba1c < 8 THEN 'Yes' WHEN lbg.last_hba1c IS NULL AND lfbg.last_fbg < 150 THEN 'Yes' END AS diabetes_control,
	lgfr.last_gfr,
	lgfr.last_gfr_date,
	lc.last_creatinine,
	lc.last_creatinine_date,	
	lndx.asthma,
	lndx.chronic_kidney_disease,
	lndx.cardiovascular_disease,
	lndx.copd,
	lndx.diabetes_type1,
	lndx.diabetes_type2,
	CASE WHEN lndx.diabetes_type1 IS NOT NULL OR lndx.diabetes_type2 IS NOT NULL THEN 1 END AS diabetes_any,
	lndx.hypertension,
	lndx.hypothyroidism,
	lndx.hyperthyroidism,		
	lndx.focal_epilepsy,
	lndx.generalised_epilepsy,
	lndx.unclassified_epilepsy,
	lndx.other_ncd,
	lrf.occupational_exposure,
	lrf.traditional_medicine,
	lrf.secondhand_smoking,
	lrf.smoker,
	lrf.kitchen_smoke,
	lrf.alcohol_use,
	lrf.other_risk_factor,
	leh.delayed_milestones,
	leh.cerebral_malaria,
	leh.birth_trauma,
	leh.neonatal_sepsis,
	leh.meningitis,
	leh.head_injury,
	leh.other_epilepsy_history
FROM cohort c
LEFT OUTER JOIN patient_identifier pi
	ON c.patient_id = pi.patient_id
LEFT OUTER JOIN person_attributes pa
	ON c.patient_id = pa.person_id
LEFT OUTER JOIN person_details_default pdd 
	ON c.patient_id = pdd.person_id
LEFT OUTER JOIN patient_encounter_details_default ped 
	ON c.initial_encounter_id = ped.encounter_id
LEFT OUTER JOIN last_ncd_diagnosis lndx
	ON c.initial_encounter_id = lndx.initial_encounter_id
LEFT OUTER JOIN last_risk_factors lrf
	ON c.initial_encounter_id = lrf.initial_encounter_id
LEFT OUTER JOIN last_epilepsy_history leh
	ON c.initial_encounter_id = leh.initial_encounter_id
LEFT OUTER JOIN last_ncd_visit lnv
	ON c.initial_encounter_id = lnv.initial_encounter_id
LEFT OUTER JOIN last_eye_exam lee
	ON c.initial_encounter_id = lee.initial_encounter_id
LEFT OUTER JOIN last_foot_exam lfe
	ON c.initial_encounter_id = lfe.initial_encounter_id
LEFT OUTER JOIN asthma_severity asev
	ON c.initial_encounter_id = asev.initial_encounter_id
LEFT OUTER JOIN seizure_onset so
	ON c.initial_encounter_id = so.initial_encounter_id
LEFT OUTER JOIN last_bp lbp
	ON c.initial_encounter_id = lbp.initial_encounter_id
LEFT OUTER JOIN last_bmi lbmi
	ON c.initial_encounter_id = lbmi.initial_encounter_id
LEFT OUTER JOIN last_fbg lfbg
	ON c.initial_encounter_id = lfbg.initial_encounter_id
LEFT OUTER JOIN last_hba1c lbg
	ON c.initial_encounter_id = lbg.initial_encounter_id
LEFT OUTER JOIN last_gfr lgfr
	ON c.initial_encounter_id = lgfr.initial_encounter_id
LEFT OUTER JOIN last_creatinine lc
	ON c.initial_encounter_id = lc.initial_encounter_id
LEFT OUTER JOIN last_visit_location lvl
	ON c.initial_encounter_id = lvl.initial_encounter_id;