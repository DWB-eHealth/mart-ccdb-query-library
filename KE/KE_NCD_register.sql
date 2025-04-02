-- The first CTEs build the frame for patients entering and exiting the cohort. This frame is based on NCD forms with visit types of 'initial visit' and 'discharge visit'. The query takes all initial visit dates and matches discharge visit dates if the discharge visit date falls between the initial visit date and the next initial visit date (if present).
WITH initial AS (
	SELECT 
		patient_id, encounter_id AS initial_encounter_id, visit_location AS initial_visit_location, date_of_visit AS initial_visit_date, DENSE_RANK () OVER (PARTITION BY patient_id ORDER BY date_of_visit) AS initial_visit_order, LEAD (date_of_visit) OVER (PARTITION BY patient_id ORDER BY date_of_visit) AS next_initial_visit_date
	FROM ncd WHERE visit_type = 'Initial visit')
cohort AS (
	SELECT
		i.patient_id, i.initial_encounter_id, i.initial_visit_location, i.initial_visit_date, CASE WHEN i.initial_visit_order > 1 THEN 'Yes' END readmission, d.encounter_id AS discharge_encounter_id, d.discharge_date2 AS discharge_date, d.patient_outcome_at_end_of_msf_care
	FROM initial i
	LEFT JOIN (SELECT patient_id, encounter_id, COALESCE(patient_outcome_date::date, date_of_visit::date) AS discharge_date2, patient_outcome_at_end_of_msf_care FROM ncd WHERE visit_type = 'Patient outcome') d 
		ON i.patient_id = d.patient_id AND (d.discharge_date2 IS NULL OR (d.discharge_date2 >= i.initial_visit_date AND (d.discharge_date2 < i.next_initial_visit_date OR i.next_initial_visit_date IS NULL)))),
-- The last completed and missed appointment CTEs determine if a patient currently enrolled in the cohort has not attended their appointments.  
last_completed_appointment AS (
	SELECT patient_id, initial_encounter_id, appointment_start_time::date, appointment_service, appointment_location
	FROM (
		SELECT
			pad.patient_id,
			c.initial_encounter_id,
			pad.appointment_start_time,
			pad.appointment_service,
			pad.appointment_location,
			ROW_NUMBER() OVER (PARTITION BY pad.patient_id ORDER BY pad.appointment_start_time DESC) AS rn
		FROM patient_appointment_default pad
		LEFT OUTER JOIN cohort c
			ON pad.patient_id = c.patient_id AND c.initial_visit_date <= pad.appointment_start_time::date AND COALESCE(c.discharge_date, CURRENT_DATE) >= pad.appointment_start_time::date
		WHERE pad.appointment_start_time < now() AND (pad.appointment_status = 'Completed' OR pad.appointment_status = 'CheckedIn')) foo
	WHERE rn = 1 AND initial_encounter_id IS NOT NULL),
first_missed_appointment AS (
	SELECT patient_id, initial_encounter_id, appointment_start_time::date, appointment_service,rn
	FROM (
		SELECT
			pa.patient_id,
			c.initial_encounter_id,
			pa.appointment_start_time,
			pa.appointment_service,
			ROW_NUMBER() OVER (PARTITION BY pa.patient_id ORDER BY pa.appointment_start_time) AS rn
		FROM last_completed_appointment lca
		RIGHT JOIN patient_appointment_default pa
			ON lca.patient_id = pa.patient_id 
		LEFT OUTER JOIN cohort c
			ON pa.patient_id = c.patient_id AND c.initial_visit_date <= pa.appointment_start_time::date AND COALESCE(c.discharge_date, CURRENT_DATE) >= pa.appointment_start_time::date 
		WHERE pa.appointment_start_time > lca.appointment_start_time AND pa.appointment_status = 'Missed') foo
	WHERE rn = 1 AND initial_encounter_id IS NOT NULL),
last_form AS (
	SELECT patient_id, initial_encounter_id, last_form_date, last_form_type
	FROM (
		SELECT 
			c.patient_id,
			c.initial_encounter_id,
			nvsl.date AS last_form_date,
			nvsl.last_form_type AS last_form_type,
			ROW_NUMBER() OVER (PARTITION BY c.initial_encounter_id ORDER BY nvsl.date DESC, nvsl.last_form_type) AS rn
		FROM (
		    SELECT 
				patient_id, COALESCE(discharge_date, date) AS date, visit_type AS last_form_type 
			FROM ncd 
			UNION 
			SELECT patient_id, COALESCE(date, date_of_sample_collection) date, form_field_path AS last_form_type
			FROM vitals_and_laboratory_information
			ORDER BY last_form_type) nvsl
		LEFT OUTER JOIN cohort c
		    ON nvsl.patient_id = c.patient_id AND c.initial_visit_date <= nvsl.date AND COALESCE(c.discharge_date, CURRENT_DATE) >= nvsl.date) foo
    WHERE rn = 1 AND initial_encounter_id IS NOT NULL),
last_visit AS (
	SELECT
		c.patient_id,
		c.initial_encounter_id,
		c.initial_visit_date,
		c.discharge_date,
		c.patient_outcome,
		c.discharge_encounter_id,
		lca.appointment_start_time AS last_appointment_date,
		lca.appointment_service AS last_appointment_service,
		lca.appointment_location AS last_appointment_location,
		lf.last_form_date,
		lf.last_form_type,
		CASE 
		    WHEN lca.appointment_start_time::date > lf.last_form_date THEN lca.appointment_start_time::date 
		    WHEN lca.appointment_start_time <= lf.last_form_date THEN lf.last_form_date::date 
		    WHEN lca.appointment_start_time IS NOT NULL AND lf.last_form_date IS NULL THEN lca.appointment_start_time::date 
		    WHEN lca.appointment_start_time IS NULL AND lf.last_form_date IS NOT NULL THEN lf.last_form_date::date ELSE NULL END AS last_visit_date,
		CASE 
		    WHEN lca.appointment_start_time > lf.last_form_date THEN lca.appointment_service 
		    WHEN lca.appointment_start_time <= lf.last_form_date THEN lf.last_form_type 
		    WHEN lca.appointment_start_time IS NOT NULL AND lf.last_form_date IS NULL THEN lca.appointment_service 
		    WHEN lca.appointment_start_time IS NULL AND lf.last_form_date IS NOT NULL THEN lf.last_form_type ELSE NULL END AS last_visit_type,
		CASE 
		    WHEN c.discharge_encounter_id IS NULL AND lca.appointment_start_time > lf.last_form_date THEN (DATE_PART('day',(now())-(lca.appointment_start_time::timestamp)))::int 
		    WHEN c.discharge_encounter_id IS NULL AND lca.appointment_start_time <= lf.last_form_date THEN (DATE_PART('day',(now())-(lf.last_form_date::timestamp)))::int 
		    WHEN c.discharge_encounter_id IS NULL AND lca.appointment_start_time IS NOT NULL AND lf.last_form_date IS NULL THEN (DATE_PART('day',(now())-(lca.appointment_start_time::timestamp)))::int 
		    WHEN c.discharge_encounter_id IS NULL AND lca.appointment_start_time IS NULL AND lf.last_form_date IS NOT NULL THEN (DATE_PART('day',(now())-(lf.last_form_date::timestamp)))::int ELSE NULL END AS days_since_last_visit,
		CASE WHEN c.discharge_encounter_id IS NULL AND fma.appointment_start_time > lf.last_form_date OR lf.last_form_date IS NULL AND fma.appointment_start_time IS NOT NULL THEN fma.appointment_start_time::date ELSE NULL END AS last_missed_appointment_date,
		CASE WHEN c.discharge_encounter_id IS NULL AND fma.appointment_start_time > lf.last_form_date OR lf.last_form_date IS NULL AND fma.appointment_start_time IS NOT NULL THEN fma.appointment_service ELSE NULL END AS last_missed_appointment_service,
		CASE WHEN c.discharge_encounter_id IS NULL AND fma.appointment_start_time > lf.last_form_date OR lf.last_form_date IS NULL AND fma.appointment_start_time IS NOT NULL THEN (DATE_PART('day',(now())-(fma.appointment_start_time::timestamp)))::int ELSE NULL END AS days_since_last_missed_appointment
	FROM cohort c
	LEFT OUTER JOIN last_completed_appointment lca 
		ON c.initial_encounter_id = lca.initial_encounter_id
	LEFT OUTER JOIN first_missed_appointment fma
		ON c.initial_encounter_id = fma.initial_encounter_id
	LEFT OUTER JOIN last_form lf
		ON c.initial_encounter_id = lf.initial_encounter_id),		
-- The NCD diagnosis CTEs extract all NCD diagnoses for patients reported between their initial visit and discharge visit. Diagnoses are only reported once. For specific disease groups, the second CTE extracts only the last reported diagnosis among the groups. These groups include types of diabetes, types of epilespy, and hyper-/hypothyroidism. The final CTE pivotes the diagnoses horizontally.
cohort_diagnosis AS (
	SELECT
		c.patient_id, c.initial_encounter_id, n.date, d.ncdiagnosis AS diagnosis
	FROM ncdiagnosis d 
	LEFT JOIN ncd n USING(encounter_id)
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
    WHERE rn = 1),
ncd_diagnosis_pivot AS (
	SELECT 
		DISTINCT ON (initial_encounter_id, patient_id) initial_encounter_id, 
		patient_id,
		MAX (CASE WHEN diagnosis = 'Asthma' THEN 1 ELSE NULL END) AS asthma,
		MAX (CASE WHEN diagnosis = 'Chronic kidney disease' THEN 1 ELSE NULL END) AS chronic_kidney_disease,
		MAX (CASE WHEN diagnosis = 'Cardiovascular disease' THEN 1 ELSE NULL END) AS cardiovascular_disease,
		MAX (CASE WHEN diagnosis = 'Chronic obstructive pulmonary disease' THEN 1 ELSE NULL END) AS copd,
		MAX (CASE WHEN diagnosis = 'Diabetes mellitus, type 1' THEN 1 ELSE NULL END) AS diabetes_type1,
		MAX (CASE WHEN diagnosis = 'Diabetes mellitus, type 2' THEN 1 ELSE NULL END) AS diabetes_type2,
		MAX (CASE WHEN diagnosis = 'Hypertension' THEN 1 ELSE NULL END) AS hypertension,
		MAX (CASE WHEN diagnosis = 'Hypothyroidism' THEN 1 ELSE NULL END) AS hypothyroidism,
		MAX (CASE WHEN diagnosis = 'Hyperthyroidism' THEN 1 ELSE NULL END) AS hyperthyroidism,
		MAX (CASE WHEN diagnosis = 'Focal epilepsy' THEN 1 ELSE NULL END) AS focal_epilepsy,
		MAX (CASE WHEN diagnosis = 'Generalised epilepsy' THEN 1 ELSE NULL END) AS generalised_epilepsy,
		MAX (CASE WHEN diagnosis = 'Unclassified epilepsy' THEN 1 ELSE NULL END) AS unclassified_epilepsy,
		MAX (CASE WHEN diagnosis = 'Other' THEN 1 ELSE NULL END) AS other_ncd
	FROM cohort_diagnosis_last
	GROUP BY initial_encounter_id, patient_id),
ncd_diagnosis_list AS (
	SELECT initial_encounter_id, STRING_AGG(diagnosis, ', ') AS diagnosis_list
	FROM cohort_diagnosis_last
	GROUP BY initial_encounter_id),
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
		ON c.patient_id = nrfp.patient_id AND c.initial_visit_date <= nrfp.date AND COALESCE(c.discharge_date, CURRENT_DATE) >= nrfp.date
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
		ON c.patient_id = ehp.patient_id AND c.initial_visit_date <= ehp.date AND COALESCE(c.discharge_date, CURRENT_DATE) >= ehp.date
	WHERE ehp.date IS NOT NULL	
	GROUP BY c.patient_id, c.initial_encounter_id, c.initial_visit_date, c.discharge_encounter_id, c.discharge_date, ehp.date, ehp.delayed_milestones, ehp.cerebral_malaria, ehp.birth_trauma, ehp.neonatal_sepsis, ehp.meningitis, ehp.head_injury, ehp.other_epilepsy_history
	ORDER BY c.patient_id, c.initial_encounter_id, c.initial_visit_date, ehp.date DESC),		
-- The hospitalised CTE checks there is a hospitlisation reported in visits taking place in the last 6 months. 
hospitalisation_last_6m AS (
	SELECT DISTINCT ON (c.patient_id, c.initial_encounter_id) c.patient_id,	c.initial_encounter_id, COUNT(n.hospitalised_since_last_visit) AS nb_hospitalised_last_6m, CASE WHEN n.hospitalised_since_last_visit IS NOT NULL THEN 'Yes' ELSE 'No' END AS hospitalised_last_6m
		FROM cohort c
		LEFT OUTER JOIN ncd n
			ON c.patient_id = n.patient_id AND c.initial_visit_date <= n.date::date AND COALESCE(c.discharge_date, CURRENT_DATE) >= n.date::date
		WHERE n.hospitalised_since_last_visit = 'Yes' and n.date <= current_date and n.date >= current_date - interval '6 months'
		GROUP BY c.patient_id, c.initial_encounter_id, n.hospitalised_since_last_visit),
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
		ON c.patient_id = n.patient_id AND c.initial_visit_date <= n.date::date AND COALESCE(c.discharge_date, CURRENT_DATE) >= n.date::date
	WHERE n.eye_exam_performed = 'Yes'
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
		ON c.patient_id = n.patient_id AND c.initial_visit_date <= n.date::date AND COALESCE(c.discharge_date, CURRENT_DATE) >= n.date::date
	WHERE n.foot_exam_performed = 'Yes'
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
		ON c.patient_id = n.patient_id AND c.initial_visit_date <= n.date::date AND COALESCE(c.discharge_date, CURRENT_DATE) >= n.date::date
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
		ON c.patient_id = n.patient_id AND c.initial_visit_date <= n.date::date AND COALESCE(c.discharge_date, CURRENT_DATE) >= n.date::date
	WHERE n.age_at_onset_of_seizure_in_years IS NOT NULL
	ORDER BY c.patient_id, c.initial_encounter_id, n.patient_id, n.date::date DESC),
-- The last NCD visit CTE extracts the last NCD visit data per cohort enrollment to look at if there are values reported for pregnancy, family planning, hospitalization, missed medication, seizures, or asthma/COPD exacerbations repoted at the last visit. 
last_ncd_form AS (
	SELECT 
		DISTINCT ON (c.patient_id, c.initial_encounter_id) c.patient_id,
		c.initial_encounter_id,
		c.initial_visit_date, 
		c.discharge_encounter_id,
		c.discharge_date, 
		n.date::date AS last_form_date,
		n.visit_type AS last_form_type,
		CASE WHEN n.currently_pregnant = 'Yes' THEN 'Yes' END AS pregnant_last_visit,
		CASE WHEN n.family_planning_counseling = 'Yes' THEN 'Yes' END AS fp_last_visit,
		CASE WHEN n.hospitalised_since_last_visit = 'Yes' THEN 'Yes' END AS hospitalised_last_visit,
		CASE WHEN n.missed_medication_doses_in_last_7_days = 'Yes' THEN 'Yes' END AS missed_medication_last_visit,
		CASE WHEN n.seizures_since_last_visit = 'Yes' THEN 'Yes' END AS seizures_last_visit,
		CASE WHEN n.exacerbation_per_week IS NOT NULL AND n.exacerbation_per_week > 0 THEN 'Yes' END AS exacerbations_last_visit,
		n.exacerbation_per_week AS nb_exacerbations_last_visit
	FROM cohort c
	LEFT OUTER JOIN ncd n
		ON c.patient_id = n.patient_id AND c.initial_visit_date <= n.date::date AND COALESCE(c.discharge_date, CURRENT_DATE) >= n.date::date
	ORDER BY c.patient_id, c.initial_encounter_id, n.patient_id, n.date::date DESC),
-- The last visit location CTE finds the last visit location reported in NCD forms.
last_form_location AS (	
	SELECT 
		DISTINCT ON (c.patient_id, c.initial_encounter_id, c.initial_visit_date, c.discharge_date) c.initial_encounter_id,
		nvsl.date AS last_form_date,
		nvsl.visit_location AS last_form_location
	FROM cohort c
	LEFT OUTER JOIN (SELECT patient_id, date, visit_location FROM NCD UNION SELECT patient_id, COALESCE(date_of_sample_collection, date) AS date, location_name AS visit_location FROM vitals_and_laboratory_information) nvsl
		ON c.patient_id = nvsl.patient_id AND c.initial_visit_date <= nvsl.date::date AND COALESCE(c.discharge_date, CURRENT_DATE) >= nvsl.date::date
	WHERE nvsl.visit_location IS NOT NULL
	GROUP BY c.patient_id, c.initial_encounter_id, c.initial_visit_date, c.discharge_date, nvsl.date, nvsl.visit_location
	ORDER BY c.patient_id, c.initial_encounter_id, c.initial_visit_date, c.discharge_date, nvsl.date DESC),
-- The last BP CTE extracts the last complete blood pressure measurements reported per cohort enrollment. Uses date reported on form. If no date is present, uses date of sample collection. If neither date or date of sample collection are present, results are not considered. 
last_bp AS (
	SELECT 
		DISTINCT ON (c.patient_id, c.initial_encounter_id) c.patient_id,
		c.initial_encounter_id,
		c.initial_visit_date, 
		c.discharge_encounter_id,
		c.discharge_date, 
		COALESCE(vli.date, vli.date_of_sample_collection) AS last_bp_date,
		vli.systolic_blood_pressure,
		vli.diastolic_blood_pressure
	FROM cohort c
	LEFT OUTER JOIN vitals_and_laboratory_information vli
		ON c.patient_id = vli.patient_id AND c.initial_visit_date <= COALESCE(vli.date, vli.date_of_sample_collection) AND COALESCE(c.discharge_date, CURRENT_DATE) >= COALESCE(vli.date, vli.date_of_sample_collection) 
	WHERE COALESCE(vli.date, vli.date_of_sample_collection) IS NOT NULL AND vli.systolic_blood_pressure IS NOT NULL AND vli.diastolic_blood_pressure IS NOT NULL
	ORDER BY c.patient_id, c.initial_encounter_id, vli.patient_id, COALESCE(vli.date, vli.date_of_sample_collection) DESC),
-- The last BMI CTE extracts the last BMI measurement reported per cohort enrollment. Uses date reported on form. If no date is present, uses date of sample collection. If neither date or date of sample collection are present, results are not considered. 
last_bmi AS (
	SELECT 
		DISTINCT ON (c.patient_id, c.initial_encounter_id) c.patient_id,
		c.initial_encounter_id,
		c.initial_visit_date, 
		c.discharge_encounter_id,
		c.discharge_date, 
		COALESCE(vli.date, vli.date_of_sample_collection) AS last_bmi_date,
		vli.bmi_kg_m2 AS last_bmi
	FROM cohort c
	LEFT OUTER JOIN vitals_and_laboratory_information vli
		ON c.patient_id = vli.patient_id AND c.initial_visit_date <= COALESCE(vli.date, vli.date_of_sample_collection) AND COALESCE(c.discharge_date, CURRENT_DATE) >= COALESCE(vli.date, vli.date_of_sample_collection) 
	WHERE COALESCE(vli.date, vli.date_of_sample_collection) IS NOT NULL AND vli.bmi_kg_m2 IS NOT NULL
	ORDER BY c.patient_id, c.initial_encounter_id, vli.patient_id, COALESCE(vli.date, vli.date_of_sample_collection) DESC),
-- The last fasting blood glucose CTE extracts the last fasting blood glucose measurement reported per cohort enrollment. Uses date of sample collection reported on form. If no date of sample collection is present, uses date of form. If neither date or date of sample collection are present, results are not considered. 
last_fbg AS (
	SELECT 
		DISTINCT ON (c.patient_id, c.initial_encounter_id) c.patient_id,
		c.initial_encounter_id,
		c.initial_visit_date, 
		c.discharge_encounter_id,
		c.discharge_date, 
		COALESCE(vli.date_of_sample_collection, vli.date) AS last_fbg_date,
		vli.fasting_blood_glucose_mg_dl AS last_fbg
	FROM cohort c
	LEFT OUTER JOIN vitals_and_laboratory_information vli
		ON c.patient_id = vli.patient_id AND c.initial_visit_date <= COALESCE(vli.date_of_sample_collection, vli.date) AND COALESCE(c.discharge_date, CURRENT_DATE) >= COALESCE(vli.date_of_sample_collection, vli.date)
	WHERE COALESCE(vli.date, vli.date_of_sample_collection) IS NOT NULL AND vli.fasting_blood_glucose_mg_dl IS NOT NULL
	ORDER BY c.patient_id, c.initial_encounter_id, vli.patient_id, COALESCE(vli.date_of_sample_collection, vli.date) DESC),
-- The last HbA1c CTE extracts the last fasting blood glucose measurement reported per cohort enrollment. Uses date of sample collection reported on form. If no date of sample collection is present, uses date of form. If neither date or date of sample collection are present, results are not considered. 
last_hba1c AS (
	SELECT 
		DISTINCT ON (c.patient_id, c.initial_encounter_id) c.patient_id,
		c.initial_encounter_id,
		c.initial_visit_date, 
		c.discharge_encounter_id,
		c.discharge_date, 
		COALESCE(vli.date_of_sample_collection, vli.date) AS last_hba1c_date, 
		vli.hba1c AS last_hba1c
	FROM cohort c
	LEFT OUTER JOIN vitals_and_laboratory_information vli
		ON c.patient_id = vli.patient_id AND c.initial_visit_date <= COALESCE(vli.date_of_sample_collection, vli.date) AND COALESCE(c.discharge_date, CURRENT_DATE) >= COALESCE(vli.date_of_sample_collection, vli.date)
	WHERE COALESCE(vli.date, vli.date_of_sample_collection) IS NOT NULL AND vli.hba1c IS NOT NULL
	ORDER BY c.patient_id, c.initial_encounter_id, vli.patient_id, COALESCE(vli.date_of_sample_collection, vli.date) DESC),
-- The last GFR CTE extracts the last GFR measurement reported per cohort enrollment. Uses date of sample collection reported on form. If no date of sample collection is present, uses date of form. If neither date or date of sample collection are present, results are not considered. 
last_gfr AS (
	SELECT 
		DISTINCT ON (c.patient_id, c.initial_encounter_id) c.patient_id,
		c.initial_encounter_id,
		c.initial_visit_date, 
		c.discharge_encounter_id,
		c.discharge_date, 
		COALESCE(vli.date_of_sample_collection, vli.date) AS last_gfr_date, 
		vli.gfr_ml_min_1_73m2 AS last_gfr
	FROM cohort c
	LEFT OUTER JOIN vitals_and_laboratory_information vli
		ON c.patient_id = vli.patient_id AND c.initial_visit_date <= COALESCE(vli.date_of_sample_collection, vli.date) AND COALESCE(c.discharge_date, CURRENT_DATE) >= COALESCE(vli.date_of_sample_collection, vli.date)
	WHERE COALESCE(vli.date, vli.date_of_sample_collection) IS NOT NULL AND vli.gfr_ml_min_1_73m2 IS NOT NULL
	ORDER BY c.patient_id, c.initial_encounter_id, vli.patient_id, COALESCE(vli.date_of_sample_collection, vli.date) DESC),
-- The last creatinine CTE extracts the last creatinine measurement reported per cohort enrollment. Uses date of sample collection reported on form. If no date of sample collection is present, uses date of form. If neither date or date of sample collection are present, results are not considered. 
last_creatinine AS (
	SELECT 
		DISTINCT ON (c.patient_id, c.initial_encounter_id) c.patient_id,
		c.initial_encounter_id,
		c.initial_visit_date, 
		c.discharge_encounter_id,
		c.discharge_date, 
		COALESCE(vli.date_of_sample_collection, vli.date) AS last_creatinine_date, 
		vli.creatinine_mg_dl AS last_creatinine
	FROM cohort c
	LEFT OUTER JOIN vitals_and_laboratory_information vli
		ON c.patient_id = vli.patient_id AND c.initial_visit_date <= COALESCE(vli.date_of_sample_collection, vli.date) AND COALESCE(c.discharge_date, CURRENT_DATE) >= COALESCE(vli.date_of_sample_collection, vli.date)
	WHERE COALESCE(vli.date, vli.date_of_sample_collection) IS NOT NULL AND vli.creatinine_mg_dl IS NOT NULL
	ORDER BY c.patient_id, c.initial_encounter_id, vli.patient_id, COALESCE(vli.date_of_sample_collection, vli.date) DESC),
-- The last urine protein CTE extracts the last urine protein result reported per cohort enrollment. Uses date of sample collection reported on form. If no date of sample collection is present, uses date of form. If neither date or date of sample collection are present, results are not considered. 
last_urine_protein AS (
	SELECT 
		DISTINCT ON (c.patient_id, c.initial_encounter_id) c.patient_id,
		c.initial_encounter_id,
		c.initial_visit_date, 
		c.discharge_encounter_id,
		c.discharge_date, 
		COALESCE(vli.date_of_sample_collection, vli.date) AS last_urine_protein_date, 
		vli.urine_protein AS last_urine_protein
	FROM cohort c
	LEFT OUTER JOIN vitals_and_laboratory_information vli
		ON c.patient_id = vli.patient_id AND c.initial_visit_date <= COALESCE(vli.date_of_sample_collection, vli.date) AND COALESCE(c.discharge_date, CURRENT_DATE) >= COALESCE(vli.date_of_sample_collection, vli.date)
	WHERE COALESCE(vli.date, vli.date_of_sample_collection) IS NOT NULL AND vli.urine_protein IS NOT NULL
	ORDER BY c.patient_id, c.initial_encounter_id, vli.patient_id, COALESCE(vli.date_of_sample_collection, vli.date) DESC),
-- The last HIV test CTE extracts the last HIV test result reported per cohort enrollment. Uses date of sample collection reported on form. If no date of sample collection is present, uses date of form. If neither date or date of sample collection are present, results are not considered. 
last_hiv AS (
	SELECT 
		DISTINCT ON (c.patient_id, c.initial_encounter_id) c.patient_id,
		c.initial_encounter_id,
		c.initial_visit_date, 
		c.discharge_encounter_id,
		c.discharge_date, 
		COALESCE(vli.date_of_sample_collection, vli.date) AS last_hiv_date, 
		vli.hiv_test AS last_hiv
	FROM cohort c
	LEFT OUTER JOIN vitals_and_laboratory_information vli
		ON c.patient_id = vli.patient_id AND c.initial_visit_date <= COALESCE(vli.date_of_sample_collection, vli.date) AND COALESCE(c.discharge_date, CURRENT_DATE) >= COALESCE(vli.date_of_sample_collection, vli.date)
	WHERE COALESCE(vli.date, vli.date_of_sample_collection) IS NOT NULL AND vli.hiv_test IS NOT NULL
	ORDER BY c.patient_id, c.initial_encounter_id, vli.patient_id, COALESCE(vli.date_of_sample_collection, vli.date) DESC),
-- The next appointment CTE extracts the next appointment date for all patients currently enrolled in the cohort (excludes patients with a discharge).  
next_appointment AS (
	SELECT patient_id, appointment_start_time, appointment_service, appointment_location
	FROM (
		SELECT
			patient_id,
			appointment_start_time,
			appointment_service,
			appointment_location,
			ROW_NUMBER() OVER (PARTITION BY patient_id ORDER BY appointment_start_time ASC) AS rn
		FROM patient_appointment_default
		WHERE appointment_start_time > now()) foo
	WHERE rn = 1)
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
	CASE WHEN ((DATE_PART('year', CURRENT_DATE) - DATE_PART('year', c.initial_visit_date)) * 12 + (DATE_PART('month', CURRENT_DATE) - DATE_PART('month', c.initial_visit_date))) >= 6 AND c.discharge_date IS NULL THEN 'Yes' END AS in_cohort_6m,
	CASE WHEN ((DATE_PART('year', CURRENT_DATE) - DATE_PART('year', c.initial_visit_date)) * 12 + (DATE_PART('month', CURRENT_DATE) - DATE_PART('month', c.initial_visit_date))) >= 12 AND c.discharge_date IS NULL THEN 'Yes' END AS in_cohort_12m,
	CASE WHEN c.initial_visit_date IS NOT NULL AND c.discharge_date IS NULL AND c.patient_outcome IS NULL AND lv.days_since_last_visit < 90 THEN 'Yes' WHEN c.initial_visit_date IS NOT NULL AND c.discharge_date IS NULL AND c.patient_outcome IS NULL AND lv.days_since_last_visit >= 90 AND (lv.days_since_last_missed_appointment IS NULL OR lv.days_since_last_missed_appointment < 90) THEN 'Yes' ELSE NULL END AS active_patient,
	CASE WHEN c.initial_visit_date IS NOT NULL AND c.discharge_date IS NULL AND c.patient_outcome IS NULL AND lv.days_since_last_missed_appointment >= 90 AND lv.days_since_last_missed_appointment <= lv.days_since_last_visit THEN 'Yes' ELSE NULL END AS inactive_patient,
	CASE WHEN c.discharge_date IS NULL THEN na.appointment_start_time::date END AS next_appointment,
	CASE WHEN c.discharge_date IS NULL THEN na.appointment_service END AS next_appointment_service,
	CASE WHEN c.discharge_date IS NULL THEN na.appointment_location END AS next_appointment_location,
	c.readmission,
	c.initial_visit_location,
	lfl.last_form_location,
	lv.last_appointment_location,
	CASE WHEN lfl.last_form_location IS NOT NULL AND lv.last_appointment_location IS NULL THEN lfl.last_form_location WHEN lfl.last_form_location IS NULL AND lv.last_appointment_location IS NOT NULL THEN lv.last_appointment_location WHEN lfl.last_form_date > lv.last_appointment_date AND lfl.last_form_location IS NOT NULL AND lv.last_appointment_location IS NOT NULL THEN lfl.last_form_location WHEN lfl.last_form_date <= lv.last_appointment_date AND lfl.last_form_location IS NOT NULL AND lv.last_appointment_location IS NOT NULL THEN lv.last_appointment_location ELSE NULL END AS last_visit_location,
	lv.last_form_date,
	lv.last_form_type,	
	lv.last_appointment_date,
	lv.last_appointment_service,
	lv.last_visit_date,
	lv.last_visit_type,
	lv.days_since_last_visit,
	lv.last_missed_appointment_date,
	lv.last_missed_appointment_service,
	lv.days_since_last_missed_appointment,
	c.discharge_date,
	c.patient_outcome,
	lnf.pregnant_last_visit,
	lnf.fp_last_visit,
	lnf.hospitalised_last_visit,
	lnf.missed_medication_last_visit,
	lnf.seizures_last_visit,
	lnf.exacerbations_last_visit,
	lnf.nb_exacerbations_last_visit,
	h6m.nb_hospitalised_last_6m,
	h6m.hospitalised_last_6m,
	lee.last_eye_exam_date,
	lfe.last_foot_exam_date,
	asev.asthma_severity,
	so.seizure_onset_age,
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
	lgfr.last_gfr,
	lgfr.last_gfr_date,
	CASE WHEN lgfr.last_gfr < 30 THEN 'Yes' WHEN lgfr.last_gfr >= 30 THEN 'No' END AS gfr_control,
	lc.last_creatinine,
	lc.last_creatinine_date,	
	lup.last_urine_protein,
	lup.last_urine_protein_date,
	lh.last_hiv,
	lh.last_hiv_date,
	ndx.asthma,
	ndx.chronic_kidney_disease,
	ndx.cardiovascular_disease,
	ndx.copd,
	ndx.diabetes_type1,
	ndx.diabetes_type2,
	CASE WHEN ndx.diabetes_type1 IS NOT NULL OR ndx.diabetes_type2 IS NOT NULL THEN 1 END AS diabetes_any,
	ndx.hypertension,
	ndx.hypothyroidism,
	ndx.hyperthyroidism,		
	ndx.focal_epilepsy,
	ndx.generalised_epilepsy,
	ndx.unclassified_epilepsy,
	ndx.other_ncd,
	ndl.diagnosis_list,
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
LEFT OUTER JOIN last_visit lv
	ON c.initial_encounter_id = lv.initial_encounter_id
LEFT OUTER JOIN ncd_diagnosis_pivot ndx
	ON c.initial_encounter_id = ndx.initial_encounter_id
LEFT OUTER JOIN ncd_diagnosis_list ndl
	ON c.initial_encounter_id = ndl.initial_encounter_id
LEFT OUTER JOIN last_risk_factors lrf
	ON c.initial_encounter_id = lrf.initial_encounter_id
LEFT OUTER JOIN last_epilepsy_history leh
	ON c.initial_encounter_id = leh.initial_encounter_id
LEFT OUTER JOIN last_ncd_form lnf
	ON c.initial_encounter_id = lnf.initial_encounter_id
LEFT OUTER JOIN hospitalisation_last_6m h6m
	ON c.initial_encounter_id = h6m.initial_encounter_id
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
LEFT OUTER JOIN last_urine_protein lup
	ON c.initial_encounter_id = lup.initial_encounter_id
LEFT OUTER JOIN last_hiv lh
	ON c.initial_encounter_id = lh.initial_encounter_id
LEFT OUTER JOIN last_form_location lfl
	ON c.initial_encounter_id = lfl.initial_encounter_id
LEFT OUTER JOIN next_appointment na 
	ON c.patient_id = na.patient_id;