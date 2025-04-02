-- The initial and cohort CTEs build the frame for patients entering and exiting the cohort. This frame is based on the HepC form with visit type of 'initial visit'. The query takes all initial visit dates and matches the next initial visit date (if present) or the current date. 
WITH initial AS (
	SELECT 
		patient_id, encounter_id AS initial_encounter_id, visit_location AS initial_visit_location, date AS initial_visit_date, DENSE_RANK () OVER (PARTITION BY patient_id ORDER BY date) AS initial_visit_order, LEAD (date) OVER (PARTITION BY patient_id ORDER BY date) AS next_initial_visit_date
	FROM hepatitis_c WHERE visit_type = 'Initial visit'),
cohort AS (
	SELECT
		i1.patient_id, i1.initial_encounter_id, i1.initial_visit_location, i1.initial_visit_date, CASE WHEN i1.initial_visit_order > 1 THEN 'Yes' END readmission, COALESCE(i2.initial_visit_date, CURRENT_DATE) AS end_date
	FROM initial i1
	LEFT OUTER JOIN initial i2
		ON i1.patient_id = i2.patient_id AND  i1.initial_visit_order = (i2.initial_visit_order - 1)),
-- The treatment failure CTE extracts the first treatment failure date, if present, and PCR result 12 weeks post treatment completion for each patient. 
treatment_failure AS (
	SELECT initial_encounter_id, first_treatment_failure, hcv_pcr_12_weeks_after_treatment_end
	FROM (
		SELECT 
			c.patient_id,
			c.initial_encounter_id,
			tf.date::date AS first_treatment_failure,
			tf.hcv_pcr_12_weeks_after_treatment_end,
			ROW_NUMBER() OVER (PARTITION BY tf.patient_id ORDER BY tf.date) AS rn
		FROM cohort c
		LEFT OUTER JOIN hepatitis_c tf
			ON c.patient_id = tf.patient_id AND c.initial_visit_date <= tf.date::date AND c.end_date >= tf.date::date
		WHERE tf.patient_outcome = 'Treatment failure') foo
	WHERE rn = 1),
-- The last discharge CTE extracts the last discharge information for the patient, including both the last patient outcome and the PCR result 12 weeks post treatment completion for each patient.
last_discharge AS (
	SELECT initial_encounter_id, patient_outcome, last_pcr_12w
	FROM (
		SELECT
			c.patient_id,
			c.initial_encounter_id,
			d.patient_outcome,
			d.hcv_pcr_12_weeks_after_treatment_end AS last_pcr_12w,
			ROW_NUMBER() OVER (PARTITION BY d.patient_id ORDER BY d.date DESC) AS rn
		FROM cohort c
		LEFT OUTER JOIN hepatitis_c d 
			ON c.patient_id = d.patient_id AND c.initial_visit_date <= d.date::date AND c.end_date >= d.date::date
		WHERE d.visit_type = 'Discharge visit') foo
	WHERE rn = 1),
-- The last appointment / form / visit CTEs extract the last appointment or form reported for each patient and identify if a patient currently enrolled in the cohort has not attended their last appointments.  
last_completed_appointment AS (
	SELECT patient_id, appointment_start_time, appointment_service, appointment_location
	FROM (
		SELECT
			patient_id,
			appointment_start_time,
			appointment_service,
			appointment_location,
			ROW_NUMBER() OVER (PARTITION BY patient_id ORDER BY appointment_start_time DESC) AS rn
		FROM patient_appointment_default
		WHERE appointment_start_time < now() AND (appointment_status = 'Completed' OR appointment_status = 'CheckedIn')) foo
	WHERE rn = 1),
first_missed_appointment AS (
	SELECT patient_id, appointment_start_time, appointment_service
	FROM (
		SELECT
			pa.patient_id,
			pa.appointment_start_time,
			pa.appointment_service,
			ROW_NUMBER() OVER (PARTITION BY pa.patient_id ORDER BY pa.appointment_start_time) AS rn
		FROM last_completed_appointment lca
		RIGHT JOIN patient_appointment_default pa
			ON lca.patient_id = pa.patient_id 
		WHERE pa.appointment_start_time > lca.appointment_start_time AND pa.appointment_status = 'Missed') foo
	WHERE rn = 1),
last_form AS (
	SELECT initial_encounter_id, last_form_date, last_form_type 
	FROM (
		SELECT 
			c.patient_id,
			c.initial_encounter_id,
			nvsl.date AS last_form_date,
			nvsl.last_form_type AS last_form_type,
			ROW_NUMBER() OVER (PARTITION BY nvsl.patient_id ORDER BY nvsl.date DESC) AS rn
		FROM cohort c
		LEFT OUTER JOIN (
			SELECT 
				patient_id, COALESCE(discharge_date, date) AS date, visit_type AS last_form_type 
			FROM hepatitis_c 
			UNION 
			SELECT patient_id, date, form_field_path AS last_form_type
			FROM vitals_and_laboratory_information) nvsl
			ON c.patient_id = nvsl.patient_id AND c.initial_visit_date <= nvsl.date::date AND c.end_date >= nvsl.date::date) foo
	WHERE rn = 1),
last_visit AS (
	SELECT
		lca.patient_id,
		c.initial_encounter_id,
		lca.appointment_start_time::date AS last_appointment_date,
		lca.appointment_service AS last_appointment_service,
		lca.appointment_location AS last_appointment_location,
		lf.last_form_date,
		lf.last_form_type,
		CASE WHEN lca.appointment_start_time >= lf.last_form_date THEN lca.appointment_start_time::date WHEN lca.appointment_start_time < lf.last_form_date THEN lf.last_form_date::date WHEN lca.appointment_start_time IS NOT NULL AND lf.last_form_date IS NULL THEN lca.appointment_start_time::date WHEN lca.appointment_start_time IS NULL AND lf.last_form_date IS NOT NULL THEN lf.last_form_date::date ELSE NULL END AS last_visit_date,
		CASE WHEN lca.appointment_start_time >= lf.last_form_date THEN lca.appointment_service WHEN lca.appointment_start_time < lf.last_form_date THEN lf.last_form_type WHEN lca.appointment_start_time IS NOT NULL AND lf.last_form_date IS NULL THEN lca.appointment_service WHEN lca.appointment_start_time IS NULL AND lf.last_form_date IS NOT NULL THEN lf.last_form_type ELSE NULL END AS last_visit_type,
		CASE WHEN lca.appointment_start_time >= lf.last_form_date THEN (DATE_PART('day',(now())-(lca.appointment_start_time::timestamp)))::int WHEN lca.appointment_start_time < lf.last_form_date THEN (DATE_PART('day',(now())-(lf.last_form_date::timestamp)))::int WHEN lca.appointment_start_time IS NOT NULL AND lf.last_form_date IS NULL THEN (DATE_PART('day',(now())-(lca.appointment_start_time::timestamp)))::int  WHEN lca.appointment_start_time IS NULL AND lf.last_form_date IS NOT NULL THEN (DATE_PART('day',(now())-(lf.last_form_date::timestamp)))::int ELSE NULL END AS days_since_last_visit,
		fma.appointment_start_time::date AS last_missed_appointment_date,
		fma.appointment_service AS last_missed_appointment_service,
		CASE WHEN fma.appointment_start_time IS NOT NULL THEN (DATE_PART('day',(now())-(fma.appointment_start_time::timestamp)))::int ELSE NULL END AS days_since_last_missed_appointment
	FROM cohort c
	LEFT OUTER JOIN last_completed_appointment lca 
		ON c.patient_id = lca.patient_id AND c.initial_visit_date <= lca.appointment_start_time AND c.end_date >= lca.appointment_start_time
	LEFT OUTER JOIN first_missed_appointment fma
		ON c.patient_id = fma.patient_id AND c.initial_visit_date <= fma.appointment_start_time AND c.end_date >= fma.appointment_start_time
	LEFT OUTER JOIN last_form lf
		ON c.initial_encounter_id = lf.initial_encounter_id),		
-- The last Hepatitis C visit CTE extracts the last visit data per cohort enrollment to look at if there are values reported for illicit drug use, pregnancy, hospitalisation, jaundice, hepatic encephalopathy, ascites, haematemesis, or cirrhosis repoted at the last visit. 
last_hepc_visit AS (
	SELECT initial_encounter_id, last_med_visit_date, last_med_visit_type, drug_use_last_visit, pregnant_last_visit, breastfeeding_last_visit, hospitalised_last_visit, jaundice_last_visit, hepatic_encephalopathy_last_visit, ascites_last_visit, haematememesis_last_visit, cirrhosis_last_visit
	FROM (
		SELECT  
			c.patient_id,
			c.initial_encounter_id,
			hc.date::date AS last_med_visit_date,
			hc.visit_type AS last_med_visit_type,
			hc.illicit_drug_use AS drug_use_last_visit,
			hc.currently_pregnant AS pregnant_last_visit,
			hc.currently_breastfeeding AS breastfeeding_last_visit, --T&T update
			hc.hospitalised_since_last_visit AS hospitalised_last_visit,
			hc.jaundice AS jaundice_last_visit,
			hc.hepatic_encephalopathy AS hepatic_encephalopathy_last_visit,
			hc.ascites AS ascites_last_visit,
			hc.haematemesis AS haematememesis_last_visit,
			hc.clinical_decompensated_cirrhosis AS cirrhosis_last_visit, 
			ROW_NUMBER() OVER (PARTITION BY hc.patient_id ORDER BY hc.date DESC) AS rn
		FROM cohort c
		LEFT OUTER JOIN hepatitis_c hc
			ON c.patient_id = hc.patient_id AND c.initial_visit_date <= hc.date::date AND c.end_date >= hc.date::date) foo
	WHERE rn = 1),	
-- The hospitalised CTE checks there is a hospitlisation reported in visits taking place in the last 6 months. 
hospitalisation_last_6m AS (
	SELECT 
		c.patient_id,
		c.initial_encounter_id, 
		COUNT(hc.hospitalised_since_last_visit) AS nb_hospitalised_last_6m, 
		CASE WHEN hc.hospitalised_since_last_visit IS NOT NULL THEN 'Yes' ELSE 'No' END AS hospitalised_last_6m
	FROM cohort c
	LEFT OUTER JOIN hepatitis_c hc
		ON c.patient_id = hc.patient_id AND c.initial_visit_date <= hc.date::date AND c.end_date >= hc.date::date
	WHERE hc.hospitalised_since_last_visit = 'Yes' and hc.date <= current_date and hc.date >= current_date - interval '6 months'
	GROUP BY c.patient_id, c.initial_encounter_id, hc.hospitalised_since_last_visit),	
-- The initial treatment CTE extracts treatment start data from the initial visit per cohort enrollment. If multiple initial visits have treatment initiation data, the most recent one is reported. 
treatment_order AS (
	SELECT 
		DISTINCT ON (hc.patient_id, c.initial_encounter_id, hc.treatment_start_date, hc.medication_duration, hepatitis_c_treatment_choice) hc.patient_id,
		c.initial_encounter_id,
		hc.date::date,
		hc.treatment_start_date,
		hc.medication_duration,
		hc.hepatitis_c_treatment_choice,
		CASE WHEN hc.treatment_end_date IS NOT NULL THEN hc.treatment_end_date WHEN hc.treatment_end_date IS NULL AND hc.medication_duration = '12 weeks' THEN (hc.treatment_start_date + INTERVAL '84 days')::date WHEN hc.treatment_end_date IS NULL AND hc.medication_duration = '24 weeks' THEN (hc.treatment_start_date + INTERVAL '168 days')::date END AS treatment_end_date,
		DENSE_RANK () OVER (PARTITION BY c.initial_encounter_id ORDER BY date) AS treatment_order
	FROM hepatitis_c hc
	LEFT OUTER JOIN cohort c
		ON c.patient_id = hc.patient_id AND c.initial_visit_date <= hc.date::date AND c.end_date >= hc.date::date
	WHERE hc.treatment_start_date IS NOT NULL AND hc.medication_duration IS NOT NULL AND hc.hepatitis_c_treatment_choice IS NOT NULL
	ORDER BY hc.patient_id, c.initial_encounter_id, hc.treatment_start_date, hc.medication_duration, hepatitis_c_treatment_choice, hc.date::date ASC),
treatment_secondary AS (
		SELECT
			DISTINCT ON (patient_id, initial_encounter_id) patient_id,
			initial_encounter_id,
			date::date,
			treatment_start_date,
			medication_duration,
			hepatitis_c_treatment_choice,
			treatment_end_date
		FROM treatment_order
		WHERE treatment_order > 1
		ORDER BY patient_id, initial_encounter_id, treatment_order DESC),
-- The first viral load CTE extracts the first viral load result from the lab form for each patient in the cohort where test results are present. 
first_vl AS (
	SELECT 
		DISTINCT ON (tto.patient_id, tto.initial_encounter_id) tto.patient_id,
		tto.initial_encounter_id,
		COALESCE(vli.date_of_sample_collection::date, vli.date::date) AS initial_vl_date, 
		vli.hcv_rna_pcr_qualitative_result AS initial_vl_result,
		vli.visit_type
	FROM treatment_order tto
	LEFT OUTER JOIN vitals_and_laboratory_information vli
		ON tto.patient_id = vli.patient_id AND tto.treatment_start_date >= COALESCE(vli.date_of_sample_collection::date, vli.date::date)
	WHERE COALESCE(vli.date_of_sample_collection::date, vli.date::date) IS NOT NULL AND vli.hcv_rna_pcr_qualitative_result IS NOT NULL AND tto.treatment_order = 1
	ORDER BY tto.patient_id, tto.initial_encounter_id, vli.patient_id, COALESCE(vli.date_of_sample_collection::date, vli.date::date) DESC),
-- The last HIV CTE extracts the most recent HIV result from the lab form for each patient in the cohort where test results are present. 
last_hiv AS (
	SELECT initial_encounter_id, last_hiv_date, last_hiv
	FROM (
		SELECT 
			c.patient_id,
			c.initial_encounter_id,
			COALESCE(vli.date_of_sample_collection::date, vli.date::date) AS last_hiv_date, 
			vli.hiv_test AS last_hiv, 
			ROW_NUMBER() OVER (PARTITION BY vli.patient_id ORDER BY COALESCE(vli.date_of_sample_collection::date, vli.date::date) DESC) AS rn 
		FROM cohort c
		LEFT OUTER JOIN vitals_and_laboratory_information vli
			ON c.patient_id = vli.patient_id AND c.initial_visit_date <= COALESCE(vli.date_of_sample_collection::date, vli.date::date) AND c.end_date >= COALESCE(vli.date_of_sample_collection::date, vli.date::date)
		WHERE COALESCE(vli.date_of_sample_collection::date, vli.date::date) IS NOT NULL AND vli.hiv_test IS NOT NULL) foo
	WHERE rn = 1),
-- The last visit location CTE finds the last visit location reported in Hepatitis C forms.
last_form_location AS (	
	SELECT 
		DISTINCT ON (c.patient_id, c.initial_encounter_id, c.initial_visit_date, c.end_date) c.initial_encounter_id,
		nvsl.date AS last_form_date,
		nvsl.visit_location AS last_form_location
	FROM cohort c
	LEFT OUTER JOIN (SELECT 
			patient_id, date, visit_location FROM hepatitis_c UNION SELECT patient_id, date, location_name AS visit_location 
		FROM vitals_and_laboratory_information) nvsl
		ON c.patient_id = nvsl.patient_id AND c.initial_visit_date <= nvsl.date::date AND c.end_date >= nvsl.date::date
	WHERE nvsl.visit_location IS NOT NULL
	GROUP BY c.patient_id, c.initial_encounter_id, c.initial_visit_date, c.end_date, nvsl.date, nvsl.visit_location
	ORDER BY c.patient_id, c.initial_encounter_id, c.initial_visit_date, c.end_date, nvsl.date DESC)
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
	pa."Sub_Block" AS sub_block, --T&T update
	pa."Legal_status",
	pa."Civil_status",
	pa."Education_level",
	pa."Occupation",
	pa."Personal_Situation",
	pa."Living_conditions",
	c.initial_visit_date AS enrollment_date,
	c.readmission,
	CASE WHEN fvl.visit_type = 'T&T Initial' THEN 'Yes' ELSE NULL END AS Test_and_Treat --T&T update
	fvl.initial_vl_date,
	fvl.initial_vl_result,
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
	lhv.last_med_visit_date,
	lhv.last_med_visit_type,
	lhv.drug_use_last_visit,
	lhv.pregnant_last_visit,
	lhv.breastfeeding_last_visit, --T&T update
	lhv.hospitalised_last_visit,
	lhv.jaundice_last_visit,
	lhv.hepatic_encephalopathy_last_visit,
	lhv.ascites_last_visit,
	lhv.haematememesis_last_visit,
	lhv.cirrhosis_last_visit,
	h6m.nb_hospitalised_last_6m,
	h6m.hospitalised_last_6m,
	hiv.last_hiv_date,
	hiv.last_hiv,
	ti.treatment_start_date AS treatment_start_date_initial,
	ti.medication_duration AS treatment_duration_initial,
	ti.hepatitis_c_treatment_choice AS treatment_initial,
	ti.treatment_end_date AS treatment_end_date_initial,
	ts.treatment_start_date AS treatment_start_date_fu,
	ts.medication_duration AS treatment_duration_fu,
	ts.hepatitis_c_treatment_choice AS treatment_fu,
	ts.treatment_end_date AS treatment_end_date_fu,
	COALESCE(ts.treatment_end_date, ti.treatment_end_date) AS treatment_end_date_last,
	CASE WHEN COALESCE(ts.treatment_end_date, ti.treatment_end_date) > CURRENT_DATE THEN 'Yes' ELSE NULL END AS currently_on_treatment,
	CASE WHEN COALESCE(ts.treatment_end_date, ti.treatment_end_date) < CURRENT_DATE THEN 'Yes' END AS completed_treatment,
	(COALESCE(ts.treatment_end_date, ti.treatment_end_date) + INTERVAL '84 days')::date AS post_treatment_pcr_due_date,
	tf.first_treatment_failure,
	tf.hcv_pcr_12_weeks_after_treatment_end AS treatment_failure_PCR_12w,
	CASE WHEN lv.last_visit_type = 'Discharge visit' THEN ld.patient_outcome ELSE NULL END AS patient_outcome,
	CASE WHEN lv.last_visit_type = 'Discharge visit' THEN lv.last_visit_date ELSE NULL END AS discharge_date,
	ld.last_pcr_12w, 
	CASE WHEN lv.last_visit_type != 'Discharge visit' THEN 'Yes' ELSE NULL END AS in_cohort
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
LEFT OUTER JOIN last_hepc_visit lhv	
	ON c.initial_encounter_id = lhv.initial_encounter_id
LEFT OUTER JOIN hospitalisation_last_6m h6m
	ON c.initial_encounter_id = h6m.initial_encounter_id
LEFT OUTER JOIN first_vl fvl 
	ON c.initial_encounter_id = fvl.initial_encounter_id
LEFT OUTER JOIN last_hiv hiv 
	ON c.initial_encounter_id = hiv.initial_encounter_id
LEFT OUTER JOIN treatment_order ti
	ON c.initial_encounter_id = ti.initial_encounter_id AND treatment_order = 1
LEFT OUTER JOIN treatment_secondary ts
	ON c.initial_encounter_id = ts.initial_encounter_id
LEFT OUTER JOIN last_form_location lfl
	ON c.initial_encounter_id = lfl.initial_encounter_id
LEFT OUTER JOIN treatment_failure tf
	ON c.initial_encounter_id = tf.initial_encounter_id
LEFT OUTER JOIN last_discharge ld 
	ON c.initial_encounter_id = ld.initial_encounter_id;