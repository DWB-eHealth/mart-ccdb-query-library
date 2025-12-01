-- The initial and cohort CTEs build the frame for patients entering and exiting the cohort. This frame is based on the HepC form with visit type of 'initial visit'. The query takes all initial visit dates and matches the next initial visit date (if present) or the current date. The cohort frame is built using the initial visit date and the current date or the following date as the bookends, this allows for cases who are discharged as treatment failure and subsequently started on second-line treatment to be analyzed as a single enrollment (row) in the register. 
WITH initial AS (
	SELECT 
		patient_id, encounter_id AS initial_encounter_id, visit_location AS initial_visit_location, date AS initial_visit_date, DENSE_RANK () OVER (PARTITION BY patient_id ORDER BY date) AS initial_visit_order, LEAD (date) OVER (PARTITION BY patient_id ORDER BY date) AS next_initial_visit_date, currently_pregnant AS pregnant_initial, currently_breastfeeding AS breastfeeding_initial
	FROM hepatitis_c WHERE visit_type = 'Initial visit'),
cohort AS (
	SELECT
		i1.patient_id, i1.initial_encounter_id, i1.initial_visit_location, i1.initial_visit_date, CASE WHEN i1.initial_visit_order > 1 THEN 'Yes' END readmission, COALESCE(i2.initial_visit_date, CURRENT_DATE) AS end_date, i2.initial_visit_date AS next_initial_visit_date, i1.pregnant_initial, i1.breastfeeding_initial
	FROM initial i1
	LEFT OUTER JOIN initial i2
		ON i1.patient_id = i2.patient_id AND  i1.initial_visit_order = (i2.initial_visit_order - 1)),
-- The treatment failure CTE extracts the first treatment failure date, if present, and PCR result 12 weeks post treatment completion for each patient. 
treatment_failure AS (
	SELECT 
		c.patient_id,
		c.initial_encounter_id,
		tf.date::date AS first_treatment_failure,
		tf.hcv_pcr_12_weeks_after_treatment_end,
		ROW_NUMBER() OVER (PARTITION BY c.initial_encounter_id ORDER BY tf.date) AS rn
	FROM cohort c
	LEFT OUTER JOIN hepatitis_c tf
		ON c.patient_id = tf.patient_id AND c.initial_visit_date <= tf.date::date AND (c.next_initial_visit_date IS NULL OR c.next_initial_visit_date >= tf.date::date)
	WHERE tf.patient_outcome = 'Treatment failure'),
treatment_failure_max AS ( 
	SELECT 
		patient_id,
		max(rn) AS nb_treatment_failures
	FROM (
		SELECT 
			patient_id,
			ROW_NUMBER() OVER (PARTITION BY patient_id ORDER BY date) AS rn
		FROM hepatitis_c 
		WHERE patient_outcome = 'Treatment failure') foo
	GROUP BY patient_id),
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
	SELECT patient_id, appointment_start_time::date AS missed_appointment_date, appointment_service AS missed_appointment_service, CASE WHEN appointment_start_time IS NOT NULL THEN (DATE_PART('day',(now())-(appointment_start_time::timestamp)))::int ELSE NULL END AS days_since_missed_appointment
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
-- The last Hepatitis C visit CTE extracts the last visit data per cohort enrollment to look at if there are values reported for illicit drug use, pregnancy, hospitalisation, jaundice, hepatic encephalopathy, ascites, haematemesis, or cirrhosis repoted at the last visit. 
last_hepc_form AS (
	SELECT initial_encounter_id, last_med_visit_date, last_med_visit_type, drug_use_last_visit, pregnant_last_visit, breastfeeding_last_visit, hospitalised_last_visit, jaundice_last_visit, hepatic_encephalopathy_last_visit, ascites_last_visit, haematememesis_last_visit, cirrhosis_last_visit, last_pcr_12w, (DATE_PART('day',(now())-(last_med_visit_date::timestamp)))::int AS days_since_last_med_visit, discharge_date, patient_outcome
	FROM (
		SELECT  
			c.patient_id,
			c.initial_encounter_id,
			hc.date::date AS last_med_visit_date,
			hc.visit_type AS last_med_visit_type,
			hc.illicit_drug_use AS drug_use_last_visit,
			hc.currently_pregnant AS pregnant_last_visit,
			hc.currently_breastfeeding AS breastfeeding_last_visit,
			hc.hospitalised_since_last_visit AS hospitalised_last_visit,
			hc.jaundice AS jaundice_last_visit,
			hc.hepatic_encephalopathy AS hepatic_encephalopathy_last_visit,
			hc.ascites AS ascites_last_visit,
			hc.haematemesis AS haematememesis_last_visit,
			hc.clinical_decompensated_cirrhosis AS cirrhosis_last_visit, 
			hc.hcv_pcr_12_weeks_after_treatment_end AS last_pcr_12w,
			CASE WHEN hc.visit_type = 'Discharge visit' THEN COALESCE(hc.discharge_date, date)::date END AS discharge_date,
			hc.patient_outcome,
			ROW_NUMBER() OVER (PARTITION BY c.initial_encounter_id ORDER BY hc.date DESC, CASE WHEN hc.visit_type = 'Discharge visit' THEN 1 WHEN hc.visit_type = 'Follow up visit' THEN 2 ELSE 3 END) AS rn
		FROM cohort c
		LEFT OUTER JOIN hepatitis_c hc
			ON c.patient_id = hc.patient_id AND c.initial_visit_date <= hc.date::date AND (c.next_initial_visit_date IS NULL OR c.next_initial_visit_date >= hc.date::date)) foo
	WHERE rn = 1),	
-- The last form CTE extracts the last form data per cohort enrollment to look at the last type of form and form location used.
last_form AS (
	SELECT patient_id, initial_encounter_id, last_form_date, last_form_type, last_login_location, last_form_location, COALESCE(last_form_location, last_login_location) AS last_location, last_form
	FROM (
		SELECT 
			c.patient_id,
			c.initial_encounter_id,
			hcvl.date AS last_form_date,
			hcvl.visit_type AS last_form_type,
			hcvl.login_location AS last_login_location,
			hcvl.form_location AS last_form_location,
			hcvl.form_field_path AS last_form,
			ROW_NUMBER() OVER (PARTITION BY hcvl.patient_id ORDER BY hcvl.date DESC, CASE WHEN hcvl.form_field_path = 'Hepatitis C' THEN 1 WHEN hcvl.form_field_path = 'Vitals and Laboratory Information' THEN 2 ELSE 3 END, CASE WHEN hcvl.visit_type = 'Discharge visit' THEN 1 WHEN hcvl.visit_type = 'Follow up visit' THEN 2 ELSE 3 END) AS rn
		FROM cohort c
		LEFT OUTER JOIN last_hepc_form lhf
			ON c.initial_encounter_id = lhf.initial_encounter_id
		LEFT OUTER JOIN (
			SELECT patient_id, date, location_name AS login_location, visit_location AS form_location, visit_type, form_field_path FROM hepatitis_c
			UNION
			SELECT patient_id, COALESCE(date, date_of_sample_collection) AS date, location_name AS login_location, NULL AS form_location, visit_type, form_field_path FROM vitals_and_laboratory_information) hcvl
			ON c.patient_id = hcvl.patient_id AND c.initial_visit_date <= hcvl.date::date AND (lhf.discharge_date IS NULL OR lhf.discharge_date >= hcvl.date::date)) foo
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
		ON c.patient_id = hc.patient_id AND c.initial_visit_date <= hc.date::date AND (c.next_initial_visit_date IS NULL OR c.next_initial_visit_date >= hc.date::date)
	WHERE hc.hospitalised_since_last_visit = 'Yes' and hc.date <= current_date and hc.date >= current_date - interval '6 months'
	GROUP BY c.patient_id, c.initial_encounter_id, hc.hospitalised_since_last_visit),	
-- The initial treatment CTE extracts treatment start data from the initial visit per cohort enrollment. If multiple initial visits have treatment initiation data, the most recent one is reported. 
treatment_order AS (
	SELECT 
		DISTINCT ON (hc.patient_id, c.initial_encounter_id, hc.treatment_start_date, hc.medication_duration, hc.hepatitis_c_treatment_choice) hc.patient_id,
		c.initial_encounter_id,
		hc.date::date,
		hc.treatment_start_date,
		hc.medication_duration,
		hc.hepatitis_c_treatment_choice,
		CASE WHEN hc.treatment_end_date IS NOT NULL THEN hc.treatment_end_date WHEN hc.treatment_end_date IS NULL AND hc.medication_duration = '12 weeks' THEN (hc.treatment_start_date + INTERVAL '84 days')::date WHEN hc.treatment_end_date IS NULL AND hc.medication_duration = '24 weeks' THEN (hc.treatment_start_date + INTERVAL '168 days')::date END AS treatment_end_date,
		DENSE_RANK () OVER (PARTITION BY c.initial_encounter_id ORDER BY date) AS treatment_order
	FROM hepatitis_c hc
	LEFT OUTER JOIN cohort c
		ON c.patient_id = hc.patient_id AND c.initial_visit_date <= hc.date::date AND (c.next_initial_visit_date IS NULL OR c.next_initial_visit_date >= hc.date::date)
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
		vli.visit_type,
		vli.initial_test_and_treat_pcr
	FROM treatment_order tto
	LEFT OUTER JOIN vitals_and_laboratory_information vli
		ON tto.patient_id = vli.patient_id AND tto.treatment_start_date >= COALESCE(vli.date_of_sample_collection::date, vli.date::date)
	WHERE COALESCE(vli.date_of_sample_collection::date, vli.date::date) IS NOT NULL AND vli.hcv_rna_pcr_qualitative_result IS NOT NULL AND tto.treatment_order = 1
	ORDER BY tto.patient_id, tto.initial_encounter_id, vli.patient_id, COALESCE(vli.date_of_sample_collection::date, vli.date::date) DESC),
-- The last HIV CTE extracts the most recent HIV result from the lab form for each patient in the cohort where test results are present. 
last_hiv AS (
	SELECT patient_id, last_hiv_date, last_hiv
	FROM (
		SELECT 
			patient_id,
			COALESCE(date_of_sample_collection::date, date::date) AS last_hiv_date, 
			hiv_test AS last_hiv, 
			ROW_NUMBER() OVER (PARTITION BY patient_id ORDER BY COALESCE(date_of_sample_collection::date, date::date) DESC) AS rn 
		FROM vitals_and_laboratory_information vli
		WHERE COALESCE(date_of_sample_collection::date, date::date) IS NOT NULL AND hiv_test IS NOT NULL) foo
	WHERE rn = 1),
-- The last Hep B CTE extracts the most recent Hep B result from the lab form for each patient in the cohort where test results are present. 
last_hepb AS (
	SELECT patient_id, last_hepb_date, last_hepb
	FROM (
		SELECT 
			patient_id,
			COALESCE(date_of_sample_collection::date, date::date) AS last_hepb_date, 
			hbsag AS last_hepb, 
			ROW_NUMBER() OVER (PARTITION BY patient_id ORDER BY COALESCE(date_of_sample_collection::date, date::date) DESC) AS rn 
		FROM vitals_and_laboratory_information vli
		WHERE COALESCE(date_of_sample_collection::date, date::date) IS NOT NULL AND hbsag IS NOT NULL) foo
	WHERE rn = 1),
-- The first HIV CTE extracts the first HIV result from the lab form for each patient in the cohort where test results are present. 
first_hiv AS (
	SELECT patient_id, first_hiv_date, first_hiv
	FROM (
		SELECT 
			patient_id,
			COALESCE(date_of_sample_collection::date, date::date) AS first_hiv_date, 
			hiv_test AS first_hiv, 
			ROW_NUMBER() OVER (PARTITION BY patient_id ORDER BY COALESCE(date_of_sample_collection::date, date::date)) AS rn 
		FROM vitals_and_laboratory_information vli
		WHERE COALESCE(date_of_sample_collection::date, date::date) IS NOT NULL AND hiv_test IS NOT NULL) foo
	WHERE rn = 1),
-- The first Hep B CTE extracts the first Hep B result from the lab form for each patient in the cohort where test results are present. 
first_hepb AS (
	SELECT patient_id, first_hepb_date, first_hepb
	FROM (
		SELECT 
			patient_id,
			COALESCE(date_of_sample_collection::date, date::date) AS first_hepb_date, 
			hbsag AS first_hepb, 
			ROW_NUMBER() OVER (PARTITION BY patient_id ORDER BY COALESCE(date_of_sample_collection::date, date::date)) AS rn 
		FROM vitals_and_laboratory_information vli
		WHERE COALESCE(date_of_sample_collection::date, date::date) IS NOT NULL AND hbsag IS NOT NULL) foo
	WHERE rn = 1),
-- The medication_edit CTE reformates the medication data to include both coded and non-coded drug names, start and end dates, and whether the medication is ongoing.
medication_edit AS (
	SELECT
		c.patient_id,
		c.initial_encounter_id,
		mdd.encounter_id,
		mdd.order_id,
		COALESCE(mdd.coded_drug_name, mdd.non_coded_drug_name) AS medication_name,
		mdd.start_date::date AS date_started,
		COALESCE(mdd.date_stopped, mdd.calculated_end_date)::date AS date_ended,
		CASE
			WHEN COALESCE(mdd.date_stopped, mdd.calculated_end_date) IS NULL AND mdd.start_date::date <= CURRENT_DATE THEN 1
			WHEN COALESCE(mdd.date_stopped, mdd.calculated_end_date)::date > CURRENT_DATE AND mdd.start_date::date <= CURRENT_DATE THEN 1
			ELSE NULL
		END AS ongoing
	FROM
		medication_data_default mdd
		LEFT JOIN cohort c ON mdd.patient_id = c.patient_id
		AND c.initial_visit_date <= mdd.start_date
		AND COALESCE(c.end_date, CURRENT_DATE) >= mdd.start_date
),
-- The medication_list CTE aggregates all medications reported for each patient, regardless of if the medication is still active or not.
medication_list AS (
	SELECT
		initial_encounter_id,
		STRING_AGG(
			DISTINCT medication_name,
			', '
			ORDER BY
				medication_name
		) AS medication_list
	FROM
		medication_edit
	GROUP BY
		initial_encounter_id
),
-- The medication_list_ongoing CTE aggregates only active medications reported for each patient.
medication_list_ongoing AS (
	SELECT
		initial_encounter_id,
		STRING_AGG(
			DISTINCT medication_name,
			', '
			ORDER BY
				medication_name
		) AS medication_list_ongoing
	FROM
		medication_edit
	WHERE
		ongoing = 1
	GROUP BY
		initial_encounter_id
),
-- The following CTEs correct for the inccorect age calculation in Bahmni-Mart.
base AS (
	SELECT
		p.person_id,
		p.birthyear,
		p.age::int,
		CURRENT_DATE AS t,
		EXTRACT(YEAR FROM CURRENT_DATE)::int - p.birthyear AS delta
	FROM person_details_default p
	WHERE
		p.age IS NOT NULL
),
fixed AS (
	SELECT
		person_id,
		birthyear,
		t,
		delta,
		age,
		CASE
			WHEN age = delta THEN age
			WHEN age = delta - 1 THEN age
			WHEN age = delta + 1 THEN delta
			ELSE NULL
		END AS age_fixed,
		CASE
			WHEN age IN (delta, delta - 1) THEN 'as_is'
			WHEN age = delta + 1 THEN 'corrected_from_delta_plus_1'
			ELSE 'unusable'
		END AS age_fix_status
	FROM base
),
anchor AS (
	SELECT
		*,
		make_date(birthyear, 
			EXTRACT(MONTH FROM t)::int, LEAST(
				EXTRACT(DAY FROM t)::int, 
					EXTRACT(DAY FROM (date_trunc('month', make_date(birthyear,
						EXTRACT(MONTH FROM t)::int, 1)) + INTERVAL '1 month' - INTERVAL '1 day'))::int)) AS t_md_in_yob
	FROM fixed f
),
age_bounds AS (
	SELECT
		person_id,
		age_fixed,
		age_fix_status,
		CASE
			WHEN age_fixed = delta THEN make_date(birthyear, 1, 1)
			WHEN age_fixed = delta - 1 THEN (t_md_in_yob + INTERVAL '1 day')::date
			WHEN age_fixed IS NULL THEN make_date(birthyear, 1, 1)
		END AS dob_min,
		CASE
			WHEN age_fixed = delta THEN t_md_in_yob
			WHEN age_fixed = delta - 1 THEN make_date(birthyear, 12, 31)
			WHEN age_fixed IS NULL THEN make_date(birthyear, 12, 31)
		END AS dob_max
	FROM
		anchor
)
-- Main query --
SELECT
	pi."Patient_Identifier",
	c.patient_id,
	c.initial_encounter_id,
	pa."Other_patient_identifier",
	pa."Previous_MSF_code",
	ab.age_fixed::int AS age_current,
	CASE
		WHEN ab.age_fixed::int <= 4 THEN '0-4'
		WHEN ab.age_fixed::int >= 5 AND ab.age_fixed::int <= 14 THEN '05-14'
		WHEN ab.age_fixed::int >= 15 AND ab.age_fixed::int <= 24 THEN '15-24'
		WHEN ab.age_fixed::int >= 25 AND ab.age_fixed::int <= 34 THEN '25-34'
		WHEN ab.age_fixed::int >= 35 AND ab.age_fixed::int <= 44 THEN '35-44'
		WHEN ab.age_fixed::int >= 45 AND ab.age_fixed::int <= 54 THEN '45-54'
		WHEN ab.age_fixed::int >= 55 AND ab.age_fixed::int <= 64 THEN '55-64'
		WHEN ab.age_fixed::int >= 65 THEN '65+'
		ELSE NULL
	END AS age_group_current,
	(((c.initial_visit_date - ab.dob_min) + (c.initial_visit_date - ab.dob_max))::numeric / (2 * 365.2425))::int AS age_admission,
	CASE
		WHEN (((c.initial_visit_date - ab.dob_min) + (c.initial_visit_date - ab.dob_max))::numeric / (2 * 365.2425))::int <= 4 THEN '0-4'
		WHEN (((c.initial_visit_date - ab.dob_min) + (c.initial_visit_date - ab.dob_max))::numeric / (2 * 365.2425))::int >= 5
		AND (((c.initial_visit_date - ab.dob_min) + (c.initial_visit_date - ab.dob_max))::numeric / (2 * 365.2425))::int <= 14 THEN '05-14'
		WHEN (((c.initial_visit_date - ab.dob_min) + (c.initial_visit_date - ab.dob_max))::numeric / (2 * 365.2425))::int >= 15
		AND (((c.initial_visit_date - ab.dob_min) + (c.initial_visit_date - ab.dob_max))::numeric / (2 * 365.2425))::int <= 24 THEN '15-24'
		WHEN (((c.initial_visit_date - ab.dob_min) + (c.initial_visit_date - ab.dob_max))::numeric / (2 * 365.2425))::int >= 25
		AND (((c.initial_visit_date - ab.dob_min) + (c.initial_visit_date - ab.dob_max))::numeric / (2 * 365.2425))::int <= 34 THEN '25-34'
		WHEN (((c.initial_visit_date - ab.dob_min) + (c.initial_visit_date - ab.dob_max))::numeric / (2 * 365.2425))::int >= 35
		AND (((c.initial_visit_date - ab.dob_min) + (c.initial_visit_date - ab.dob_max))::numeric / (2 * 365.2425))::int <= 44 THEN '35-44'
		WHEN (((c.initial_visit_date - ab.dob_min) + (c.initial_visit_date - ab.dob_max))::numeric / (2 * 365.2425))::int >= 45
		AND (((c.initial_visit_date - ab.dob_min) + (c.initial_visit_date - ab.dob_max))::numeric / (2 * 365.2425))::int <= 54 THEN '45-54'
		WHEN (((c.initial_visit_date - ab.dob_min) + (c.initial_visit_date - ab.dob_max))::numeric / (2 * 365.2425))::int >= 55
		AND (((c.initial_visit_date - ab.dob_min) + (c.initial_visit_date - ab.dob_max))::numeric / (2 * 365.2425))::int <= 64 THEN '55-64'
		WHEN (((c.initial_visit_date - ab.dob_min) + (c.initial_visit_date - ab.dob_max))::numeric / (2 * 365.2425))::int >= 65 THEN '65+'
		ELSE NULL
	END AS age_group_admission,
	pdd.gender,
	pa."patientCity" AS camp_location, 
	pa."patientDistrict" AS block,
	pa."Subblock" AS subblock,
	pa."Legal_status",
	pa."Civil_status",
	pa."Education_level",
	pa."Occupation",
	pa."Personal_Situation",
	pa."Living_conditions",
	c.initial_visit_date AS enrollment_date,
	c.readmission,
	CASE WHEN fvl.initial_test_and_treat_pcr = 'Yes' THEN fvl.initial_test_and_treat_pcr ELSE NULL END AS test_and_treat,
	fvl.initial_vl_date,
	fvl.initial_vl_result,
	c.initial_visit_location,
	c.pregnant_initial, 
	c.breastfeeding_initial,
	lf.last_form_location,
	lf.last_login_location,
	lf.last_location,
	lf.last_form_date,
	lf.last_form_type,	
	lf.last_form, 
	fma.missed_appointment_date,
	fma.missed_appointment_service,
	fma.days_since_missed_appointment,
	lhf.last_med_visit_date,
	lhf.last_med_visit_type,
	lhf.days_since_last_med_visit,
	lhf.drug_use_last_visit,
	lhf.pregnant_last_visit,
	lhf.breastfeeding_last_visit, --T&T update
	lhf.hospitalised_last_visit,
	lhf.jaundice_last_visit,
	lhf.hepatic_encephalopathy_last_visit,
	lhf.ascites_last_visit,
	lhf.haematememesis_last_visit,
	lhf.cirrhosis_last_visit,
	h6m.nb_hospitalised_last_6m,
	h6m.hospitalised_last_6m,
	fh.first_hiv_date,
	fh.first_hiv,
	fhb.first_hepb_date,
	fhb.first_hepb,
	hiv.last_hiv_date,
	hiv.last_hiv,
	hepb.last_hepb_date,
	hepb.last_hepb,
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
	tfm.nb_treatment_failures, 
	CASE WHEN lhf.last_med_visit_type = 'Discharge visit' THEN lhf.patient_outcome ELSE NULL END AS patient_outcome,
	CASE WHEN lhf.last_med_visit_type = 'Discharge visit' THEN lhf.discharge_date ELSE NULL END AS discharge_date,
	lhf.last_pcr_12w, 
	CASE WHEN lhf.last_med_visit_type != 'Discharge visit' THEN 'Yes' ELSE NULL END AS in_cohort,
	ml.medication_list,
	mlo.medication_list_ongoing
FROM cohort c
LEFT OUTER JOIN patient_identifier pi
	ON c.patient_id = pi.patient_id
LEFT OUTER JOIN person_attributes pa
	ON c.patient_id = pa.person_id
LEFT OUTER JOIN person_details_default pdd 
	ON c.patient_id = pdd.person_id
LEFT OUTER JOIN age_bounds ab 
	ON c.patient_id = ab.person_id
LEFT OUTER JOIN patient_encounter_details_default ped 
	ON c.initial_encounter_id = ped.encounter_id
LEFT OUTER JOIN first_missed_appointment fma
	ON c.patient_id = fma.patient_id AND c.initial_visit_date <= fma.missed_appointment_date AND (c.next_initial_visit_date IS NULL OR c.next_initial_visit_date >= fma.missed_appointment_date)
LEFT OUTER JOIN last_hepc_form lhf	
	ON c.initial_encounter_id = lhf.initial_encounter_id
LEFT OUTER JOIN hospitalisation_last_6m h6m
	ON c.initial_encounter_id = h6m.initial_encounter_id
LEFT OUTER JOIN first_vl fvl 
	ON c.initial_encounter_id = fvl.initial_encounter_id
LEFT OUTER JOIN last_hiv hiv 
	ON c.patient_id = hiv.patient_id AND (c.next_initial_visit_date IS NULL OR c.next_initial_visit_date <= hiv.last_hiv_date)
LEFT OUTER JOIN last_hepb hepb
	ON c.patient_id = hepb.patient_id AND (c.next_initial_visit_date IS NULL OR c.next_initial_visit_date <= hepb.last_hepb_date)
LEFT OUTER JOIN treatment_order ti
	ON c.initial_encounter_id = ti.initial_encounter_id AND treatment_order = 1
LEFT OUTER JOIN treatment_secondary ts
	ON c.initial_encounter_id = ts.initial_encounter_id
LEFT OUTER JOIN last_form lf
	ON c.initial_encounter_id = lf.initial_encounter_id
LEFT OUTER JOIN treatment_failure tf
	ON c.initial_encounter_id = tf.initial_encounter_id AND tf.rn = 1
LEFT OUTER JOIN treatment_failure_max tfm 
	ON c.patient_id = tfm.patient_id
LEFT OUTER JOIN first_hiv fh 
	ON c.patient_id = fh.patient_id AND (c.next_initial_visit_date IS NULL OR c.next_initial_visit_date <= fh.first_hiv_date)
LEFT OUTER JOIN first_hepb fhb 
	ON c.patient_id = fhb.patient_id AND (c.next_initial_visit_date IS NULL OR c.next_initial_visit_date <= fhb.first_hepb_date)
LEFT OUTER JOIN medication_list ml 
	ON c.initial_encounter_id = ml.initial_encounter_id
LEFT OUTER JOIN medication_list_ongoing mlo 
	ON c.initial_encounter_id = mlo.initial_encounter_id;