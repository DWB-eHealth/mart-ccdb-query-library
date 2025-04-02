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
-- The NCD diagnosis CTEs extract all NCD diagnoses for patients reported between their initial visit and discharge visit. Diagnoses are only reported once. For specific disease groups, the second CTE extracts only the last reported diagnosis among the groups. These groups include types of diabetes, types of epilespy, and hyper-/hypothyroidism.
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
-- The last visit location CTEs find the last visit location reported in NCD forms and appointment scheduling module.
last_form_location AS (	
	SELECT 
		DISTINCT ON (c.patient_id, c.initial_encounter_id, c.initial_visit_date, c.discharge_date) c.initial_encounter_id,
		nvsl.date AS last_form_date,
		nvsl.visit_location AS last_form_location
	FROM cohort c
	LEFT OUTER JOIN (SELECT patient_id, date, visit_location FROM NCD UNION SELECT patient_id, date, location_name AS visit_location FROM vitals_and_laboratory_information) nvsl
		ON c.patient_id = nvsl.patient_id AND c.initial_visit_date <= nvsl.date::date AND COALESCE(c.discharge_date::date, CURRENT_DATE) >= nvsl.date::date
	WHERE nvsl.visit_location IS NOT NULL
	GROUP BY c.patient_id, c.initial_encounter_id, c.initial_visit_date, c.discharge_date, nvsl.date, nvsl.visit_location
	ORDER BY c.patient_id, c.initial_encounter_id, c.initial_visit_date, c.discharge_date, nvsl.date DESC),
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
last_visit_location AS (	
	SELECT 
		c.initial_encounter_id,
		CASE WHEN lfl.last_form_date > lca.appointment_start_time THEN lfl.last_form_location WHEN lfl.last_form_date <= lca.appointment_start_time THEN lca.appointment_location WHEN lfl.last_form_date IS NOT NULL AND lca.appointment_start_time IS NULL THEN lfl.last_form_location WHEN lfl.last_form_date IS NULL AND lca.appointment_start_time IS NOT NULL THEN lca.appointment_location ELSE NULL END AS last_visit_location
	FROM cohort c
	LEFT OUTER JOIN last_form_location lfl
		ON c.initial_encounter_id = lfl.initial_encounter_id 
	LEFT OUTER JOIN last_completed_appointment lca
		ON c.patient_id = lca.patient_id AND c.initial_visit_date <= lca.appointment_start_time::date AND COALESCE(c.discharge_date::date, CURRENT_DATE) >= lca.appointment_start_time::date)
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
	c.initial_visit_date AS enrollment_date,
	CASE WHEN c.discharge_date IS NULL THEN 'Yes' END AS in_cohort,
	c.readmission,
	c.initial_visit_location,
	lvl.last_visit_location,
	c.discharge_date,
	c.patient_outcome,
	cdl.diagnosis
FROM cohort_diagnosis_last cdl
LEFT OUTER JOIN cohort c
	ON cdl.initial_encounter_id = c.initial_encounter_id
LEFT OUTER JOIN patient_identifier pi
	ON c.patient_id = pi.patient_id
LEFT OUTER JOIN person_attributes pa
	ON c.patient_id = pa.person_id
LEFT OUTER JOIN person_details_default pdd 
	ON c.patient_id = pdd.person_id
LEFT OUTER JOIN patient_encounter_details_default ped 
	ON c.initial_encounter_id = ped.encounter_id
LEFT OUTER JOIN last_visit_location lvl
	ON c.initial_encounter_id = lvl.initial_encounter_id
WHERE c.patient_id IS NOT NULL;