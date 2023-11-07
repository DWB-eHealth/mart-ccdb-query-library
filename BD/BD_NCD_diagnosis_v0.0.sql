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
-- The NCD diagnosis CTE select the last reported NCD diagnosis per cohort enrollment. 
last_ncd_diagnosis AS (
    SELECT 
		c.initial_encounter_id, nd.patient_id, nd.date, nd.diagnosis
	FROM (SELECT d.ncdiagnosis AS diagnosis, n.date::date, d.patient_id, n.rn FROM ncdiagnosis d 
	LEFT JOIN (SELECT encounter_id, date, ROW_NUMBER() OVER (PARTITION BY patient_id ORDER BY date DESC) AS rn FROM ncd) n 
		USING(encounter_id)) nd
	JOIN cohort c
		ON nd.patient_id = c.patient_id AND c.initial_visit_date <= nd.date AND CASE WHEN c.discharge_date IS NOT NULL THEN c.discharge_date ELSE current_date END >= nd.date
	WHERE nd.rn = 1),
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
	ORDER BY c.patient_id, c.initial_encounter_id, c.initial_visit_date, c.discharge_date, ncd.date DESC)
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
	lnd.diagnosis
FROM last_ncd_diagnosis lnd
LEFT OUTER JOIN cohort c
	ON lnd.initial_encounter_id = c.initial_encounter_id
LEFT OUTER JOIN patient_identifier pi
	ON c.patient_id = pi.patient_id
LEFT OUTER JOIN person_attributes pa
	ON c.patient_id = pa.person_id
LEFT OUTER JOIN person_details_default pdd 
	ON c.patient_id = pdd.person_id
LEFT OUTER JOIN patient_encounter_details_default ped 
	ON c.initial_encounter_id = ped.encounter_id
LEFT OUTER JOIN last_visit_location lvl
	ON c.initial_encounter_id = lvl.initial_encounter_id;
