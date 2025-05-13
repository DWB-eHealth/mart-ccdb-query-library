-- The first CTEs build the frame for patients entering and exiting the cohort. This frame is based on NCD forms with visit types of 'initial visit' and 'patient outcome'. The query takes all initial visit dates and matches patient outcome visit dates if the patient outcome visit date falls between the initial visit date and the next initial visit date (if present).
WITH initial AS (
	SELECT 
		patient_id, encounter_id AS initial_encounter_id, visit_location AS initial_visit_location, date_of_visit AS initial_visit_date, DENSE_RANK () OVER (PARTITION BY patient_id ORDER BY date_of_visit) AS initial_visit_order, LEAD (date_of_visit) OVER (PARTITION BY patient_id ORDER BY date_of_visit) AS next_initial_visit_date
	FROM ncd_consultations WHERE visit_type = 'Initial visit'),
cohort AS (
	SELECT
		i.patient_id, i.initial_encounter_id, i.initial_visit_location, i.initial_visit_date, CASE WHEN i.initial_visit_order > 1 THEN 'Yes' END readmission, d.encounter_id AS outcome_encounter_id, d.outcome_date2 AS outcome_date, d.patient_outcome_at_end_of_msf_care AS patient_outcome 
	FROM initial i
	LEFT JOIN (SELECT patient_id, encounter_id, date_of_visit::date AS outcome_date2, patient_outcome_at_end_of_msf_care FROM ncd_consultations WHERE visit_type = 'Patient outcome') d 
		ON i.patient_id = d.patient_id AND (d.outcome_date2 IS NULL OR (d.outcome_date2 >= i.initial_visit_date AND (d.outcome_date2 < i.next_initial_visit_date OR i.next_initial_visit_date IS NULL)))),
-- The NCD diagnosis CTEs extract all NCD diagnoses for patients reported between their initial visit and discharge visit. Diagnoses are only reported once. For specific disease groups, the second CTE extracts only the last reported diagnosis among the groups. These groups include types of diabetes, types of epilespy, and hypertension stages. The next CTE pivotes the diagnoses horizontally. The final CTE extracts a list of all diagnoses reported for each patient.
cohort_diagnosis AS (
	SELECT
		c.patient_id, c.initial_encounter_id, COALESCE(d.date_of_diagnosis, n.date_of_visit) AS date_of_diagnosis, d.ncd_cohort_diagnosis AS diagnosis
	FROM ncd_consultations_ncd_diagnosis d 
	LEFT JOIN ncd_consultations n USING(encounter_id)
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
                    WHEN diagnosis IN ('Asthma', 'Chronic obstructive pulmonary disease', 'Sickle cell disease') THEN 'Group1'
                    WHEN diagnosis IN ('Diabetes mellitus, type 1', 'Diabetes mellitus, type 2') THEN 'Group2'
                    WHEN diagnosis IN ('Focal epilepsy', 'Generalised epilepsy', 'Unclassified epilepsy') THEN 'Group3'
					WHEN diagnosis IN ('Hypertension Stage 1', 'Hypertension Stage 2', 'Hypertension Stage 3') THEN 'Group4'
                    ELSE 'Other'
                END AS diagnosis_group
            FROM cohort_diagnosis cd) cdg) foo
    WHERE rn = 1)
-- Main query --
SELECT 
	pi."Patient_Identifier",
	c.patient_id,
	c.initial_encounter_id,
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
	pad.city_village AS county,
	pad.state_province AS sub_county,
	pad.county_district AS ward,
	pad.address2 AS village,
	c.initial_visit_date AS enrollment_date,
	CASE WHEN c.outcome_date IS NULL THEN 'Yes' END AS in_cohort,
	c.outcome_date,
	c.patient_outcome,
	cdl.diagnosis
FROM cohort_diagnosis_last cdl
LEFT OUTER JOIN cohort c
	ON cdl.initial_encounter_id = c.initial_encounter_id
LEFT OUTER JOIN patient_identifier pi
	ON c.patient_id = pi.patient_id
LEFT OUTER JOIN person_details_default pdd 
	ON c.patient_id = pdd.person_id
LEFT OUTER JOIN person_address_default pad 
	ON c.patient_id = pad.person_id
LEFT OUTER JOIN patient_encounter_details_default ped 
	ON c.initial_encounter_id = ped.encounter_id
WHERE c.patient_id IS NOT NULL;