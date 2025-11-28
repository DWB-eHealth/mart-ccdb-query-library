-- The first CTEs build the frame for patients entering and exiting the cohort. This frame is based on NCD forms with visit types of 'initial visit' and 'discharge visit'. The query takes all initial visit dates and matches discharge visit dates if the discharge visit date falls between the initial visit date and the next initial visit date (if present).
WITH initial AS (
	SELECT 
		patient_id, encounter_id AS initial_encounter_id, visit_location AS initial_visit_location, date AS initial_visit_date, DENSE_RANK () OVER (PARTITION BY patient_id ORDER BY date) AS initial_visit_order, LEAD (date) OVER (PARTITION BY patient_id ORDER BY date) AS next_initial_visit_date
	FROM ncd WHERE visit_type = 'Initial visit'),
cohort AS (
	SELECT
		i.patient_id, i.initial_encounter_id, i.initial_visit_location, i.initial_visit_date, CASE WHEN i.initial_visit_order > 1 THEN 'Yes' END readmission, d.encounter_id AS discharge_encounter_id, d.discharge_date2 AS discharge_date, d.patient_outcome
	FROM initial i
	LEFT JOIN (SELECT patient_id, encounter_id, COALESCE(discharge_date::date, date::date) AS discharge_date2, patient_outcome FROM ncd WHERE visit_type = 'Discharge visit') d 
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
		ON c.patient_id = lca.patient_id AND c.initial_visit_date <= lca.appointment_start_time::date AND COALESCE(c.discharge_date::date, CURRENT_DATE) >= lca.appointment_start_time::date),
-- The following CTEs correct for the inccorect age calculation in Bahmni-Mart.
base AS (
	SELECT
		p.person_id,
		p.birthyear,
		p.age :: INT,
		CURRENT_DATE AS t,
		EXTRACT(
			YEAR
			FROM
				CURRENT_DATE
		) :: INT - p.birthyear AS delta
	FROM
		person_details_default p
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
	FROM
		base
),
anchor AS (
	SELECT
		*,
		make_date(
			birthyear,
			EXTRACT(
				MONTH
				FROM
					t
			) :: INT,
			LEAST(
				EXTRACT(
					DAY
					FROM
						t
				) :: INT,
				EXTRACT(
					DAY
					FROM
						(
							date_trunc(
								'month',
								make_date(
									birthyear,
									EXTRACT(
										MONTH
										FROM
											t
									) :: INT,
									1
								)
							) + INTERVAL '1 month' - INTERVAL '1 day'
						)
				) :: INT
			)
		) AS t_md_in_yob
	FROM
		fixed f
),
age_bounds AS (
	SELECT
		person_id,
		age_fixed,
		age_fix_status,
		CASE
			WHEN age_fixed = delta THEN make_date(birthyear, 1, 1)
			WHEN age_fixed = delta - 1 THEN (t_md_in_yob + INTERVAL '1 day') :: DATE
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
	ab.age_fixed :: int AS age_current,
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
		WHEN (((c.initial_visit_date - ab.dob_min) + (c.initial_visit_date - ab.dob_max))::numeric / (2 * 365.2425)) :: INT <= 4 THEN '0-4'
		WHEN (((c.initial_visit_date - ab.dob_min) + (c.initial_visit_date - ab.dob_max))::numeric / (2 * 365.2425)) :: INT >= 5
		AND (((c.initial_visit_date - ab.dob_min) + (c.initial_visit_date - ab.dob_max))::numeric / (2 * 365.2425)) :: INT <= 14 THEN '05-14'
		WHEN (((c.initial_visit_date - ab.dob_min) + (c.initial_visit_date - ab.dob_max))::numeric / (2 * 365.2425)) :: INT >= 15
		AND (((c.initial_visit_date - ab.dob_min) + (c.initial_visit_date - ab.dob_max))::numeric / (2 * 365.2425)) :: INT <= 24 THEN '15-24'
		WHEN (((c.initial_visit_date - ab.dob_min) + (c.initial_visit_date - ab.dob_max))::numeric / (2 * 365.2425)) :: INT >= 25
		AND (((c.initial_visit_date - ab.dob_min) + (c.initial_visit_date - ab.dob_max))::numeric / (2 * 365.2425)) :: INT <= 34 THEN '25-34'
		WHEN (((c.initial_visit_date - ab.dob_min) + (c.initial_visit_date - ab.dob_max))::numeric / (2 * 365.2425)) :: INT >= 35
		AND (((c.initial_visit_date - ab.dob_min) + (c.initial_visit_date - ab.dob_max))::numeric / (2 * 365.2425)) :: INT <= 44 THEN '35-44'
		WHEN (((c.initial_visit_date - ab.dob_min) + (c.initial_visit_date - ab.dob_max))::numeric / (2 * 365.2425)) :: INT >= 45
		AND (((c.initial_visit_date - ab.dob_min) + (c.initial_visit_date - ab.dob_max))::numeric / (2 * 365.2425)) :: INT <= 54 THEN '45-54'
		WHEN (((c.initial_visit_date - ab.dob_min) + (c.initial_visit_date - ab.dob_max))::numeric / (2 * 365.2425)) :: INT >= 55
		AND (((c.initial_visit_date - ab.dob_min) + (c.initial_visit_date - ab.dob_max))::numeric / (2 * 365.2425)) :: INT <= 64 THEN '55-64'
		WHEN (((c.initial_visit_date - ab.dob_min) + (c.initial_visit_date - ab.dob_max))::numeric / (2 * 365.2425)) :: INT >= 65 THEN '65+'
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
LEFT OUTER JOIN age_bounds ab 
	ON c.patient_id = ab.person_id
LEFT OUTER JOIN patient_encounter_details_default ped 
	ON c.initial_encounter_id = ped.encounter_id
LEFT OUTER JOIN last_visit_location lvl
	ON c.initial_encounter_id = lvl.initial_encounter_id
WHERE c.patient_id IS NOT NULL;