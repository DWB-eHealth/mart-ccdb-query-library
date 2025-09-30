-- *COHORT FRAME
-- The cohort CTE builds the enrollment window for patients entering and exiting the NCD cohort. The enrollment window is based on NCD forms with visit types of 'initial visit' and 'patient outcome', matching all initial visit forms to the cooresponding patient outcome visit form, if present. If an individual patient has more than one initial visit form, then the patient outcome visit form will be matched to the initial visit form where the patient outcome visit form date falls between the first initial visit date and the following initial visit date.
WITH cohort AS (
	SELECT
		i.patient_id,
		i.initial_encounter_id,
		i.initial_visit_location,
		i.initial_visit_date,
		CASE
			WHEN i.initial_visit_order > 1 THEN 'Yes'
		END readmission,
		d.encounter_id AS outcome_encounter_id,
		d.outcome_date2 AS outcome_date,
		d.patient_outcome_at_end_of_msf_care AS patient_outcome
	FROM
		(
			SELECT
				patient_id,
				encounter_id AS initial_encounter_id,
				visit_location AS initial_visit_location,
				date_of_visit AS initial_visit_date,
				DENSE_RANK () OVER (
					PARTITION BY patient_id
					ORDER BY
						date_of_visit
				) AS initial_visit_order,
				LEAD (date_of_visit) OVER (
					PARTITION BY patient_id
					ORDER BY
						date_of_visit
				) AS next_initial_visit_date
			FROM
				ncd_consultations
			WHERE
				visit_type = 'Initial visit'
		) i
		LEFT JOIN (
			SELECT
				patient_id,
				encounter_id,
				date_of_visit AS outcome_date2,
				patient_outcome_at_end_of_msf_care
			FROM
				ncd_consultations
			WHERE
				visit_type = 'Patient outcome'
		) d ON i.patient_id = d.patient_id
		AND (
			d.outcome_date2 IS NULL
			OR (
				d.outcome_date2 >= i.initial_visit_date
				AND (
					d.outcome_date2 < i.next_initial_visit_date
					OR i.next_initial_visit_date IS NULL
				)
			)
		)
),
-- In the cohort_diagnosis CTE, each diagnosis is only counted once per patient and the date for the diagnosis reported is the earliest date that the diagnosis is made. For specific diagnosis groups, only the last reported diagnosis is extraCTEd. These groups are diabetes (includeing Diabetes mellitus, type 1 and Diabetes mellitus, type 2), epilepsy (including Focal epilepsy, Generalised epilepsy, Unclassified epilepsy), and hypertension (including Hypertension Stage 1, Hypertension Stage 2, Hypertension Stage 3).
cohort_diagnosis AS (
	SELECT
		patient_id,
		initial_encounter_id,
		diagnosis,
		MIN(date_of_diagnosis) AS date_of_diagnosis
	FROM
		(
			SELECT
				cd.patient_id,
				cd.initial_encounter_id,
				cd.diagnosis,
				cd.date_of_diagnosis,
				CASE
					WHEN cd.diagnosis_group IN ('Group2', 'Group3', 'Group4') THEN FIRST_VALUE(cd.diagnosis) OVER (
						PARTITION BY cd.patient_id,
						cd.initial_encounter_id,
						cd.diagnosis_group
						ORDER BY
							cd.date_of_diagnosis DESC
					)
					ELSE cd.diagnosis
				END AS selected_diagnosis
			FROM
				(
					SELECT
						c.patient_id,
						c.initial_encounter_id,
						COALESCE(d.date_of_diagnosis, n.date_of_visit) AS date_of_diagnosis,
						d.ncd_cohort_diagnosis AS diagnosis,
						CASE
							WHEN d.ncd_cohort_diagnosis IN (
								'Asthma',
								'Chronic obstructive pulmonary disease',
								'Sickle Cell Disease'
							) THEN 'Group1'
							WHEN d.ncd_cohort_diagnosis IN (
								'Diabetes mellitus, type 1',
								'Diabetes mellitus, type 2'
							) THEN 'Group2'
							WHEN d.ncd_cohort_diagnosis IN (
								'Focal epilepsy',
								'Generalised epilepsy',
								'Unclassified epilepsy'
							) THEN 'Group3'
							WHEN d.ncd_cohort_diagnosis IN (
								'Hypertension Stage 1',
								'Hypertension Stage 2',
								'Hypertension Stage 3'
							) THEN 'Group4'
							ELSE 'Other'
						END AS diagnosis_group
					FROM
						ncd_consultations_ncd_diagnosis d
						LEFT JOIN ncd_consultations n USING(encounter_id)
						JOIN cohort c ON d.patient_id = c.patient_id
						AND c.initial_visit_date <= n.date_of_visit
						AND COALESCE(c.outcome_date, CURRENT_DATE) >= n.date_of_visit
				) cd
		) ranked
	WHERE
		diagnosis = selected_diagnosis
	GROUP BY
		patient_id,
		initial_encounter_id,
		diagnosis
)
-- Main query --
SELECT
	pi. "Patient_Identifier",
	c.patient_id,
	c.initial_encounter_id,
	pdd.age :: INT AS age_current,
	CASE
		WHEN pdd.age :: INT <= 4 THEN '0-4'
		WHEN pdd.age :: INT >= 5
		AND pdd.age :: INT <= 14 THEN '05-14'
		WHEN pdd.age :: INT >= 15
		AND pdd.age :: INT <= 24 THEN '15-24'
		WHEN pdd.age :: INT >= 25
		AND pdd.age :: INT <= 34 THEN '25-34'
		WHEN pdd.age :: INT >= 35
		AND pdd.age :: INT <= 44 THEN '35-44'
		WHEN pdd.age :: INT >= 45
		AND pdd.age :: INT <= 54 THEN '45-54'
		WHEN pdd.age :: INT >= 55
		AND pdd.age :: INT <= 64 THEN '55-64'
		WHEN pdd.age :: INT >= 65 THEN '65+'
		ELSE NULL
	END AS age_group_current,
	EXTRACT(
		YEAR
		FROM
			age(
				ped.encounter_datetime,
				TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')
			)
	) AS age_admission,
	CASE
		WHEN EXTRACT(
			YEAR
			FROM
				age(
					ped.encounter_datetime,
					TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')
				)
		) :: INT <= 4 THEN '0-4'
		WHEN EXTRACT(
			YEAR
			FROM
				age(
					ped.encounter_datetime,
					TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')
				)
		) :: INT >= 5
		AND EXTRACT(
			YEAR
			FROM
				age(
					ped.encounter_datetime,
					TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')
				)
		) <= 14 THEN '05-14'
		WHEN EXTRACT(
			YEAR
			FROM
				age(
					ped.encounter_datetime,
					TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')
				)
		) :: INT >= 15
		AND EXTRACT(
			YEAR
			FROM
				age(
					ped.encounter_datetime,
					TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')
				)
		) <= 24 THEN '15-24'
		WHEN EXTRACT(
			YEAR
			FROM
				age(
					ped.encounter_datetime,
					TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')
				)
		) :: INT >= 25
		AND EXTRACT(
			YEAR
			FROM
				age(
					ped.encounter_datetime,
					TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')
				)
		) <= 34 THEN '25-34'
		WHEN EXTRACT(
			YEAR
			FROM
				age(
					ped.encounter_datetime,
					TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')
				)
		) :: INT >= 35
		AND EXTRACT(
			YEAR
			FROM
				age(
					ped.encounter_datetime,
					TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')
				)
		) <= 44 THEN '35-44'
		WHEN EXTRACT(
			YEAR
			FROM
				age(
					ped.encounter_datetime,
					TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')
				)
		) :: INT >= 45
		AND EXTRACT(
			YEAR
			FROM
				age(
					ped.encounter_datetime,
					TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')
				)
		) <= 54 THEN '45-54'
		WHEN EXTRACT(
			YEAR
			FROM
				age(
					ped.encounter_datetime,
					TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')
				)
		) :: INT >= 55
		AND EXTRACT(
			YEAR
			FROM
				age(
					ped.encounter_datetime,
					TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')
				)
		) <= 64 THEN '55-64'
		WHEN EXTRACT(
			YEAR
			FROM
				age(
					ped.encounter_datetime,
					TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')
				)
		) :: INT >= 65 THEN '65+'
		ELSE NULL
	END AS age_group_admission,
	pdd.gender,
	pad.city_village AS county,
	pad.state_province AS sub_county,
	pad.county_district AS ward,
	pad.address2 AS village,
	c.initial_visit_date AS enrollment_date,
	CASE
		WHEN C .outcome_date IS NULL THEN 'Yes'
	END AS in_cohort,
	c.outcome_date,
	c.patient_outcome,
	cd.diagnosis,
	CASE
		WHEN cd.diagnosis IN (
			'Diabetes mellitus, type 1',
			'Diabetes mellitus, type 2'
		) THEN 'Diabetes'
		WHEN cd.diagnosis IN (
			'Focal epilepsy',
			'Generalised epilepsy',
			'Unclassified epilepsy'
		) THEN 'Epilepsy'
		WHEN cd.diagnosis IN (
			'Hypertension Stage 1',
			'Hypertension Stage 2',
			'Hypertension Stage 3'
		) THEN 'Hypertension'
		ELSE cd.diagnosis
	END AS diagnosis_simplified
FROM
	cohort_diagnosis cd
LEFT OUTER JOIN cohort c
	ON cd.initial_encounter_id = c.initial_encounter_id
LEFT OUTER JOIN patient_identifier pi
	ON c.patient_id = pi.patient_id
LEFT OUTER JOIN person_details_default pdd 
	ON c.patient_id = pdd.person_id
LEFT OUTER JOIN person_address_default pad 
	ON c.patient_id = pad.person_id
LEFT OUTER JOIN patient_encounter_details_default ped 
	ON c.initial_encounter_id = ped.encounter_id
WHERE c.patient_id IS NOT NULL;