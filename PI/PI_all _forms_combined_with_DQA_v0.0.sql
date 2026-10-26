-- Cohort activity timeline and data-quality audit.
WITH all_forms AS (
	SELECT
		patient_id,
		encounter_id,
		"date"::date AS encounter_date,
		visit_location,
		NULL::text AS intervention_setting,
		'Mental Health Intake' AS form_name,
		date_created,
		date_modified,
		NULL::text AS type_of_activity,
		NULL::text AS visit_type,
		NULL::text AS provider_type,
		1 AS form_order
	FROM mental_health_intake

	UNION ALL

	SELECT
		patient_id,
		encounter_id,
		"date"::date,
		visit_location,
		intervention_setting,
		'Psy Counselors Initial Assessment',
		date_created,
		date_modified,
		'Individual session',
		'Initial',
		'Psychologist',
		2
	FROM psy_counselors_initial_assessment

	UNION ALL

	SELECT
		patient_id,
		encounter_id,
		"date"::date,
		visit_location,
		intervention_setting,
		'Psy Counselors Follow Up',
		date_created,
		date_modified,
		type_of_activity,
		'Follow up',
		'Psychologist',
		3
	FROM psy_counselors_follow_up

	UNION ALL

	SELECT
		patient_id,
		encounter_id,
		"date"::date,
		visit_location,
		intervention_setting,
		'Psychiatrist mhGAP Initial Assessment',
		date_created,
		date_modified,
		'Individual session',
		'Initial',
		'mhGAP doctor',
		4
	FROM psychiatrist_mhgap_initial_assessment

	UNION ALL

	SELECT
		patient_id,
		encounter_id,
		"date"::date,
		visit_location,
		intervention_setting,
		'Psychiatrist mhGAP Follow Up',
		date_created,
		date_modified,
		type_of_activity,
		'Follow up',
		'mhGAP doctor',
		5
	FROM psychiatrist_mhgap_follow_up

	UNION ALL

	SELECT
		patient_id,
		encounter_id,
		"discharge_date"::date,
		visit_location,
		NULL::text,
		'Mental Health Discharge',
		date_created,
		date_modified,
		NULL::text,
		NULL::text,
		NULL::text,
		6
	FROM mental_health_discharge
),
ordered AS (
	SELECT
		all_forms.*,
		COALESCE(
			SUM(CASE WHEN form_name = 'Mental Health Discharge' THEN 1 ELSE 0 END) OVER (
				PARTITION BY patient_id
				ORDER BY encounter_date, form_order, encounter_id
				ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
			),
			0
		) AS discharges_before
	FROM all_forms
),
stay_starts AS (
	SELECT
		ordered.*,
		CASE
			WHEN form_name = 'Mental Health Intake'
				AND discharges_before > COALESCE(
					MAX(CASE WHEN form_name = 'Mental Health Intake' THEN discharges_before END) OVER (
						PARTITION BY patient_id
						ORDER BY encounter_date, form_order, encounter_id
						ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
					),
					0
				)
			THEN 1
			ELSE 0
		END AS is_new_stay
	FROM ordered
),
staged AS (
	SELECT
		stay_starts.*,
		1 + SUM(is_new_stay) OVER (
			PARTITION BY patient_id
			ORDER BY encounter_date, form_order, encounter_id
			ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
		) AS stay_number
	FROM stay_starts
),
base AS (
	SELECT
		patient_id,
		encounter_id,
		encounter_date,
		visit_location,
		intervention_setting,
		form_name,
		date_created,
		date_modified,
		type_of_activity,
		visit_type,
		provider_type,
		form_order,
		stay_number,
		DENSE_RANK() OVER (
			PARTITION BY patient_id
			ORDER BY encounter_date
		) AS patient_day_number,
		CASE
			WHEN form_name IN ('Psy Counselors Follow Up', 'Psychiatrist mhGAP Follow Up') THEN
				CASE
					WHEN COUNT(*) OVER (
						PARTITION BY patient_id, encounter_date, form_name, type_of_activity
					) > 1 THEN 'Yes'
					ELSE 'No'
				END
			ELSE
				CASE
					WHEN COUNT(*) OVER (
						PARTITION BY patient_id, encounter_date, form_name
					) > 1 THEN 'Yes'
					ELSE 'No'
				END
		END AS more_than_one_form_per_day,
		CASE
			WHEN form_name IN ('Psy Counselors Follow Up', 'Psychiatrist mhGAP Follow Up')
				AND COUNT(*) OVER (
					PARTITION BY patient_id, encounter_date, form_name
				) > 1
				AND MIN(type_of_activity) OVER (
					PARTITION BY patient_id, encounter_date, form_name
				) <> MAX(type_of_activity) OVER (
					PARTITION BY patient_id, encounter_date, form_name
				)
				AND MAX(CASE WHEN type_of_activity = 'Missed appointment' THEN 1 ELSE 0 END) OVER (
					PARTITION BY patient_id, encounter_date, form_name
				) = 1
			THEN 'Yes'
			ELSE NULL
		END AS missed_appointment_other_activity,
		LEAD(encounter_id) OVER (
			PARTITION BY patient_id
			ORDER BY encounter_date, form_order, encounter_id
		) AS next_encounter_id,
		LEAD(encounter_date) OVER (
			PARTITION BY patient_id
			ORDER BY encounter_date, form_order, encounter_id
		) AS next_encounter_date,
		LEAD(form_name) OVER (
			PARTITION BY patient_id
			ORDER BY encounter_date, form_order, encounter_id
		) AS next_form_name
	FROM staged
),
stays AS (
	SELECT
		base.*,
		MIN(CASE WHEN form_name = 'Mental Health Intake' THEN encounter_date END) OVER (
			PARTITION BY patient_id, stay_number
		) AS stay_intake_date,
		MIN(CASE WHEN form_name = 'Psy Counselors Initial Assessment' THEN encounter_date END) OVER (
			PARTITION BY patient_id, stay_number
		) AS stay_psy_ia_date,
		MIN(CASE WHEN form_name = 'Psychiatrist mhGAP Initial Assessment' THEN encounter_date END) OVER (
			PARTITION BY patient_id, stay_number
		) AS stay_mhgap_ia_date,
		ROW_NUMBER() OVER (
			PARTITION BY patient_id, stay_number, form_name
			ORDER BY encounter_date, form_order, encounter_id
		) AS form_rank_in_stay
	FROM base
)
SELECT
	patient_id,
	encounter_id,
	encounter_date,
	visit_location,
	intervention_setting,
	form_name,
	date_created AS "Date Created",
	date_modified AS "Date Modified",
	type_of_activity AS "Type of Activity",
	visit_type,
	provider_type,
	patient_day_number,
	more_than_one_form_per_day,
	missed_appointment_other_activity AS "Missed appointment with other activity same day",
	stay_number,
	next_encounter_id AS "Next consultation encounter ID",
	next_encounter_date AS "Next consultation encounter date",
	next_form_name AS "Next consultation form name",
	CASE
		WHEN form_name = 'Mental Health Discharge'
			AND (next_form_name <> 'Mental Health Intake' OR next_form_name IS NULL)
		THEN 'Yes'
		ELSE NULL
	END AS "Discharged not followed by intake",
	CASE
		WHEN form_name = 'Mental Health Intake' AND form_rank_in_stay > 1 THEN 'Yes'
		ELSE NULL
	END AS "Duplicate intake",
	CASE
		WHEN form_name = 'Mental Health Discharge' AND form_rank_in_stay > 1 THEN 'Yes'
		ELSE NULL
	END AS "Duplicate discharge",
	CASE
		WHEN form_name = 'Mental Health Intake'
			AND (
				next_form_name NOT IN (
					'Psy Counselors Initial Assessment',
					'Psychiatrist mhGAP Initial Assessment'
				)
				OR next_form_name IS NULL
			)
		THEN 'Yes'
		ELSE NULL
	END AS "Intake not followed by initial assessment",
	CASE
		WHEN stay_intake_date IS NULL THEN 'Yes'
		ELSE NULL
	END AS "Stay without intake",
	CASE
		WHEN stay_psy_ia_date IS NULL AND stay_mhgap_ia_date IS NULL THEN 'Yes'
		ELSE NULL
	END AS "Stay without initial assessment",
	CASE
		WHEN form_name = 'Psy Counselors Follow Up'
			AND (stay_psy_ia_date IS NULL OR stay_psy_ia_date > encounter_date)
		THEN 'Yes'
		ELSE NULL
	END AS "Psy follow up without prior initial assessment",
	CASE
		WHEN form_name = 'Psychiatrist mhGAP Follow Up'
			AND (stay_mhgap_ia_date IS NULL OR stay_mhgap_ia_date > encounter_date)
		THEN 'Yes'
		ELSE NULL
	END AS "mhGAP follow up without prior initial assessment"
FROM stays
ORDER BY
	patient_id,
	encounter_date,
	form_order,
	encounter_id;
