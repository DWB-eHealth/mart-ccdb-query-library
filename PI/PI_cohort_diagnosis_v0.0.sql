-- Returns one row per main or secondary diagnosis recorded during each psychiatrist mhGap assessment.
WITH assessments AS (
	SELECT patient_id, encounter_id, date::date AS form_date, 'Initial assessment' AS form_type, main_diagnosis, secondary_diagnosis
	FROM psychiatrist_mhgap_initial_assessment
	UNION ALL
	SELECT patient_id, encounter_id, date::date AS form_date, 'Follow-up' AS form_type, main_diagnosis, secondary_diagnosis
	FROM psychiatrist_mhgap_follow_up
),
diagnoses AS (
	SELECT
		assessment.patient_id,
		assessment.encounter_id,
		assessment.form_date,
		assessment.form_type,
		CASE
			WHEN BTRIM(diagnosis.value) = 'Other' THEN 'Other mental health diagnosis'
			ELSE BTRIM(diagnosis.value)
		END AS diagnosis,
		diagnosis.type AS type_of_diagnosis
	FROM assessments assessment
	CROSS JOIN LATERAL (
		VALUES
			(assessment.main_diagnosis, 'Main'),
			(assessment.secondary_diagnosis, 'Secondary')
	) AS diagnosis(value, type)
	WHERE NULLIF(BTRIM(diagnosis.value), '') IS NOT NULL
		AND BTRIM(diagnosis.value) <> 'None'
)
SELECT DISTINCT ON (patient_id, encounter_id, form_date, form_type, diagnosis)
	patient_id,
	encounter_id,
	form_date,
	form_type,
	diagnosis,
	type_of_diagnosis
FROM diagnoses
ORDER BY
	patient_id,
	encounter_id,
	form_date,
	form_type,
	diagnosis,
	CASE type_of_diagnosis WHEN 'Main' THEN 1 ELSE 2 END;
