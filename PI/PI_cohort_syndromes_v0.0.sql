-- Returns one row per syndrome recorded during each initial assessment.
SELECT
	pcia.patient_id,
	pcia.encounter_id,
	pcia.date::date AS initial_assessment_date,
	syndrome.value AS syndrome,
	syndrome.type AS syndrome_type
FROM psy_counselors_initial_assessment pcia
CROSS JOIN LATERAL (
	VALUES
		(NULLIF(BTRIM(pcia.main_syndrome), ''), 'Principal'),
		(NULLIF(BTRIM(pcia.additional_syndrome), ''), 'Additional')
) AS syndrome(value, type)
WHERE syndrome.value IS NOT NULL
ORDER BY
	pcia.patient_id,
	initial_assessment_date,
	pcia.encounter_id,
	syndrome.type;
