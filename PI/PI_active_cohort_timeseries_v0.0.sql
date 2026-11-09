-- Counts patients in the active cohort at each month end, using the cohort status
-- criteria from PI_cohort_register_per_patient.
WITH intake AS (
	SELECT
		mhi.patient_id,
		mhi.encounter_id AS intake_encounter_id,
		mhi.date::date AS intake_date,
		LEAD(mhi.date::date) OVER (
			PARTITION BY mhi.patient_id
			ORDER BY mhi.date::date
		) AS next_intake_date
	FROM mental_health_intake mhi
),
cohort AS (
	SELECT DISTINCT ON (i.patient_id, i.intake_date)
		i.patient_id,
		i.intake_encounter_id,
		i.intake_date,
		i.next_intake_date,
		mhd.discharge_date::date AS discharge_date
	FROM intake i
	LEFT JOIN mental_health_discharge mhd
		ON mhd.patient_id = i.patient_id
		AND mhd.discharge_date::date >= i.intake_date
		AND (i.next_intake_date IS NULL OR mhd.discharge_date::date < i.next_intake_date)
	ORDER BY
		i.patient_id,
		i.intake_date,
		mhd.discharge_date DESC NULLS LAST
),
all_forms AS (
	SELECT patient_id, date::date AS form_date, 'Initial' AS stage
	FROM psychiatrist_mhgap_initial_assessment
	UNION ALL
	SELECT patient_id, date::date, 'Initial'
	FROM psy_counselors_initial_assessment
	UNION ALL
	SELECT patient_id, date::date, 'Follow_up'
	FROM psychiatrist_mhgap_follow_up
	UNION ALL
	SELECT patient_id, date::date, 'Follow_up'
	FROM psy_counselors_follow_up
),
stay_forms AS (
	SELECT
		c.patient_id,
		c.intake_encounter_id,
		c.intake_date,
		af.form_date,
		af.stage
	FROM cohort c
	JOIN all_forms af
		ON af.patient_id = c.patient_id
		AND af.form_date >= c.intake_date
		AND (c.next_intake_date IS NULL OR af.form_date < c.next_intake_date)
		AND (c.discharge_date IS NULL OR af.form_date <= c.discharge_date)
),
followups AS (
	SELECT
		patient_id,
		intake_encounter_id,
		form_date,
		ROW_NUMBER() OVER (
			PARTITION BY patient_id, intake_encounter_id
			ORDER BY form_date
		) AS rn
	FROM stay_forms
	WHERE stage = 'Follow_up'
),
stay_stats AS (
	SELECT
		sf.patient_id,
		sf.intake_encounter_id,
		MIN(sf.form_date) FILTER (WHERE sf.stage = 'Initial') AS first_assessment_date,
		MIN(f.form_date) FILTER (WHERE f.rn = 2) AS second_follow_up_date
	FROM stay_forms sf
	LEFT JOIN followups f
		ON f.patient_id = sf.patient_id
		AND f.intake_encounter_id = sf.intake_encounter_id
	GROUP BY sf.patient_id, sf.intake_encounter_id
),
active_stays AS (
	SELECT
		c.patient_id,
		ss.second_follow_up_date AS active_start_date,
		c.discharge_date,
		c.next_intake_date
	FROM cohort c
	JOIN stay_stats ss
		ON ss.patient_id = c.patient_id
		AND ss.intake_encounter_id = c.intake_encounter_id
	WHERE ss.first_assessment_date IS NOT NULL
		AND ss.second_follow_up_date IS NOT NULL
),
date_range AS (
	SELECT
		(date_trunc('month', CURRENT_DATE) - INTERVAL '1 year')::date AS min_date,
		CURRENT_DATE AS max_date
),
day_range AS (
	SELECT generate_series(min_date, max_date, '1 day'::interval)::date AS reporting_day
	FROM date_range
),
daily_active_patients AS (
	SELECT
		dr.reporting_day,
		COUNT(DISTINCT ast.patient_id) AS active_patients_total,
		CASE
			WHEN dr.reporting_day = (date_trunc('month', dr.reporting_day) + INTERVAL '1 month - 1 day')::date THEN 1
		END AS last_day_of_month
	FROM day_range dr
	LEFT JOIN active_stays ast
		ON ast.active_start_date <= dr.reporting_day
		AND (ast.next_intake_date IS NULL OR dr.reporting_day < ast.next_intake_date)
		AND (ast.discharge_date IS NULL OR dr.reporting_day < ast.discharge_date)
	GROUP BY dr.reporting_day
)
SELECT
	dap.reporting_day,
	dap.active_patients_total
FROM daily_active_patients dap
WHERE dap.last_day_of_month = 1
	AND dap.reporting_day > date_trunc('month', CURRENT_DATE) - INTERVAL '1 year'
ORDER BY dap.reporting_day;
