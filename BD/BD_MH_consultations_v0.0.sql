-- The first CTE builds the frame for patients entering and exiting the cohort. This frame is based on the MH intake form and the MH discharge form. The query takes all intake dates and matches discharge dates if the discharge date falls between the intake date and the next intake date (if present).
WITH intake AS (
	SELECT 
		patient_id, encounter_id AS intake_encounter_id, date::date AS intake_date, DENSE_RANK () OVER (PARTITION BY patient_id ORDER BY date) AS intake_order, LEAD (date::date) OVER (PARTITION BY patient_id ORDER BY date) AS next_intake_date
	FROM mental_health_intake),
cohort AS (
	SELECT
		i.patient_id, i.intake_encounter_id, i.intake_date, CASE WHEN i.intake_order > 1 THEN 'Yes' END readmission, mhd.encounter_id AS discharge_encounter_id, mhd.discharge_date::date
	FROM intake i
	LEFT JOIN mental_health_discharge mhd 
		ON i.patient_id = mhd.patient_id AND mhd.discharge_date >= i.intake_date AND (mhd.discharge_date < i.next_intake_date OR i.next_intake_date IS NULL)),
-- The Consulations sub-table creates a master table of all consultations/sessions as reported by the clinical forms.
consultations_cte AS (
	SELECT
		pcia.date::date,
		pcia.patient_id,
		pcia.visit_location,
		pcia.intervention_setting,
		'Individual session' AS type_of_activity,
		'Initial' AS visit_type,
		CASE WHEN provider_type IS NULL THEN 'Counselor' ELSE provider_type END AS provider_type,
		pcia.encounter_id
	FROM psy_counselors_initial_assessment pcia 
	UNION
	SELECT
		pmia.date::date,
		pmia.patient_id,
		pmia.visit_location,
		pmia.intervention_setting,
		'Individual session' AS type_of_activity,
		'Initial' AS visit_type,
		CASE WHEN provider_type IS NULL THEN 'Psychiatrist' ELSE provider_type END AS provider_type,
		pmia.encounter_id
	FROM psychiatrist_mhgap_initial_assessment pmia
	UNION
	SELECT
		pcfu.date::date,
		pcfu.patient_id,
		pcfu.visit_location,
		pcfu.intervention_setting,
		pcfu.type_of_activity,
		'Follow up' AS visit_type,
		CASE WHEN provider_type IS NULL THEN 'Counselor' ELSE provider_type END AS provider_type,
		pcfu.encounter_id
	FROM psy_counselors_follow_up pcfu 
	UNION
	SELECT 
		pmfu.date::date,
		pmfu.patient_id,
		pmfu.visit_location,
		pmfu.intervention_setting,
		pmfu.type_of_activity,
		'Follow up' AS visit_type,
		CASE WHEN provider_type IS NULL THEN 'Psychiatrist' ELSE provider_type END AS provider_type,
		pmfu.encounter_id
	FROM psychiatrist_mhgap_follow_up pmfu),
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
	DISTINCT ON (cc.patient_id, cc.date::date, cc.visit_location, cc.intervention_setting, cc.type_of_activity, cc.visit_type, cc.provider_type) cc.patient_id,
	pi."Patient_Identifier",
	c.intake_encounter_id,
	ab.age_fixed::int AS age_current,
	CASE
		WHEN ab.age_fixed::int <= 3 THEN '0-3'
		WHEN ab.age_fixed::int >= 4
		AND ab.age_fixed::int <= 7 THEN '04-07'
		WHEN ab.age_fixed::int >= 8
		AND ab.age_fixed::int <= 14 THEN '08-14'
		WHEN ab.age_fixed::int >= 15
		AND ab.age_fixed::int <= 17 THEN '15-17'
		WHEN ab.age_fixed::int >= 18
		AND ab.age_fixed::int <= 59 THEN '18-59'
		WHEN ab.age_fixed::int >= 60 THEN '60+'
		ELSE NULL
	END AS age_group_current,
	pdd.gender,
	pa."patientCity" AS camp_location, 
	pa."patientDistrict" AS block,
	pa."Subblock" AS subblock,
	cc.date::date AS visit_date,
	cc.visit_location,
	cc.intervention_setting,
	cc.type_of_activity,
	cc.visit_type,
	cc.provider_type,
	cc.encounter_id
FROM consultations_cte cc
LEFT OUTER JOIN cohort c
	ON cc.patient_id = c.patient_id AND cc.date >= c.intake_date AND (cc.date <= c.discharge_date OR c.discharge_date is NULL)
LEFT OUTER JOIN patient_identifier pi
	ON cc.patient_id = pi.patient_id
LEFT OUTER JOIN age_bounds ab 
	ON c.patient_id = ab.person_id
LEFT OUTER JOIN person_details_default pdd 
	ON cc.patient_id = pdd.person_id
LEFT OUTER JOIN person_attributes pa
	ON c.patient_id = pa.person_id;