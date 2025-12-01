-- The following CTEs correct for the inccorect age calculation in Bahmni-Mart.
WITH base AS (
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
	pi."Patient_Identifier",
	pdd.person_id,
	pdd.gender,
	pdd.birthyear,
	ab.age_fixed::int AS age_current_correct,
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
	END AS age_group_current
FROM person_details_default pdd
LEFT OUTER JOIN patient_identifier pi
	ON pdd.person_id = pi.patient_id
LEFT OUTER JOIN age_bounds ab 
	ON pdd.person_id = ab.person_id;