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
-- The first psy initial assessment CTE extracts the date from the first psy initial assessment. If multiple initial assessments are completed per cohort enrollment then the first is used.
first_psy_initial_assessment AS (
	SELECT DISTINCT ON (c.patient_id, c.intake_encounter_id, c.intake_date, c.discharge_date) c.intake_encounter_id, pcia.date::date
	FROM cohort c
	LEFT OUTER JOIN psy_counselors_initial_assessment pcia
		ON c.patient_id = pcia.patient_id
	WHERE (pcia.date::date >= c.intake_date) AND (pcia.date::date <= c.discharge_date OR c.discharge_date IS NULL)
	GROUP BY c.patient_id, c.intake_encounter_id, c.intake_date, c.discharge_date, pcia.date
	ORDER BY c.patient_id, c.intake_encounter_id, c.intake_date, c.discharge_date, pcia.date ASC),
-- The first clinician initial assessment CTE extracts the date from the first clinician initial assesment. If multiple initial assessments are completed per cohort enrollment then the first is used.
first_clinician_initial_assessment AS (
	SELECT DISTINCT ON (c.patient_id, c.intake_encounter_id, c.intake_date, c.discharge_date) c.intake_encounter_id, pmia.date::date
	FROM cohort c
	LEFT OUTER JOIN psychiatrist_mhgap_initial_assessment pmia 
		ON c.patient_id = pmia.patient_id
	WHERE (pmia.date::date >= c.intake_date) AND (pmia.date::date <= c.discharge_date OR c.discharge_date IS NULL)
	GROUP BY c.patient_id, c.intake_encounter_id, c.intake_date, c.discharge_date, pmia.date
	ORDER BY c.patient_id, c.intake_encounter_id, c.intake_date, c.discharge_date, pmia.date ASC),
-- The visit location CTE finds the last visit location reported across all clinical consultaiton/session forms.
last_visit_location AS (	
	SELECT 
		DISTINCT ON (c.patient_id, c.intake_encounter_id, c.intake_date, c.discharge_date) c.intake_encounter_id,
		vl.visit_location AS visit_location
	FROM cohort c
	LEFT OUTER JOIN (
		SELECT pcia.date::date, pcia.patient_id, pcia.visit_location FROM psy_counselors_initial_assessment pcia WHERE pcia.visit_location IS NOT NULL 
		UNION 
		SELECT pmia.date::date, pmia.patient_id, pmia.visit_location FROM psychiatrist_mhgap_initial_assessment pmia WHERE pmia.visit_location IS NOT NULL 
		UNION
		SELECT pcfu.date::date, pcfu.patient_id, pcfu.visit_location FROM psy_counselors_follow_up pcfu WHERE pcfu.visit_location IS NOT NULL 
		UNION
		SELECT pmfu.date::date, pmfu.patient_id, pmfu.visit_location FROM psychiatrist_mhgap_follow_up pmfu WHERE pmfu.visit_location IS NOT NULL
		UNION
		SELECT mhd.discharge_date AS date, mhd.patient_id, mhd.visit_location FROM mental_health_discharge mhd WHERE mhd.visit_location IS NOT NULL) vl
		ON c.patient_id = vl.patient_id
	WHERE vl.date >= c.intake_date AND (vl.date <= c.discharge_date OR c.discharge_date IS NULL)
	GROUP BY c.patient_id, c.intake_encounter_id, c.intake_date, c.discharge_date, vl.date, vl.visit_location
	ORDER BY c.patient_id, c.intake_encounter_id, c.intake_date, c.discharge_date, vl.date DESC),
-- THe Stressor CTE creates a vertical list of all stressors.
stressor_vertical AS (
    SELECT 
        encounter_id,
        stressor_1 AS stressor
    FROM mental_health_intake
    UNION
    SELECT
        encounter_id,
        stressor_2 AS stressor
    FROM mental_health_intake
    UNION
    SELECT 
        encounter_id,
        stressor_3 AS stressor
    FROM mental_health_intake),
stressors AS (
	SELECT
		DISTINCT ON (encounter_id, stressor) encounter_id,
		stressor
	FROM stressor_vertical
	where stressor IS NOT NULL),
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
	pi."Patient_Identifier",
	c.patient_id,
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
	(((c.intake_date - ab.dob_min) + (c.intake_date - ab.dob_max))::numeric / (2 * 365.2425))::int AS age_admission, 
	CASE
		WHEN (((c.intake_date - ab.dob_min) + (c.intake_date - ab.dob_max))::numeric / (2 * 365.2425))::int  <= 3 THEN '0-3'
		WHEN (((c.intake_date - ab.dob_min) + (c.intake_date - ab.dob_max))::numeric / (2 * 365.2425))::int  >= 4
		AND (((c.intake_date - ab.dob_min) + (c.intake_date - ab.dob_max))::numeric / (2 * 365.2425))::int  <= 7 THEN '04-07'
		WHEN (((c.intake_date - ab.dob_min) + (c.intake_date - ab.dob_max))::numeric / (2 * 365.2425))::int  >= 8
		AND (((c.intake_date - ab.dob_min) + (c.intake_date - ab.dob_max))::numeric / (2 * 365.2425))::int  <= 14 THEN '08-14'
		WHEN (((c.intake_date - ab.dob_min) + (c.intake_date - ab.dob_max))::numeric / (2 * 365.2425))::int  >= 15
		AND (((c.intake_date - ab.dob_min) + (c.intake_date - ab.dob_max))::numeric / (2 * 365.2425))::int  <= 17 THEN '15-17'
		WHEN (((c.intake_date - ab.dob_min) + (c.intake_date - ab.dob_max))::numeric / (2 * 365.2425))::int  >= 18
		AND (((c.intake_date - ab.dob_min) + (c.intake_date - ab.dob_max))::numeric / (2 * 365.2425))::int  <= 59 THEN '18-59'
		WHEN (((c.intake_date - ab.dob_min) + (c.intake_date - ab.dob_max))::numeric / (2 * 365.2425))::int  >= 60 THEN '60+'
		ELSE NULL
	END AS age_group_admission,
	pdd.gender,
	c.intake_date, 
	CASE 
		WHEN fpia.date IS NOT NULL AND fcia.date IS NULL THEN fpia.date
		WHEN fcia.date IS NOT NULL AND fpia.date IS NULL THEN fcia.date
		WHEN fpia.date IS NOT NULL AND fcia.date IS NOT NULL AND fcia.date::date <= fpia.date::date THEN fcia.date
		WHEN fpia.date IS NOT NULL AND fcia.date IS NOT NULL AND fcia.date::date > fpia.date::date THEN fpia.date
		ELSE NULL
	END	AS enrollment_date,
	c.discharge_date,
	CASE 
		WHEN (fpia.date IS NOT NULL OR fcia.date IS NOT NULL) AND c.discharge_date IS NULL THEN 'Yes'
		ELSE null
	END AS in_cohort,
	c.readmission,	
	mhi.visit_location AS entry_visit_location,
	lvl.visit_location,
	s.stressor
FROM stressors s
LEFT OUTER JOIN cohort c
    ON s.encounter_id = c.intake_encounter_id
LEFT OUTER JOIN first_psy_initial_assessment fpia 
	ON c.intake_encounter_id = fpia.intake_encounter_id
LEFT OUTER JOIN first_clinician_initial_assessment fcia 
	ON c.intake_encounter_id = fcia.intake_encounter_id
LEFT OUTER JOIN patient_identifier pi
	ON c.patient_id = pi.patient_id
LEFT OUTER JOIN age_bounds ab 
	ON c.patient_id = ab.person_id
LEFT OUTER JOIN person_details_default pdd 
	ON c.patient_id = pdd.person_id
LEFT OUTER JOIN patient_encounter_details_default ped 
	ON c.intake_encounter_id = ped.encounter_id
LEFT OUTER JOIN mental_health_intake mhi
	ON c.intake_encounter_id = mhi.encounter_id
LEFT OUTER JOIN last_visit_location lvl
	ON c.intake_encounter_id = lvl.intake_encounter_id;