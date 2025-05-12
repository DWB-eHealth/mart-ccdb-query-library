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
	FROM psychiatrist_mhgap_follow_up pmfu)
-- Main query --
SELECT 
	DISTINCT ON (cc.patient_id, cc.date::date, cc.visit_location, cc.intervention_setting, cc.type_of_activity, cc.visit_type, cc.provider_type) cc.patient_id,
	pi."Patient_Identifier",
	c.intake_encounter_id,
	pdd.age AS age_current,
	CASE 
		WHEN pdd.age::int <= 3 THEN '0-3'
		WHEN pdd.age::int >= 4 AND pdd.age::int <= 7 THEN '04-07'
		WHEN pdd.age::int >= 8 AND pdd.age::int <= 14 THEN '08-14'
		WHEN pdd.age::int >= 15 AND pdd.age::int <= 17 THEN '15-17'
		WHEN pdd.age::int >= 18 AND pdd.age::int <= 59 THEN '18-59'
		WHEN pdd.age::int >= 60 THEN '60+'
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
LEFT OUTER JOIN person_details_default pdd 
	ON cc.patient_id = pdd.person_id
LEFT OUTER JOIN person_attributes pa
	ON c.patient_id = pa.person_id;