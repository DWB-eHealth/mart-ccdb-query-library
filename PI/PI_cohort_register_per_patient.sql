WITH patients AS (
  SELECT
    pi.patient_id,
    pi."Patient_Identifier" AS patient_identifier,
    rd.date_created::date AS registration_date
  FROM patient_identifier pi
  LEFT JOIN registration_date rd
    ON rd.patient_id = pi.patient_id
  WHERE pi."Patient_Identifier" IS NOT NULL
    AND pi."Patient_Identifier" <> ''
),

/* ================== ALL FORMS ===================== */
all_forms AS (
  SELECT patient_id, "date"::date AS form_date, visit_location, 'Psychiatrist' AS provider, 'Initial' AS stage
  FROM psychiatrist_mhgap_initial_assessment
  UNION ALL
  SELECT patient_id, "date"::date, visit_location, 'Psychologist', 'Initial'
  FROM psy_counselors_initial_assessment
  UNION ALL
  SELECT patient_id, "date"::date, visit_location, 'Psychiatrist', 'Follow_up'
  FROM psychiatrist_mhgap_follow_up
  UNION ALL
  SELECT patient_id, "date"::date, visit_location, 'Psychologist', 'Follow_up'
  FROM psy_counselors_follow_up
),

/* ===================== INTAKES ===================== */
intake AS (
  SELECT
    mhi.patient_id,
    mhi.encounter_id AS intake_encounter_id,
    mhi."date"::date AS intake_date,
    mhi.visit_location AS visit_location_intake,
    mhi.source_of_initial_patient_referral,
    ROW_NUMBER() OVER (
      PARTITION BY mhi.patient_id
      ORDER BY mhi."date"
    ) AS intake_order,
    LEAD(mhi."date"::date) OVER (
      PARTITION BY mhi.patient_id
      ORDER BY mhi."date"
    ) AS next_intake_date
  FROM mental_health_intake mhi
),

/* ===================== DISCHARGE ===================== */
cohort AS (
  SELECT DISTINCT ON (i.patient_id, i.intake_date)
    i.patient_id,
    i.intake_date,
    i.intake_encounter_id,
    d.discharge_date::date AS discharge_date,
    d.encounter_id AS discharge_encounter_id,
    d.patient_outcome AS discharge_status,
    d.mhos_at_discharge,
    d.cgi_s_score_at_discharge AS cgi_s_discharge,
    d.cgi_i_score_at_discharge AS cgi_i_discharge
  FROM intake i
  LEFT JOIN mental_health_discharge d
    ON d.patient_id = i.patient_id
   AND d.discharge_date::date >= i.intake_date
   AND (i.next_intake_date IS NULL OR d.discharge_date::date < i.next_intake_date)
  ORDER BY i.patient_id, i.intake_date, d.discharge_date DESC NULLS LAST
),

/* ===================== LAST STAY ===================== */
last_stay AS (
  SELECT *
  FROM (
    SELECT
      i.*,
      c.discharge_date,
      c.discharge_encounter_id,
      c.discharge_status,
      c.mhos_at_discharge,
      c.cgi_s_discharge,
      c.cgi_i_discharge,
      CASE
        WHEN i.intake_order > 1 THEN 'Yes'
        ELSE 'No'
      END AS readmission,
      ROW_NUMBER() OVER (
        PARTITION BY i.patient_id
        ORDER BY i.intake_date DESC
      ) AS rn
    FROM intake i
    LEFT JOIN cohort c
      ON c.patient_id = i.patient_id
     AND c.intake_date = i.intake_date
  ) x
  WHERE rn = 1
),

/* ===================== FORMS IN STAY ===================== */
stay_forms AS (
  SELECT
    ls.patient_id,
    ls.intake_date,
    af.form_date,
    af.stage,
    af.provider,
    af.visit_location
  FROM last_stay ls
  LEFT JOIN all_forms af
    ON af.patient_id = ls.patient_id
   AND af.form_date >= ls.intake_date
   AND (ls.discharge_date IS NULL OR af.form_date <= ls.discharge_date)
),

/* ===================== FOLLOW-UP RANKING ===================== */
followups AS (
  SELECT
    patient_id,
    intake_date,
    form_date,
    ROW_NUMBER() OVER (
      PARTITION BY patient_id, intake_date ORDER BY form_date
    ) AS rn
  FROM stay_forms
  WHERE stage = 'Follow_up'
),

/* ===================== STATS ===================== */
stay_stats AS (
  SELECT
    sf.patient_id,
    sf.intake_date,
    MIN(sf.form_date) FILTER (WHERE sf.stage = 'Initial') AS first_assessment_date,
    MIN(sf.form_date) FILTER (WHERE sf.stage = 'Follow_up') AS first_follow_up_date,
    MIN(f.form_date) FILTER (WHERE f.rn = 2) AS second_follow_up_date,
        MAX(sf.form_date) AS last_consultation_date
  FROM stay_forms sf
  LEFT JOIN followups f
    ON f.patient_id = sf.patient_id
   AND f.intake_date = sf.intake_date
  GROUP BY sf.patient_id, sf.intake_date
),

/* =====================================================
CONSULT TYPES
   ===================================================== */
consult_types AS (
  SELECT
    patient_id,
    intake_date,
    form_date,
    CASE
      WHEN COUNT(DISTINCT provider) > 1 THEN 'Both'
      ELSE MAX(provider)
    END AS consult_type,
    MAX(visit_location) AS visit_location
  FROM stay_forms
  GROUP BY patient_id, intake_date, form_date
),

/* ===================== PROVIDER COUNTS ===================== */
provider_counts AS (
  SELECT
    patient_id,
    intake_date,
    COUNT(*) FILTER (WHERE provider = 'Psychiatrist') AS psychiatrist_consults,
    COUNT(*) FILTER (WHERE provider = 'Psychologist') AS psychologist_consults
  FROM stay_forms
  GROUP BY patient_id, intake_date
),

/* ===================== APPOINTMENTS ===================== */
appointments AS (
  SELECT DISTINCT ON (patient_id)
    patient_id,
    appointment_start_time::date AS last_appointment_date,
    appointment_status
  FROM patient_appointment_default
  ORDER BY patient_id, appointment_start_time DESC
),

/* ===================== PRESCRIPTION ===================== */
psychotropic AS (
  SELECT
    c.patient_id,
    c.intake_encounter_id,
    CASE WHEN COUNT(*) FILTER (WHERE mdd.coded_drug_name IS NOT NULL) > 0 THEN 'Yes' ELSE 'No' END AS psychotropic_prescription
  FROM cohort c
  LEFT JOIN medication_data_default mdd
    ON mdd.patient_id = c.patient_id
   AND mdd.start_date >= c.intake_date
   AND (mdd.start_date <= c.discharge_date OR c.discharge_date IS NULL)
   AND mdd.coded_drug_name IS NOT NULL
   AND mdd.coded_drug_name NOT IN (
     'IBUPROFEN, 400 mg, tab.',
     'PARACETAMOL (acetaminophen), 500 mg, tab.'
   )
  GROUP BY c.patient_id, c.intake_encounter_id
),

/* ===================== STRESSORS (FROM INTAKE) ===================== */
stressors AS (
  SELECT
    ls.patient_id,
    ls.intake_date,
        MAX(CASE WHEN s = 'Non-conflict-related medical condition' THEN 1 ELSE 0 END) AS stressor_non_conflict_medical,
        MAX(CASE WHEN s = 'Conflict-related medical condition' THEN 1 ELSE 0 END) AS stressor_conflict_medical,
        MAX(CASE WHEN s = 'Pre-existing mental health disorder' THEN 1 ELSE 0 END) AS stressor_pre_existing_mhd,
        MAX(CASE WHEN s = 'Extreme poverty / Financial crisis' THEN 1 ELSE 0 END) AS stressor_extreme_poverty,
        MAX(CASE WHEN s = 'Hard living due to conflict' THEN 1 ELSE 0 END) AS stressor_hard_living_conflict,
        MAX(CASE WHEN s = 'House / property destroyed' THEN 1 ELSE 0 END) AS stressor_house_destroyed,
        MAX(CASE WHEN s = 'Intra-family related problem' THEN 1 ELSE 0 END) AS stressor_intrafamily_problem,
        MAX(CASE WHEN s = 'Close relative detained / died / missing / injured' THEN 1 ELSE 0 END) AS stressor_relative_detained_died,
        MAX(CASE WHEN s = 'Close relative with medical disease' THEN 1 ELSE 0 END) AS stressor_relative_medical,
        MAX(CASE WHEN s = 'Loss or excessive social role' THEN 1 ELSE 0 END) AS stressor_loss_social_role,
        MAX(CASE WHEN s = 'Victim of neglect' THEN 1 ELSE 0 END) AS stressor_neglect,
        MAX(CASE WHEN s = 'Isolated / Social exclusion' THEN 1 ELSE 0 END) AS stressor_social_exclusion,
        MAX(CASE WHEN s = 'Direct witness of violence' THEN 1 ELSE 0 END) AS stressor_witness_violence,
        MAX(CASE WHEN s = 'Direct victim of violence' THEN 1 ELSE 0 END) AS stressor_victim_violence,
        MAX(CASE WHEN s = 'Survivor of sexual violence' THEN 1 ELSE 0 END) AS stressor_sexual_violence,
        MAX(CASE WHEN s = 'Detained' THEN 1 ELSE 0 END) AS stressor_detained,
        MAX(CASE WHEN s = 'Displaced due to conflict' THEN 1 ELSE 0 END) AS stressor_displaced,
        MAX(CASE WHEN s = 'Living in targeted village' THEN 1 ELSE 0 END) AS stressor_targeted_village,
        MAX(CASE WHEN s = 'Domestic violence' THEN 1 ELSE 0 END) AS stressor_domestic_violence,
        MAX(CASE WHEN s = 'None' THEN 1 ELSE 0 END) AS stressor_none
  FROM last_stay ls
  JOIN mental_health_intake mhi
    ON mhi.encounter_id = ls.intake_encounter_id

  CROSS JOIN LATERAL (
    VALUES
      (mhi.stressor_1),
      (mhi.stressor_2),
      (mhi.stressor_3)
  ) AS v(s)

  GROUP BY ls.patient_id, ls.intake_date
),
/* ===================== STRESSORS SUMMARY ===================== */
stressors_summary AS (
  SELECT
    patient_id,
    intake_date,
    COUNT(*) AS stressors_total,
    STRING_AGG(stressor_name, ', ' ORDER BY stressor_name) AS stressors_list
  FROM (
    SELECT patient_id, intake_date, 'Non-conflict-related medical condition' AS stressor_name FROM stressors WHERE stressor_non_conflict_medical = 1 UNION ALL
    SELECT patient_id, intake_date, 'Conflict-related medical condition' FROM stressors WHERE stressor_conflict_medical = 1 UNION ALL
    SELECT patient_id, intake_date, 'Pre-existing mental health disorder' FROM stressors WHERE stressor_pre_existing_mhd = 1 UNION ALL
    SELECT patient_id, intake_date, 'Extreme poverty / Financial crisis' FROM stressors WHERE stressor_extreme_poverty = 1 UNION ALL
    SELECT patient_id, intake_date, 'Hard living due to conflict' FROM stressors WHERE stressor_hard_living_conflict = 1 UNION ALL
    SELECT patient_id, intake_date, 'House / property destroyed' FROM stressors WHERE stressor_house_destroyed = 1 UNION ALL
    SELECT patient_id, intake_date, 'Intra-family related problem' FROM stressors WHERE stressor_intrafamily_problem = 1 UNION ALL
    SELECT patient_id, intake_date, 'Close relative detained / died / missing / injured' FROM stressors WHERE stressor_relative_detained_died = 1 UNION ALL
    SELECT patient_id, intake_date, 'Close relative with medical disease' FROM stressors WHERE stressor_relative_medical = 1 UNION ALL
    SELECT patient_id, intake_date, 'Loss or excessive social role' FROM stressors WHERE stressor_loss_social_role = 1 UNION ALL
    SELECT patient_id, intake_date, 'Victim of neglect' FROM stressors WHERE stressor_neglect = 1 UNION ALL
    SELECT patient_id, intake_date, 'Isolated / Social exclusion' FROM stressors WHERE stressor_social_exclusion = 1 UNION ALL
    SELECT patient_id, intake_date, 'Direct witness of violence' FROM stressors WHERE stressor_witness_violence = 1 UNION ALL
    SELECT patient_id, intake_date, 'Direct victim of violence' FROM stressors WHERE stressor_victim_violence = 1 UNION ALL
    SELECT patient_id, intake_date, 'Survivor of sexual violence' FROM stressors WHERE stressor_sexual_violence = 1 UNION ALL
    SELECT patient_id, intake_date, 'Detained' FROM stressors WHERE stressor_detained = 1 UNION ALL
    SELECT patient_id, intake_date, 'Displaced due to conflict' FROM stressors WHERE stressor_displaced = 1 UNION ALL
    SELECT patient_id, intake_date, 'Living in targeted village' FROM stressors WHERE stressor_targeted_village = 1 UNION ALL
    SELECT patient_id, intake_date, 'Domestic violence' FROM stressors WHERE stressor_domestic_violence = 1
  ) x
  GROUP BY patient_id, intake_date
),

/* ===================== SYNDROMES ===================== */
latest_assessment AS (
  SELECT DISTINCT ON (ls.patient_id, ls.intake_date)
    ls.patient_id,
    ls.intake_date,
    f.main_syndrome,
    f.additional_syndrome
  FROM last_stay ls
  JOIN psy_counselors_initial_assessment f
    ON f.patient_id = ls.patient_id
   AND f."date"::date >= ls.intake_date
   AND (ls.discharge_date IS NULL OR f."date"::date <= ls.discharge_date)
  ORDER BY ls.patient_id, ls.intake_date, f."date" DESC
),

syndromes AS (
  SELECT
    la.patient_id,
    la.intake_date,

    MAX(CASE WHEN la.main_syndrome = 'Depression' OR la.additional_syndrome = 'Depression' THEN 1 ELSE 0 END) AS syndrome_depression,
    MAX(CASE WHEN la.main_syndrome = 'Anxiety disorder' OR la.additional_syndrome = 'Anxiety disorder' THEN 1 ELSE 0 END) AS syndrome_anxiety,
    MAX(CASE WHEN la.main_syndrome = 'Trauma related symptoms' OR la.additional_syndrome = 'Trauma related symptoms' THEN 1 ELSE 0 END) AS syndrome_trauma,
    MAX(CASE WHEN la.main_syndrome = 'Adult behavioral / substance problem' OR la.additional_syndrome = 'Adult behavioral / substance problem' THEN 1 ELSE 0 END) AS syndrome_substance,
    MAX(CASE WHEN la.main_syndrome = 'Child behavioral problem' OR la.additional_syndrome = 'Child behavioral problem' THEN 1 ELSE 0 END) AS syndrome_child_behavior,
    MAX(CASE WHEN la.main_syndrome = 'Psychosis' OR la.additional_syndrome = 'Psychosis' THEN 1 ELSE 0 END) AS syndrome_psychosis,
    MAX(CASE WHEN la.main_syndrome = 'Psychosomatic problems' OR la.additional_syndrome = 'Psychosomatic problems' THEN 1 ELSE 0 END) AS syndrome_psychosomatic,
    MAX(CASE WHEN la.main_syndrome = 'Neurocognitive problem' OR la.additional_syndrome = 'Neurocognitive problem' THEN 1 ELSE 0 END) AS syndrome_neurocognitive,
    MAX(CASE WHEN la.main_syndrome = 'Epilepsy' OR la.additional_syndrome = 'Epilepsy' THEN 1 ELSE 0 END) AS syndrome_epilepsy,
    MAX(CASE WHEN la.main_syndrome = 'Other' OR la.additional_syndrome = 'Other' THEN 1 ELSE 0 END) AS syndrome_other

  FROM latest_assessment la
  GROUP BY la.patient_id, la.intake_date
),
/* ===================== SYNDROMES SUMMARY ===================== */
/* ===================== SYNDROMES SUMMARY ===================== */
syndromes_summary AS (
  SELECT
    patient_id,
    intake_date,
    COUNT(*) AS syndromes_total,
    STRING_AGG(syndrome_name, ', ' ORDER BY syndrome_name) AS syndromes_list
  FROM (
    SELECT patient_id, intake_date, 'Depression' AS syndrome_name
    FROM syndromes WHERE syndrome_depression = 1

    UNION ALL
    SELECT patient_id, intake_date, 'Anxiety disorder'
    FROM syndromes WHERE syndrome_anxiety = 1

    UNION ALL
    SELECT patient_id, intake_date, 'Trauma related symptoms'
    FROM syndromes WHERE syndrome_trauma = 1

    UNION ALL
    SELECT patient_id, intake_date, 'Adult behavioral / substance problem'
    FROM syndromes WHERE syndrome_substance = 1

    UNION ALL
    SELECT patient_id, intake_date, 'Child behavioral problem'
    FROM syndromes WHERE syndrome_child_behavior = 1

    UNION ALL
    SELECT patient_id, intake_date, 'Psychosis'
    FROM syndromes WHERE syndrome_psychosis = 1

    UNION ALL
    SELECT patient_id, intake_date, 'Psychosomatic problems'
    FROM syndromes WHERE syndrome_psychosomatic = 1

    UNION ALL
    SELECT patient_id, intake_date, 'Neurocognitive problem'
    FROM syndromes WHERE syndrome_neurocognitive = 1

    UNION ALL
    SELECT patient_id, intake_date, 'Epilepsy'
    FROM syndromes WHERE syndrome_epilepsy = 1

    UNION ALL
    SELECT patient_id, intake_date, 'Other'
    FROM syndromes WHERE syndrome_other = 1
  ) x
  GROUP BY patient_id, intake_date
),
/* ===================== DIAGNOSES ===================== */
diagnoses_events AS (
  SELECT
    ls.patient_id,
    ls.intake_encounter_id,
    dx.date::date AS diagnosis_date,
    v.diagnosis
  FROM last_stay ls
  JOIN (
    SELECT patient_id, date, main_diagnosis, secondary_diagnosis
    FROM psychiatrist_mhgap_initial_assessment
    UNION ALL
    SELECT patient_id, date, main_diagnosis, secondary_diagnosis
    FROM psychiatrist_mhgap_follow_up
  ) dx
    ON dx.patient_id = ls.patient_id
   AND dx.date::date >= ls.intake_date
   AND (ls.discharge_date IS NULL OR dx.date::date <= ls.discharge_date)

  CROSS JOIN LATERAL (
    VALUES
      (dx.main_diagnosis),
      (dx.secondary_diagnosis)
  ) v(diagnosis)

  WHERE v.diagnosis IS NOT NULL
),

/* ===================== DIAGNOSES SUMMARY ===================== */
diagnoses_summary AS (
  SELECT
    patient_id,
    intake_encounter_id,
    COUNT(DISTINCT diagnosis) AS diagnoses_total,
    STRING_AGG(DISTINCT diagnosis, ', ' ORDER BY diagnosis) AS diagnoses_list
  FROM diagnoses_events
  GROUP BY patient_id, intake_encounter_id
),

latest_diagnosis AS (
  SELECT DISTINCT ON (patient_id, intake_encounter_id)
    patient_id,
    intake_encounter_id,
    diagnosis
  FROM diagnoses_events
  ORDER BY patient_id, intake_encounter_id, diagnosis_date DESC
),


diagnoses AS (
  SELECT
    ld.patient_id,
    ld.intake_encounter_id,

    MAX(CASE WHEN ld.diagnosis = 'Acute and transient psychotic disorder' THEN 1 END) AS "diagnosis: acute and transient psychotic disorder",
    MAX(CASE WHEN ld.diagnosis = 'Acute stress reaction' THEN 1 END) AS "diagnosis: acute stress reaction",
    MAX(CASE WHEN ld.diagnosis = 'Adjustment disorders' THEN 1 END) AS "diagnosis: adjustment disorders",
    MAX(CASE WHEN ld.diagnosis = 'Anxiety disorder' THEN 1 END) AS "diagnosis: anxiety disorder",
    MAX(CASE WHEN ld.diagnosis = 'Bipolar disorder' THEN 1 END) AS "diagnosis: bipolar disorder",
    MAX(CASE WHEN ld.diagnosis = 'Childhood emotional disorder' THEN 1 END) AS "diagnosis: childhood emotional disorder",
    MAX(CASE WHEN ld.diagnosis = 'Conduct disorders' THEN 1 END) AS "diagnosis: conduct disorders",
    MAX(CASE WHEN ld.diagnosis = 'Delirium' THEN 1 END) AS "diagnosis: delirium",
    MAX(CASE WHEN ld.diagnosis = 'Dementia' THEN 1 END) AS "diagnosis: dementia",
    MAX(CASE WHEN ld.diagnosis = 'Dissociative and conversion disorder' THEN 1 END) AS "diagnosis: dissociative and conversion disorder",
    MAX(CASE WHEN ld.diagnosis = 'Dissociative convulsions' THEN 1 END) AS "diagnosis: dissociative convulsions",
    MAX(CASE WHEN ld.diagnosis = 'Hyperkinetic disorder' THEN 1 END) AS "diagnosis: hyperkinetic disorder",
    MAX(CASE WHEN ld.diagnosis = 'Intellectual disability' THEN 1 END) AS "diagnosis: intellectual disability",
    MAX(CASE WHEN ld.diagnosis = 'Mental or behavioural disorders due to multiple drug use or other psychoactive substances' THEN 1 END) AS "diagnosis: disorders due to drugs",
    MAX(CASE WHEN ld.diagnosis = 'Mental or behavioural disorders due to use of alcohol' THEN 1 END) AS "diagnosis: disorders due to alcohol",
    MAX(CASE WHEN ld.diagnosis = 'Mild depressive episode' THEN 1 END) AS "diagnosis: mild depressive episode",
    MAX(CASE WHEN ld.diagnosis = 'Moderate depressive episode' THEN 1 END) AS "diagnosis: moderate depressive episode",
    MAX(CASE WHEN ld.diagnosis = 'Nonorganic enuresis' THEN 1 END) AS "diagnosis: nonorganic enuresis",
    MAX(CASE WHEN ld.diagnosis = 'Obsessive-compulsive disorder' THEN 1 END) AS "diagnosis: obsessive compulsive disorder",
    MAX(CASE WHEN ld.diagnosis = 'Panic disorder' THEN 1 END) AS "diagnosis: panic disorder",
    MAX(CASE WHEN ld.diagnosis = 'Pervasive developmental disorder' THEN 1 END) AS "diagnosis: pervasive developmental disorder",
    MAX(CASE WHEN ld.diagnosis = 'Post-partum depression' THEN 1 END) AS "diagnosis: post-partum depression",
    MAX(CASE WHEN ld.diagnosis = 'Post-partum psychosis' THEN 1 END) AS "diagnosis: post-partum psychosis",
    MAX(CASE WHEN ld.diagnosis = 'Post Traumatic Stress Disorder' THEN 1 END) AS "diagnosis: post traumatic stress disorder",
    MAX(CASE WHEN ld.diagnosis = 'Schizophrenia' THEN 1 END) AS "diagnosis: schizophrenia",
    MAX(CASE WHEN ld.diagnosis = 'Severe depressive episode with psychotic symptoms' THEN 1 END) AS "diagnosis: severe depressive episode with psychotic symptoms",
    MAX(CASE WHEN ld.diagnosis = 'Severe depressive episode without psychotic symptoms' THEN 1 END) AS "diagnosis: severe depressive episode without psychotic symptoms",
    MAX(CASE WHEN ld.diagnosis = 'Somatoform disorders' THEN 1 END) AS "diagnosis: somatoform disorders",
    MAX(CASE WHEN ld.diagnosis = 'Other' THEN 1 END) AS "diagnosis: other"

  FROM latest_diagnosis ld
  GROUP BY ld.patient_id, ld.intake_encounter_id
)


/* ===================== FINAL ===================== */
SELECT
  p.patient_id,
  p.patient_identifier,
  pa."Patient_code",
  p.registration_date,
	pdd.age AS age_current,
	CASE 
		WHEN pdd.age::int <= 3 THEN '0-3'
		WHEN pdd.age::int >= 4 AND pdd.age::int <= 7 THEN '04-07'
		WHEN pdd.age::int >= 8 AND pdd.age::int <= 14 THEN '08-14'
		WHEN pdd.age::int >= 15 AND pdd.age::int <= 17 THEN '15-17'
		WHEN pdd.age::int >= 18 THEN '18+'
		ELSE NULL
	END AS age_group_current,
	pdd.gender,
	pad.state_province AS governorate, 
	pad.city_village AS community_village,
	pa."Legal_status",
	pa."Civil_status",
	pa."Education_level",
	pa."Occupation",
	pa."Personal_Situation",
	pa."Living_conditions",

CASE
  WHEN ls.discharge_date IS NOT NULL THEN '5. Discharged'
  WHEN ls.intake_date IS NULL THEN '1. Registration'
  WHEN ss.first_assessment_date IS NULL THEN '0. ERROR'
  WHEN ss.second_follow_up_date IS NOT NULL THEN '4. Active cohort'
  WHEN ss.first_follow_up_date IS NOT NULL THEN '3. Waiting list'
  WHEN ss.first_assessment_date IS NOT NULL THEN '2. First contact'
  ELSE '0. ERROR'
END AS cohort,
ls.readmission,

  ls.intake_date,
  ls.visit_location_intake,
  ls.source_of_initial_patient_referral,

  ss.first_assessment_date,
  ct1.consult_type AS first_assessment_type,
 

  ss.first_follow_up_date,
  ct2.consult_type AS first_follow_up_type,

  ss.second_follow_up_date,
  ct3.consult_type AS second_follow_type,
  
  ls.discharge_date,
  ls.discharge_status,
        ls.cgi_s_discharge,
      ls.cgi_i_discharge,
  

ss.last_consultation_date,
ct_last.consult_type AS last_consultation_type,
  a.last_appointment_date,
  a.appointment_status,
  (CURRENT_DATE - a.last_appointment_date) AS days_since_last_appointment,

  pc.psychiatrist_consults,
  pc.psychologist_consults,

  psy.psychotropic_prescription,

/* STRESSORS */
st.stressor_non_conflict_medical,
st.stressor_conflict_medical,
st.stressor_pre_existing_mhd,
st.stressor_extreme_poverty,
st.stressor_hard_living_conflict,
st.stressor_house_destroyed,
st.stressor_intrafamily_problem,
st.stressor_relative_detained_died,
st.stressor_relative_medical,
st.stressor_loss_social_role,
st.stressor_neglect,
st.stressor_social_exclusion,
st.stressor_witness_violence,
st.stressor_victim_violence,
st.stressor_sexual_violence,
st.stressor_detained,
st.stressor_displaced,
st.stressor_targeted_village,
st.stressor_domestic_violence,
st.stressor_none,

/* STRESSORS SUMMARY */
sts.stressors_total,
sts.stressors_list,

/* SYNDROMES */
sy.syndrome_depression,
sy.syndrome_anxiety,
sy.syndrome_trauma,
sy.syndrome_substance,
sy.syndrome_child_behavior,
sy.syndrome_psychosis,
sy.syndrome_psychosomatic,
sy.syndrome_neurocognitive,
sy.syndrome_epilepsy,
sy.syndrome_other,

/* SYNDROMES SUMMARY */
sys.syndromes_total,
sys.syndromes_list,

/* DIAGNOSES */
d."diagnosis: acute and transient psychotic disorder",
d."diagnosis: acute stress reaction",
d."diagnosis: adjustment disorders",
d."diagnosis: anxiety disorder",
d."diagnosis: bipolar disorder",
d."diagnosis: childhood emotional disorder",
d."diagnosis: conduct disorders",
d."diagnosis: delirium",
d."diagnosis: dementia",
d."diagnosis: dissociative and conversion disorder",
d."diagnosis: dissociative convulsions",
d."diagnosis: hyperkinetic disorder",
d."diagnosis: intellectual disability",
d."diagnosis: disorders due to drugs",
d."diagnosis: disorders due to alcohol",
d."diagnosis: mild depressive episode",
d."diagnosis: moderate depressive episode",
d."diagnosis: nonorganic enuresis",
d."diagnosis: obsessive compulsive disorder",
d."diagnosis: panic disorder",
d."diagnosis: pervasive developmental disorder",
d."diagnosis: post-partum depression",
d."diagnosis: post-partum psychosis",
d."diagnosis: post traumatic stress disorder",
d."diagnosis: schizophrenia",
d."diagnosis: severe depressive episode with psychotic symptoms",
d."diagnosis: severe depressive episode without psychotic symptoms",
d."diagnosis: somatoform disorders",
d."diagnosis: other",

/* DIAGNOSES SUMMARY */
dys.diagnoses_total,
dys.diagnoses_list


FROM patients p
LEFT OUTER JOIN person_details_default pdd 	ON pdd.person_id = p.patient_id
LEFT OUTER JOIN person_address_default pad
	ON p.patient_id = pad.person_id
LEFT JOIN last_stay ls ON ls.patient_id = p.patient_id
LEFT JOIN stay_stats ss ON ss.patient_id = ls.patient_id AND ss.intake_date = ls.intake_date
LEFT JOIN provider_counts pc ON pc.patient_id = ls.patient_id AND pc.intake_date = ls.intake_date
LEFT JOIN consult_types ct1
  ON ct1.patient_id = ls.patient_id
 AND ct1.intake_date = ls.intake_date
 AND ct1.form_date = ss.first_assessment_date
LEFT JOIN consult_types ct2
  ON ct2.patient_id = ls.patient_id
 AND ct2.intake_date = ls.intake_date
 AND ct2.form_date = ss.first_follow_up_date
LEFT JOIN consult_types ct3
  ON ct3.patient_id = ls.patient_id
 AND ct3.intake_date = ls.intake_date
 AND ct3.form_date = ss.second_follow_up_date
 LEFT JOIN consult_types ct_last
  ON ct_last.patient_id = ls.patient_id
 AND ct_last.intake_date = ls.intake_date
 AND ct_last.form_date = ss.last_consultation_date
LEFT JOIN appointments a ON a.patient_id = p.patient_id
LEFT JOIN person_attributes pa ON pa.person_id = p.patient_id
LEFT JOIN psychotropic psy ON psy.intake_encounter_id = ls.intake_encounter_id
LEFT JOIN stressors st ON st.patient_id = ls.patient_id AND st.intake_date = ls.intake_date
LEFT JOIN syndromes sy ON sy.patient_id = ls.patient_id AND sy.intake_date = ls.intake_date
LEFT JOIN diagnoses d ON d.patient_id = ls.patient_id AND d.intake_encounter_id = ls.intake_encounter_id
LEFT JOIN stressors_summary sts
  ON sts.patient_id = ls.patient_id
 AND sts.intake_date = ls.intake_date

LEFT JOIN syndromes_summary sys
  ON sys.patient_id = ls.patient_id
 AND sys.intake_date = ls.intake_date
 
 
LEFT JOIN diagnoses_summary dys
  ON dys.patient_id = ls.patient_id
 AND dys.intake_encounter_id = ls.intake_encounter_id;
