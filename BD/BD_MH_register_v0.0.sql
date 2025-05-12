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
	SELECT DISTINCT ON (c.patient_id, c.intake_encounter_id, c.intake_date, c.discharge_date) c.intake_encounter_id, pcia.date::date, pcia.visit_location
	FROM cohort c
	LEFT OUTER JOIN psy_counselors_initial_assessment pcia
		ON c.patient_id = pcia.patient_id
	WHERE (pcia.date::date >= c.intake_date) AND (pcia.date::date <= c.discharge_date OR c.discharge_date IS NULL)
	GROUP BY c.patient_id, c.intake_encounter_id, c.intake_date, c.discharge_date, pcia.date, pcia.visit_location
	ORDER BY c.patient_id, c.intake_encounter_id, c.intake_date, c.discharge_date, pcia.date ASC),
-- The first clinician initial assessment CTE extracts the date from the first clinician initial assesment. If multiple initial assessments are completed per cohort enrollment then the first is used.
first_clinician_initial_assessment AS (
	SELECT DISTINCT ON (c.patient_id, c.intake_encounter_id, c.intake_date, c.discharge_date) c.intake_encounter_id, pmia.date::date, pmia.visit_location
	FROM cohort c
	LEFT OUTER JOIN psychiatrist_mhgap_initial_assessment pmia 
		ON c.patient_id = pmia.patient_id
	WHERE (pmia.date::date >= c.intake_date) AND (pmia.date::date <= c.discharge_date OR c.discharge_date IS NULL)
	GROUP BY c.patient_id, c.intake_encounter_id, c.intake_date, c.discharge_date, pmia.date, pmia.visit_location
	ORDER BY c.patient_id, c.intake_encounter_id, c.intake_date, c.discharge_date, pmia.date ASC),
-- The Syndrome CTEs pivot syndrome data horizontally from the MH counselor initial assessment form. If more than one form is filled per cohort enrollment than the data from the last form is reported. 
syndrome_pivot AS (
	SELECT 
		DISTINCT ON (pcia.patient_id, pcia.date) pcia.patient_id, 
		pcia.date::date,
		MAX (CASE WHEN pcia.main_syndrome IS NOT NULL THEN pcia.main_syndrome ELSE NULL END) AS main_syndrome,	
		MAX (CASE WHEN pcia.additional_syndrome IS NOT NULL THEN pcia.additional_syndrome ELSE NULL END) AS additional_syndrome,
		MAX (CASE WHEN pcia.main_syndrome = 'Depression' OR pcia.additional_syndrome = 'Depression'  THEN 1 ELSE NULL END) AS depression,	
		MAX (CASE WHEN pcia.main_syndrome = 'Anxiety disorder' OR pcia.additional_syndrome = 'Anxiety disorder'  THEN 1 ELSE NULL END) AS anxiety_disorder,
		MAX (CASE WHEN pcia.main_syndrome = 'Trauma related symptoms' OR pcia.additional_syndrome = 'Trauma related symptoms'  THEN 1 ELSE NULL END) AS trauma_related_symptoms,	
		MAX (CASE WHEN pcia.main_syndrome = 'Adult behavioral / substance problem' OR pcia.additional_syndrome = 'Adult behavioral / substance problem'  THEN 1 ELSE NULL END) AS adult_behavioral_substance_problem,	
		MAX (CASE WHEN pcia.main_syndrome = 'Child behavioral problem' OR pcia.additional_syndrome = 'Child behavioral problem'  THEN 1 ELSE NULL END) AS child_behavioral_problem,	
		MAX (CASE WHEN pcia.main_syndrome = 'Psychosis' OR pcia.additional_syndrome = 'Psychosis'  THEN 1 ELSE NULL END) AS psychosis,	
		MAX (CASE WHEN pcia.main_syndrome = 'Psychosomatic problems' OR pcia.additional_syndrome = 'Psychosomatic problems'  THEN 1 ELSE NULL END) AS psychosomatic_problems,	
		MAX (CASE WHEN pcia.main_syndrome = 'Neurocognitive problem' OR pcia.additional_syndrome = 'Neurocognitive problem'  THEN 1 ELSE NULL END) AS neurocognitive_problem,	
		MAX (CASE WHEN pcia.main_syndrome = 'Epilepsy' OR pcia.additional_syndrome = 'Epilepsy'  THEN 1 ELSE NULL END) AS epilepsy,	
		MAX (CASE WHEN pcia.main_syndrome = 'Other' OR pcia.additional_syndrome = 'Other'  THEN 1 ELSE NULL END) AS other_syndrome
	FROM psy_counselors_initial_assessment pcia
	GROUP BY pcia.patient_id, pcia.date),
last_syndrome AS (
	SELECT 
		DISTINCT ON (c.patient_id, c.intake_encounter_id) c.patient_id,
		c.intake_encounter_id,
		c.intake_date, 
		c.discharge_encounter_id,
		c.discharge_date, 
		sp.date,
		sp.main_syndrome,
		sp.additional_syndrome,
		sp.depression,	
		sp.anxiety_disorder,
		sp.trauma_related_symptoms,	
		sp.adult_behavioral_substance_problem,	
		sp.child_behavioral_problem,	
		sp.psychosis,	
		sp.psychosomatic_problems,	
		sp.neurocognitive_problem,	
		sp.epilepsy,	
		sp.other_syndrome
	FROM cohort c
	LEFT OUTER JOIN syndrome_pivot sp
		ON c.patient_id = sp.patient_id AND c.intake_date <= sp.date AND CASE WHEN c.discharge_date IS NOT NULL THEN c.discharge_date ELSE current_date END >= sp.date
	GROUP BY c.patient_id, c.intake_encounter_id, c.intake_date, c.discharge_encounter_id, c.discharge_date, sp.date, sp.main_syndrome, sp.additional_syndrome,sp.depression, sp.anxiety_disorder, sp.trauma_related_symptoms, sp.adult_behavioral_substance_problem, sp.child_behavioral_problem, sp.psychosis, sp.psychosomatic_problems, sp.neurocognitive_problem, sp.epilepsy, sp.other_syndrome
	ORDER BY c.patient_id, c.intake_encounter_id, c.intake_date, sp.date DESC),
-- The initial CGI-Severity score CTE reports only one initial CGI-Severity score per cohort enrollment. The score from the first initial assessment is reported.
initial_cgis AS (
	SELECT
		DISTINCT ON (c.patient_id, c.intake_encounter_id, c.intake_date, c.discharge_date) c.intake_encounter_id,
		cgis_initial AS cgi_s_score_at_initial_assessment	
	FROM cohort c
	LEFT OUTER JOIN (SELECT date, patient_id, cgi_s_score AS cgis_initial FROM psychiatrist_mhgap_initial_assessment UNION SELECT date, patient_id, clinical_global_impression_severity_score_coded AS cgis_initial FROM psy_counselors_initial_assessment) ia
		ON c.patient_id = ia.patient_id
	WHERE ia.date::date >= c.intake_date AND (ia.date::date <= c.discharge_date OR c.discharge_date IS NULL)
	ORDER BY c.patient_id, c.intake_encounter_id, c.intake_date, c.discharge_date, ia.date::date ASC),
-- The Mental Health diagnosis CTE pivots mental health diagnosis data horizontally from the Psychiatrist mhGap initial and follow-up forms. Only the last diagnoses reported per cohort enrollment are present. 
last_mh_dx AS (
	SELECT 
		DISTINCT ON (c.patient_id, c.intake_encounter_id) c.patient_id,
		c.intake_encounter_id,
		c.intake_date, 
		c.discharge_encounter_id,
		c.discharge_date,
		mmhd.date,
		mmhd.diagnosis
	FROM cohort c
	LEFT OUTER JOIN (
		SELECT patient_id, date::date, main_diagnosis AS diagnosis, 'main_diagnosis' AS category FROM psychiatrist_mhgap_initial_assessment
		UNION
		SELECT patient_id, date::date, main_diagnosis AS diagnosis, 'main_diagnosis' AS category FROM psychiatrist_mhgap_follow_up
		UNION
		SELECT patient_id, date::date, secondary_diagnosis AS diagnosis, 'secondary_diagnosis' AS category FROM psychiatrist_mhgap_initial_assessment
		UNION
		SELECT patient_id, date::date, secondary_diagnosis AS diagnosis, 'secondary_diagnosis' AS category FROM psychiatrist_mhgap_follow_up) mmhd
		ON c.patient_id = mmhd.patient_id AND c.intake_date <= mmhd.date::date AND CASE WHEN c.discharge_date IS NOT NULL THEN c.discharge_date ELSE current_date END >= mmhd.date::date
	WHERE mmhd.diagnosis IS NOT NULL 
	GROUP BY c.patient_id, c.intake_encounter_id, c.intake_date, c.discharge_encounter_id, c.discharge_date, mmhd.date::date, mmhd.diagnosis, mmhd.category
	ORDER BY c.patient_id, c.intake_encounter_id, c.intake_date, mmhd.date::date DESC),
last_mh_diagnosis AS (
	SELECT 
		DISTINCT ON (mhdu.patient_id, intake_encounter_id) mhdu.patient_id, 
		intake_encounter_id,
		MAX (CASE WHEN mhdu.diagnosis = 'Acute and transient psychotic disorder' THEN 1 ELSE NULL END) AS acute_transient_psychotic_disorder,	
		MAX (CASE WHEN mhdu.diagnosis = 'Acute stress reaction' THEN 1 ELSE NULL END) AS acute_stress_reaction,	
		MAX (CASE WHEN mhdu.diagnosis = 'Adjustment disorders' THEN 1 ELSE NULL END) AS adjustment_disorders,	
		MAX (CASE WHEN mhdu.diagnosis = 'Anxiety disorder' THEN 1 ELSE NULL END) AS anxiety_disorder,
		MAX (CASE WHEN mhdu.diagnosis = 'Bipolar disorder' THEN 1 ELSE NULL END) AS bipolar_disorder,	
	 	MAX (CASE WHEN mhdu.diagnosis = 'Childhood emotional disorder' THEN 1 ELSE NULL END) AS childhood_emotional_disorder,
		MAX (CASE WHEN mhdu.diagnosis = 'Conduct disorders' THEN 1 ELSE NULL END) AS conduct_disorders,
		MAX (CASE WHEN mhdu.diagnosis = 'Delirium' THEN 1 ELSE NULL END) AS delirium,	
		MAX (CASE WHEN mhdu.diagnosis = 'Dementia' THEN 1 ELSE NULL END) AS dementia,	
		MAX (CASE WHEN mhdu.diagnosis = 'Dissociative and conversion disorder' THEN 1 ELSE NULL END) AS dissociative_conversion_disorder,
		MAX (CASE WHEN mhdu.diagnosis = 'Dissociative convulsions' THEN 1 ELSE NULL END) AS dissociative_convulsions,
		MAX (CASE WHEN mhdu.diagnosis = 'Hyperkinetic disorder' THEN 1 ELSE NULL END) AS hyperkinetic_disorder,
		MAX (CASE WHEN mhdu.diagnosis = 'Intellectual disability' THEN 1 ELSE NULL END) AS intellectual_disability,
		MAX (CASE WHEN mhdu.diagnosis = 'Mental or behavioural disorders due to multiple drug use or other psychoactive substances' THEN 1 ELSE NULL END) AS disorders_due_drug_psychoactive_substances,
		MAX (CASE WHEN mhdu.diagnosis = 'Mental or behavioural disorders due to use of alcohol' THEN 1 ELSE NULL END) AS disorders_due_alcohol,
		MAX (CASE WHEN mhdu.diagnosis = 'Mild depressive episode' THEN 1 ELSE NULL END) AS mild_depressive_episode,
		MAX (CASE WHEN mhdu.diagnosis = 'Moderate depressive episode' THEN 1 ELSE NULL END) AS moderate_depressive_episode,
		MAX (CASE WHEN mhdu.diagnosis = 'Nonorganic enuresis' THEN 1 ELSE NULL END) AS nonorganic_enuresis,
		MAX (CASE WHEN mhdu.diagnosis = 'Obsessive-compulsive disorder' THEN 1 ELSE NULL END) AS obsessive_compulsive_disorder,
		MAX (CASE WHEN mhdu.diagnosis = 'Panic disorder' THEN 1 ELSE NULL END) AS panic_disorder,
		MAX (CASE WHEN mhdu.diagnosis = 'Pervasive developmental disorder' THEN 1 ELSE NULL END) AS pervasive_developmental_disorder,
		MAX (CASE WHEN mhdu.diagnosis = 'Post-partum depression' THEN 1 ELSE NULL END) AS postpartum_depression,
		MAX (CASE WHEN mhdu.diagnosis = 'Post-partum psychosis' THEN 1 ELSE NULL END) AS postpartum_psychosis,
		MAX (CASE WHEN mhdu.diagnosis = 'Post Traumatic Stress Disorder' THEN 1 ELSE NULL END) AS ptsd,
		MAX (CASE WHEN mhdu.diagnosis = 'Schizophrenia' THEN 1 ELSE NULL END) AS schizophrenia,
		MAX (CASE WHEN mhdu.diagnosis = 'Severe depressive episode with psychotic symptoms' THEN 1 ELSE NULL END) AS severe_depressive_episode_with_psychotic_symptoms,
		MAX (CASE WHEN mhdu.diagnosis = 'Severe depressive episode without psychotic symptoms' THEN 1 ELSE NULL END) AS severe_depressive_episode_without_psychotic_symptoms,
		MAX (CASE WHEN mhdu.diagnosis = 'Somatoform disorders' THEN 1 ELSE NULL END) AS somatoform_disorders,
		MAX (CASE WHEN mhdu.diagnosis = 'Other' THEN 1 ELSE NULL END) AS other_mh
	FROM last_mh_dx mhdu
	GROUP BY mhdu.patient_id, intake_encounter_id),	
mh_diagnosis_list AS (
	SELECT intake_encounter_id, STRING_AGG(diagnosis, ', ') AS diagnosis_list
	FROM last_mh_dx
	GROUP BY intake_encounter_id),
-- The counselor initial assessment CTE counts the number of initial counselor assessments that took place per cohort enrollment. 
counselor_ia AS (
	SELECT 
		DISTINCT ON (c.patient_id, c.intake_encounter_id, c.intake_date, c.discharge_date) c.intake_encounter_id,
		COUNT(*) AS counselor_initial_consultations
	FROM cohort c
	LEFT OUTER JOIN psy_counselors_initial_assessment pcia 
		ON c.patient_id = pcia.patient_id
	WHERE pcia.date::date >= c.intake_date AND (pcia.date::date <= c.discharge_date OR c.discharge_date IS NULL)
	GROUP BY c.patient_id, c.intake_encounter_id, c.intake_date, c.discharge_date),
-- The counselor follow-up CTE counts the number of individual follow up counselor sessions that took place per cohort enrollment. 
counselor_fu_individual AS (
	SELECT 
		DISTINCT ON (c.patient_id, c.intake_encounter_id, c.intake_date, c.discharge_date) c.intake_encounter_id,
		COUNT(*) AS counselor_fu_individual_sessions
	FROM cohort c
	LEFT OUTER JOIN psy_counselors_follow_up pcfu 
		ON c.patient_id = pcfu.patient_id
	WHERE pcfu.date::date >= c.intake_date AND (pcfu.date::date <= c.discharge_date OR c.discharge_date IS NULL) AND pcfu.type_of_activity = 'Individual session'
	GROUP BY c.patient_id, c.intake_encounter_id, c.intake_date, c.discharge_date),
-- The counselor follow-up CTE counts the number of other follow up counselor sessions that were of type light contact or conducted by a CHW per cohort enrollment.
counselor_fu_light_contact_chw AS (
	SELECT 
		DISTINCT ON (c.patient_id, c.intake_encounter_id, c.intake_date, c.discharge_date) c.intake_encounter_id,
		COUNT(*) AS counselor_fu_light_contact_chw_sessions
	FROM cohort c
	LEFT OUTER JOIN psy_counselors_follow_up pcfu 
		ON c.patient_id = pcfu.patient_id
	WHERE pcfu.date::date >= c.intake_date AND (pcfu.date::date <= c.discharge_date OR c.discharge_date IS NULL) AND (pcfu.type_of_activity = 'Light contact' OR pcfu.provider_type = 'CHW') AND pcfu.type_of_activity != 'Missed appointment'
	GROUP BY c.patient_id, c.intake_encounter_id, c.intake_date, c.discharge_date),
-- The counselor follow-up CTE counts the number of other follow up counselor sessions that took place per cohort enrollment, excludes individual and missed sessions. 
counselor_fu_other AS (
	SELECT 
		DISTINCT ON (c.patient_id, c.intake_encounter_id, c.intake_date, c.discharge_date) c.intake_encounter_id,
		COUNT(*) AS counselor_fu_other_sessions
	FROM cohort c
	LEFT OUTER JOIN psy_counselors_follow_up pcfu 
		ON c.patient_id = pcfu.patient_id
	WHERE pcfu.date::date >= c.intake_date AND (pcfu.date::date <= c.discharge_date OR c.discharge_date IS NULL) AND pcfu.type_of_activity NOT IN ('Individual session', 'Light contact', 'Missed appointment') AND pcfu.provider_type != 'CHW'
	GROUP BY c.patient_id, c.intake_encounter_id, c.intake_date, c.discharge_date),
-- The psychiatrist initial assessment CTE counts the number of initial counselor assessments that took place per cohort enrollment. 
psy_ia AS (
	SELECT 
		DISTINCT ON (c.patient_id, c.intake_encounter_id, c.intake_date, c.discharge_date) c.intake_encounter_id,
		COUNT(*) AS psychiatrist_initial_consultations
	FROM cohort c
	LEFT OUTER JOIN psychiatrist_mhgap_initial_assessment pmia 
		ON c.patient_id = pmia.patient_id
	WHERE pmia.date::date >= c.intake_date AND (pmia.date::date <= c.discharge_date OR c.discharge_date IS NULL)
	GROUP BY c.patient_id, c.intake_encounter_id, c.intake_date, c.discharge_date),
-- The psychiatrist follow-up CTE counts the number of individual follow up psychiatrist sessions that took place per cohort enrollment. 
psy_fu_individual AS (
	SELECT 
		DISTINCT ON (c.patient_id, c.intake_encounter_id, c.intake_date, c.discharge_date) c.intake_encounter_id,
		COUNT(*) AS psychiatrist_fu_individual_sessions
	FROM cohort c
	LEFT OUTER JOIN psychiatrist_mhgap_follow_up pmfu 
		ON c.patient_id = pmfu.patient_id
	WHERE pmfu.date::date >= c.intake_date AND (pmfu.date::date <= c.discharge_date OR c.discharge_date IS NULL) AND pmfu.type_of_activity = 'Individual session'
	GROUP BY c.patient_id, c.intake_encounter_id, c.intake_date, c.discharge_date),
-- The psychiatrist follow-up CTE counts the number of other follow up psychiatrist sessions that took place per cohort enrollment, excludes individual and missed sessions. 
psy_fu_light_contact_chw AS (
	SELECT 
		DISTINCT ON (c.patient_id, c.intake_encounter_id, c.intake_date, c.discharge_date) c.intake_encounter_id,
		COUNT(*) AS psychiatrist_fu_light_contact_chw_sessions
	FROM cohort c
	LEFT OUTER JOIN psychiatrist_mhgap_follow_up pmfu 
		ON c.patient_id = pmfu.patient_id
	WHERE pmfu.date::date >= c.intake_date AND (pmfu.date::date <= c.discharge_date OR c.discharge_date IS NULL) AND (pmfu.type_of_activity = 'Light contact' OR pmfu.provider_type = 'CHW') AND pmfu.type_of_activity != 'Missed appointment'
	GROUP BY c.patient_id, c.intake_encounter_id, c.intake_date, c.discharge_date),
-- The psychiatrist follow-up CTE counts the number of other follow up psychiatrist sessions that took place per cohort enrollment, excludes individual and missed sessions. 
psy_fu_other AS (
	SELECT 
		DISTINCT ON (c.patient_id, c.intake_encounter_id, c.intake_date, c.discharge_date) c.intake_encounter_id,
		COUNT(*) AS psychiatrist_fu_other_sessions
	FROM cohort c
	LEFT OUTER JOIN psychiatrist_mhgap_follow_up pmfu 
		ON c.patient_id = pmfu.patient_id
	WHERE pmfu.date::date >= c.intake_date AND (pmfu.date::date <= c.discharge_date OR c.discharge_date IS NULL) AND pmfu.type_of_activity NOT IN ('Individual session', 'Light contact', 'Missed appointment') AND pmfu.provider_type != 'CHW'
	GROUP BY c.patient_id, c.intake_encounter_id, c.intake_date, c.discharge_date),
-- The psychotropic prescription CTE identifies any patient who has had at least one psychotropic prescription while in the cohort. Both past and active prescriptions are considered.
psychotropic_prescription AS (
	SELECT
		DISTINCT ON (c.patient_id, c.intake_encounter_id, c.intake_date, c.discharge_date) c.intake_encounter_id,
		CASE 
			WHEN mdd.patient_id IS NOT NULL THEN 'Yes' 
			ELSE 'No'
		END AS psychotropic_prescription
	FROM cohort c
	LEFT OUTER JOIN medication_data_default mdd
		ON c.patient_id = mdd.patient_id
	WHERE mdd.start_date >= c.intake_date AND (mdd.start_date <= c.discharge_date OR c.discharge_date IS NULL) AND mdd.coded_drug_name IN ('AMITRIPTYLINE hydrochloride, 25 mg, tab.','BIPERIDEN hydrochloride, 2 mg, tab','CARBAMAZEPINE, 100 mg, tab.','CARBAMAZEPINE, 200 mg, tab.','CHLORPROMAZINE hydrochloride, eq. 100mg base, tab.','CHLORPROMAZINE hydrochloride, eq. 25mg base, tab.','DIAZEPAM, 5 mg, tab.','DIAZEPAM, 5 mg/ml, 2 ml, amp.','FLUOXETINE hydrochloride, eq. 20 mg base, caps.','FLUPHENAZINE decanoate, 25mg/ml, 1ml, amp.','HALOPERIDOL 0.5mg, tab.','HALOPERIDOL decanoate, 50mg/ml, 1ml, amp.','HALOPERIDOL, 2 mg/ml, oral sol., 100 ml, bot. with pipette','HALOPERIDOL, 5 mg, tab.','HALOPERIDOL, 5 mg/ml, 1 ml, amp.','HYDROXYZINE dihydrochloride, 25 mg, tab.','LEVETIRACETAM, 250 mg, tab.','LEVETIRACETAM, 500mg, tab.','MIRTAZAPINE, 15 mg, tab.','OLANZAPINE, 10 mg, tab.','OLANZAPINE, 2.5 mg, tab.','OLANZAPINE, 5mg, tab.','PAROXETINE, 20 mg, breakable tab.','PHENOBARBITAL, 30 mg, tab.','PHENOBARBITAL, 50 mg, tab.','PHENOBARBITAL, 60 mg, tab.','PHENYTOIN sodium, 100 mg, tab.','PHENYTOIN, 30 mg/5ml, oral susp., 500ml, bot.','PROMETHAZINE hydrochloride, eq. 25 mg base, tab.','PROMETHAZINE hydrochloride, eq. 25 mg/ml base, 1 ml, amp.','PROMETHAZINE hydrochloride, eq. 25 mg/ml base, 2 ml, amp.','RISPERIDONE, 1 mg, tab.','RISPERIDONE, 2 mg, tab.','SERTRALINE hydrochloride, eq. 100mg base, tab.','SERTRALINE hydrochloride, eq. 50mg base, tab.','TRIHEXYPHENIDYL hydrochloride, 2 mg, tab.','VALPROATE SODIUM, 200 mg, gastro-resistant tab.','VALPROATE SODIUM, 200mg/5ml, 300 ml, bot.','VALPROATE SODIUM, 500 mg, gastro-resistant tab.')),
-- The visit location CTE finds the last visit location reported across all clinical consultaiton/session forms.
last_visit_location AS (	
	SELECT 
		DISTINCT ON (c.patient_id, c.intake_encounter_id, c.intake_date, c.discharge_date) c.intake_encounter_id,
		vl.visit_location AS visit_location
	FROM cohort c
	LEFT OUTER JOIN (
		SELECT date::date, patient_id, visit_location FROM mental_health_intake WHERE visit_location IS NOT NULL
		UNION SELECT date::date, patient_id, visit_location FROM psy_counselors_initial_assessment WHERE visit_location IS NOT NULL 
		UNION SELECT date::date, patient_id, visit_location FROM psychiatrist_mhgap_initial_assessment WHERE visit_location IS NOT NULL 
		UNION SELECT date::date, patient_id, visit_location FROM psy_counselors_follow_up WHERE visit_location IS NOT NULL 
		UNION SELECT date::date, patient_id, visit_location FROM psychiatrist_mhgap_follow_up WHERE visit_location IS NOT NULL
		UNION SELECT discharge_date AS date, patient_id, visit_location FROM mental_health_discharge WHERE visit_location IS NOT NULL) vl
		ON c.patient_id = vl.patient_id
	WHERE vl.date >= c.intake_date AND (vl.date <= c.discharge_date OR c.discharge_date IS NULL)
	GROUP BY c.patient_id, c.intake_encounter_id, c.intake_date, c.discharge_date, vl.date, vl.visit_location
	ORDER BY c.patient_id, c.intake_encounter_id, c.intake_date, c.discharge_date, vl.date DESC),
-- The last visit date CTE finds the last visit reported across all clinical consultaiton/session forms.
last_visit_date AS (	
	SELECT 
		DISTINCT ON (c.patient_id, c.intake_encounter_id, c.intake_date, c.discharge_date) c.intake_encounter_id,
		vd.date AS last_visit_date,
		vd.form_field_path AS last_visit
	FROM cohort c
	LEFT OUTER JOIN (
		SELECT date::date, patient_id, form_field_path FROM mental_health_intake
		UNION SELECT date::date, patient_id, form_field_path FROM psy_counselors_initial_assessment 
		UNION SELECT date::date, patient_id, form_field_path FROM psychiatrist_mhgap_initial_assessment  
		UNION SELECT date::date, patient_id, form_field_path FROM psy_counselors_follow_up 
		UNION SELECT date::date, patient_id, form_field_path FROM psychiatrist_mhgap_follow_up 
		UNION SELECT discharge_date AS date, patient_id, form_field_path FROM mental_health_discharge) vd
		ON c.patient_id = vd.patient_id
	WHERE vd.date >= c.intake_date AND (vd.date <= c.discharge_date OR c.discharge_date IS NULL)
	GROUP BY c.patient_id, c.intake_encounter_id, c.intake_date, c.discharge_date, vd.date, vd.form_field_path
	ORDER BY c.patient_id, c.intake_encounter_id, c.intake_date, c.discharge_date, vd.date DESC)
-- Main query --
SELECT
	pi."Patient_Identifier",
	c.patient_id,
	c.intake_encounter_id,
	pa."Other_patient_identifier",
	pa."Previous_MSF_code",
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
	EXTRACT(YEAR FROM (SELECT age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')))) AS age_admission,
	CASE 
		WHEN EXTRACT(YEAR FROM (SELECT age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy'))))::int <= 3 THEN '0-3'
		WHEN EXTRACT(YEAR FROM (SELECT age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy'))))::int >= 4 AND EXTRACT(YEAR FROM (SELECT age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')))) <= 7 THEN '04-07'
		WHEN EXTRACT(YEAR FROM (SELECT age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy'))))::int >= 8 AND EXTRACT(YEAR FROM (SELECT age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')))) <= 14 THEN '08-14'
		WHEN EXTRACT(YEAR FROM (SELECT age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy'))))::int >= 15 AND EXTRACT(YEAR FROM (SELECT age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')))) <= 17 THEN '15-17'
		WHEN EXTRACT(YEAR FROM (SELECT age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy'))))::int >= 18 AND EXTRACT(YEAR FROM (SELECT age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')))) <= 59 THEN '18-59'
		WHEN EXTRACT(YEAR FROM (SELECT age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy'))))::int >= 60 THEN '60+'
		ELSE NULL
	END AS age_group_admission,
	pdd.gender,
	pa."patientCity" AS camp_location, 
	pa."patientDistrict" AS block,
	pa."Subblock" AS subblock,
	pa."Legal_status",
	pa."Civil_status",
	pa."Education_level",
	pa."Occupation",
	pa."Personal_Situation",
	pa."Living_conditions",
	c.intake_date, 
	CASE 
		WHEN fpia.date IS NOT NULL AND fcia.date IS NULL THEN fpia.date
		WHEN fcia.date IS NOT NULL AND fpia.date IS NULL THEN fcia.date
		WHEN fpia.date IS NOT NULL AND fcia.date IS NOT NULL AND fcia.date::date <= fpia.date::date THEN fcia.date
		WHEN fpia.date IS NOT NULL AND fcia.date IS NOT NULL AND fcia.date::date > fpia.date::date THEN fpia.date
		ELSE NULL
	END	AS enrollment_date,
	CASE 
		WHEN fpia.date IS NOT NULL AND fcia.date IS NULL THEN fpia.visit_location
		WHEN fcia.date IS NOT NULL AND fpia.date IS NULL THEN fcia.visit_location
		WHEN fpia.date IS NOT NULL AND fcia.date IS NOT NULL AND fcia.date::date <= fpia.date::date THEN fcia.visit_location
		WHEN fpia.date IS NOT NULL AND fcia.date IS NOT NULL AND fcia.date::date > fpia.date::date THEN fpia.visit_location
		ELSE NULL
	END	AS enrollment_location,
	c.discharge_date,
	CASE 
		WHEN (fpia.date IS NOT NULL OR fcia.date IS NOT NULL) AND c.discharge_date IS NULL THEN 'Yes'
		ELSE null
	END AS in_cohort,
	c.readmission,
	CASE 
		WHEN fpia.date IS NOT NULL AND fcia.date IS NULL AND fpia.date = c.discharge_date THEN 'Yes'
		WHEN fcia.date IS NOT NULL AND fpia.date IS NULL AND fcia.date = c.discharge_date THEN 'Yes'
		WHEN fpia.date IS NOT NULL AND fcia.date IS NOT NULL AND fcia.date::date <= fpia.date::date AND fcia.date = c.discharge_date THEN 'Yes'
		WHEN fpia.date IS NOT NULL AND fcia.date IS NOT NULL AND fcia.date::date > fpia.date::date AND fpia.date = c.discharge_date THEN 'Yes'
		ELSE NULL
	END	AS same_day_discharge,
	mhi.visit_location AS entry_visit_location,
	CASE 
		WHEN lvl.visit_location IS NOT NULL THEN lvl.visit_location
		WHEN lvl.visit_location IS NULL THEN mhi.visit_location 
		ELSE NULL 
	END AS last_visit_location,
	CASE 
		WHEN mhi.visit_location != lvl.visit_location THEN 'Yes'
		ELSE NULL
	END AS clinic_change,
	lvd.last_visit_date,
	(CURRENT_DATE - lvd.last_visit_date) AS days_since_last_visit,
	lvd.last_visit,
	mhi.source_of_initial_patient_referral,
	CASE WHEN mhi.stressor_1 = 'Non-conflict-related medical condition' THEN 1 WHEN mhi.stressor_2 = 'Non-conflict-related medical condition' THEN 1 WHEN mhi.stressor_3 = 'Non-conflict-related medical condition' THEN 1 ELSE NULL END AS "stressor: non-conflict-related medical condition",
	CASE WHEN mhi.stressor_1 = 'Conflict-related medical condition' THEN 1 WHEN mhi.stressor_2 = 'Conflict-related medical condition' THEN 1 WHEN mhi.stressor_3 = 'Conflict-related medical condition' THEN 1 ELSE NULL END AS "stressor: conflict-related medical condition",
	CASE WHEN mhi.stressor_1 = 'Pre-existing mental health disorder' THEN 1 WHEN mhi.stressor_2 = 'Pre-existing mental health disorder' THEN 1 WHEN mhi.stressor_3 = 'Pre-existing mental health disorder' THEN 1 ELSE NULL END AS "stressor: pre-existing mental health disorder",
	CASE WHEN mhi.stressor_1 = 'Extreme poverty / Financial crisis' THEN 1 WHEN mhi.stressor_2 = 'Extreme poverty / Financial crisis' THEN 1 WHEN mhi.stressor_3 = 'Extreme poverty / Financial crisis' THEN 1 ELSE NULL END AS "stressor: extreme poverty / financial crisis",
	CASE WHEN mhi.stressor_1 = 'Hard living due to conflict' THEN 1 WHEN mhi.stressor_2 = 'Hard living due to conflict' THEN 1 WHEN mhi.stressor_3 = 'Hard living due to conflict' THEN 1 ELSE NULL END AS "stressor: hard living due to conflict",
	CASE WHEN mhi.stressor_1 = 'House / property destroyed' THEN 1 WHEN mhi.stressor_2 = 'House / property destroyed' THEN 1 WHEN mhi.stressor_3 = 'House / property destroyed' THEN 1 ELSE NULL END AS "stressor: house/ property destroyed",
	CASE WHEN mhi.stressor_1 = 'Intra-family related problem' THEN 1 WHEN mhi.stressor_2 = 'Intra-family related problem' THEN 1 WHEN mhi.stressor_3 = 'Intra-family related problem' THEN 1 ELSE NULL END AS "stressor: intra-family related problem",
	CASE WHEN mhi.stressor_1 = 'Close relative detained / died / missing / injured' THEN 1 WHEN mhi.stressor_2 = 'Close relative detained / died / missing / injured' THEN 1 WHEN mhi.stressor_3 = 'Close relative detained / died / missing / injured' THEN 1 ELSE NULL END AS "stressor: close relative detained/died/missing/injured",
	CASE WHEN mhi.stressor_1 = 'Close relative with medical disease' THEN 1 WHEN mhi.stressor_2 = 'Close relative with medical disease' THEN 1 WHEN mhi.stressor_3 = 'Close relative with medical disease' THEN 1 ELSE NULL END AS "stressor: close relative with medical disease",
	CASE WHEN mhi.stressor_1 = 'Loss or excessive social role' THEN 1 WHEN mhi.stressor_2 = 'Loss or excessive social role' THEN 1 WHEN mhi.stressor_3 = 'Loss or excessive social role' THEN 1 ELSE NULL END AS "stressor: loss or excessive social role",
	CASE WHEN mhi.stressor_1 = 'Victim of neglect' THEN 1 WHEN mhi.stressor_2 = 'Victim of neglect' THEN 1 WHEN mhi.stressor_3 = 'Victim of neglect' THEN 1 ELSE NULL END AS "stressor: victim of neglect",
	CASE WHEN mhi.stressor_1 = 'Isolated / Social exclusion' THEN 1 WHEN mhi.stressor_2 = 'Isolated / Social exclusion' THEN 1 WHEN mhi.stressor_3 = 'Isolated / Social exclusion' THEN 1 ELSE NULL END AS "stressor: isolated / social exclusion",
	CASE WHEN mhi.stressor_1 = 'Direct witness of violence' THEN 1 WHEN mhi.stressor_2 = 'Direct witness of violence' THEN 1 WHEN mhi.stressor_3 = 'Direct witness of violence' THEN 1 ELSE NULL END AS "stressor: direct witness of violence",
	CASE WHEN mhi.stressor_1 = 'Direct victim of violence' THEN 1 WHEN mhi.stressor_2 = 'Direct victim of violence' THEN 1 WHEN mhi.stressor_3 = 'Direct victim of violence' THEN 1 ELSE NULL END AS "stressor: direct victim of violence",
	CASE WHEN mhi.stressor_1 = 'Survivor of sexual violence' THEN 1 WHEN mhi.stressor_2 = 'Survivor of sexual violence' THEN 1 WHEN mhi.stressor_3 = 'Survivor of sexual violence' THEN 1 ELSE NULL END AS "stressor: survivor of sexual violence",
	CASE WHEN mhi.stressor_1 = 'Detained' THEN 1 WHEN mhi.stressor_2 = 'Detained' THEN 1 WHEN mhi.stressor_3 = 'Detained' THEN 1 ELSE NULL END AS "stressor: detained",
	CASE WHEN mhi.stressor_1 = 'None' THEN 1 WHEN mhi.stressor_2 = 'None' THEN 1 WHEN mhi.stressor_3 = 'None' THEN 1 ELSE NULL END AS "stressor: none",
	CASE WHEN mhi.stressor_1 IS NOT NULL AND mhi.stressor_1 != 'None' THEN 1 ELSE 0 END + CASE WHEN mhi.stressor_2 IS NOT NULL AND mhi.stressor_2 != 'None' THEN 1 ELSE 0 END + CASE WHEN mhi.stressor_3 IS NOT NULL AND mhi.stressor_3 != 'None' THEN 1 ELSE 0 END AS stressor_count,
	mhi.risk_factor_present,
	ls.main_syndrome,
	ls.additional_syndrome,
	ls.depression AS "syndrome: depression",	
	ls.anxiety_disorder AS "syndrome: anxiety disorder",
	ls.trauma_related_symptoms AS "syndrome: trauma related symptoms",	
	ls.adult_behavioral_substance_problem AS "syndrome: adult behavioral substance problem",	
	ls.child_behavioral_problem AS "syndrome: child behavioral problem",	
	ls.psychosis AS "syndrome: psychosis",	
	ls.psychosomatic_problems AS "syndrome: psychosomatic problems",	
	ls.neurocognitive_problem AS "syndrome: neurocognitive problem",	
	ls.epilepsy AS "syndrome: epilepsy",	
	ls.other_syndrome AS "syndrome: other",
	ic.cgi_s_score_at_initial_assessment,
	mhdx.acute_transient_psychotic_disorder AS "diagnosis: acute and transient psychotic disorder",	
	mhdx.acute_stress_reaction AS "diagnosis: acute stress reaction",	
	mhdx.adjustment_disorders AS "diagnosis: adjustment disorders",	
	mhdx.anxiety_disorder AS "diagnosis: anxiety disorder",
	mhdx.bipolar_disorder AS "diagnosis: bipolar disorder",	
	mhdx.childhood_emotional_disorder AS "diagnosis: childhood emotional disorder",
	mhdx.conduct_disorders AS "diagnosis: conduct disorders",
	mhdx.delirium AS "diagnosis: delirium",	
	mhdx.dementia AS "diagnosis: dementia",	
	mhdx.dissociative_conversion_disorder AS "diagnosis: dissociative and conversion disorder",
	mhdx.dissociative_convulsions AS "diagnosis: dissociative convulsions",
	mhdx.hyperkinetic_disorder AS "diagnosis: hyperkinetic disorder",
	mhdx.intellectual_disability AS "diagnosis: intellectual disability",
	mhdx.disorders_due_drug_psychoactive_substances AS "diagnosis: mental/behavioural disorders due to drug/psychoactive substances use",
	mhdx.disorders_due_alcohol AS "diagnosis: mental or behavioural disorders due to use of alcohol",
	mhdx.mild_depressive_episode AS "diagnosis: mild depressive episode",
	mhdx.moderate_depressive_episode AS "diagnosis: moderate depressive episode",
	mhdx.nonorganic_enuresis AS "diagnosis: nonorganic enuresis",
	mhdx.obsessive_compulsive_disorder AS "diagnosis: obsessive compulsive disorder",
	mhdx.panic_disorder AS "diagnosis: panic disorder",
	mhdx.pervasive_developmental_disorder AS "diagnosis: pervasive developmental disorder",
	mhdx.postpartum_depression AS "diagnosis: post-partum depression",
	mhdx.postpartum_psychosis AS "diagnosis: post-partum psychosis",
	mhdx.ptsd AS "diagnosis: post traumatic stress disorder",
	mhdx.schizophrenia AS "diagnosis: schizophrenia",
	mhdx.severe_depressive_episode_with_psychotic_symptoms AS "diagnosis: severe depressive episode with psychotic symptoms",
	mhdx.severe_depressive_episode_without_psychotic_symptoms AS "diagnosis: severe depressive episode without psychotic symptoms",
	mhdx.somatoform_disorders AS "diagnosis: somatoform disorders",
	mhdx.other_mh AS "diagnosis: other mental health diagnosis",
	mhdl.diagnosis_list,
	mhd.visit_location AS discharge_visit_location,
	mhd.intervention_setting AS discharge_intervention_setting,
	mhd.type_of_activity AS discharge_type_of_activity,
	mhd.mhos_at_discharge,
	mhd.cgi_s_score_at_discharge,
	mhd.cgi_i_score_at_discharge,
	mhd.patient_outcome,
	cia.counselor_initial_consultations,
	cfui.counselor_fu_individual_sessions,
	COALESCE(cia.counselor_initial_consultations,0) + COALESCE(cfui.counselor_fu_individual_sessions,0) AS counselor_individual_sessions,
	cfuo.counselor_fu_other_sessions,
	COALESCE(cia.counselor_initial_consultations,0) + COALESCE(cfui.counselor_fu_individual_sessions,0) + COALESCE(cfuo.counselor_fu_other_sessions,0) AS counselor_sessions,
	cfulc.counselor_fu_light_contact_chw_sessions,
	pia.psychiatrist_initial_consultations,
	pfui.psychiatrist_fu_individual_sessions,
	COALESCE(pia.psychiatrist_initial_consultations,0) + COALESCE(pfui.psychiatrist_fu_individual_sessions,0) AS psychiatrist_individual_sessions,
	pfuo.psychiatrist_fu_other_sessions,
	COALESCE(pia.psychiatrist_initial_consultations,0) + COALESCE(pfui.psychiatrist_fu_individual_sessions,0) + COALESCE(pfuo.psychiatrist_fu_other_sessions,0) AS psychiatrist_sessions,
	pfulc.psychiatrist_fu_light_contact_chw_sessions,
	CASE WHEN pia.psychiatrist_initial_consultations IS NOT NULL THEN 'Yes' ELSE NULL END AS followed_by_psychiatrist,
	pp.psychotropic_prescription
FROM cohort c
LEFT OUTER JOIN patient_identifier pi
	ON c.patient_id = pi.patient_id
LEFT OUTER JOIN person_attributes pa
	ON c.patient_id = pa.person_id
LEFT OUTER JOIN person_details_default pdd 
	ON c.patient_id = pdd.person_id
LEFT OUTER JOIN patient_encounter_details_default ped 
	ON c.intake_encounter_id = ped.encounter_id
LEFT OUTER JOIN first_psy_initial_assessment fpia
	ON c.intake_encounter_id = fpia.intake_encounter_id
LEFT OUTER JOIN first_clinician_initial_assessment fcia
	ON c.intake_encounter_id = fcia.intake_encounter_id
LEFT OUTER JOIN mental_health_intake mhi
	ON c.intake_encounter_id = mhi.encounter_id
LEFT OUTER JOIN last_syndrome ls 
	ON c.intake_encounter_id = ls.intake_encounter_id
LEFT OUTER JOIN initial_cgis ic
	ON c.intake_encounter_id = ic.intake_encounter_id
LEFT OUTER JOIN mental_health_discharge mhd 
	ON c.discharge_encounter_id = mhd.encounter_id
LEFT OUTER JOIN last_mh_diagnosis mhdx
	ON c.intake_encounter_id = mhdx.intake_encounter_id 
LEFT OUTER JOIN mh_diagnosis_list mhdl 
	ON c.intake_encounter_id = mhdl.intake_encounter_id
LEFT OUTER JOIN counselor_ia cia 
	ON c.intake_encounter_id = cia.intake_encounter_id
LEFT OUTER JOIN counselor_fu_individual cfui 
	ON c.intake_encounter_id = cfui.intake_encounter_id
LEFT OUTER JOIN counselor_fu_light_contact_chw cfulc 
	ON c.intake_encounter_id = cfulc.intake_encounter_id
LEFT OUTER JOIN counselor_fu_other cfuo 
	ON c.intake_encounter_id = cfuo.intake_encounter_id
LEFT OUTER JOIN psy_ia pia 
	ON c.intake_encounter_id = pia.intake_encounter_id
LEFT OUTER JOIN psy_fu_individual pfui 
	ON c.intake_encounter_id = pfui.intake_encounter_id
LEFT OUTER JOIN psy_fu_light_contact_chw pfulc 
	ON c.intake_encounter_id = pfulc.intake_encounter_id
LEFT OUTER JOIN psy_fu_other pfuo 
	ON c.intake_encounter_id = pfuo.intake_encounter_id
LEFT OUTER JOIN psychotropic_prescription pp
	ON c.intake_encounter_id = pp.intake_encounter_id
LEFT OUTER JOIN last_visit_location lvl
	ON c.intake_encounter_id = lvl.intake_encounter_id
LEFT OUTER JOIN last_visit_date lvd
	ON c.intake_encounter_id = lvd.intake_encounter_id;