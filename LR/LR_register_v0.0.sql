-- The first CTEs build the frame for patients entering and exiting the cohort. This frame is based on the MH intake form and the MH discharge form. The query takes all intake dates and matches discharge dates if the discharge date falls between the intake date and the next intake date (if present).
WITH intake AS (
	SELECT 
		patient_id, encounter_id AS intake_encounter_id, date AS intake_date, DENSE_RANK () OVER (PARTITION BY patient_id ORDER BY date) AS intake_order, LEAD (date) OVER (PARTITION BY patient_id ORDER BY date) AS next_intake_date
	FROM mental_health_intake),
cohort AS (
	SELECT
		i.patient_id, i.intake_encounter_id, i.intake_date, CASE WHEN i.intake_order > 1 THEN 'Yes' END readmission, mhd.encounter_id AS discharge_encounter_id, mhd.discharge_date
	FROM intake i
	LEFT JOIN mental_health_discharge mhd 
		ON i.patient_id = mhd.patient_id AND mhd.discharge_date >= i.intake_date AND (mhd.discharge_date < i.next_intake_date OR i.next_intake_date IS NULL)),
-- The Syndrome CTEs pivot syndrome data horizontally from the MH counselor initial assessment form. If more than one form is filled per cohort enrollment than the data from the last form is reported. 
syndrome_pivot AS (
	SELECT 
		DISTINCT ON (pcia.patient_id, pcia.date) pcia.patient_id, 
		pcia.date::date,
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
	GROUP BY c.patient_id, c.intake_encounter_id, c.intake_date, c.discharge_encounter_id, c.discharge_date, sp.date, sp.depression, sp.anxiety_disorder, sp.trauma_related_symptoms, sp.adult_behavioral_substance_problem, sp.child_behavioral_problem, sp.psychosis, sp.psychosomatic_problems, sp.neurocognitive_problem, sp.epilepsy, sp.other_syndrome
	ORDER BY c.patient_id, c.intake_encounter_id, c.intake_date, sp.date DESC),
-- The initial CGI-Severity score CTE reports only one initial CGI-Severity score per cohort enrollment. If the score has been reported in mhGAP initial assessment form, then the most recent record is reported. If no mhGAP initial assessment form is completed, then the most recent record from the counselor initial assessment form is reported. 
initial_cgis AS (
	SELECT
		DISTINCT ON (c.patient_id, c.intake_encounter_id, c.intake_date, c.discharge_date) c.intake_encounter_id,
		CASE 
			WHEN pmia.cgi_s_score IS NOT NULL THEN pmia.cgi_s_score
			WHEN pmia.cgi_s_score IS NULL AND pcia.cgi_s_score IS NOT NULL THEN pcia.cgi_s_score
			ELSE NULL 
		END AS cgi_s_score_at_initial_assessment	
	FROM cohort c
	LEFT OUTER JOIN psychiatrist_mhgap_initial_assessment pmia 
		ON c.patient_id = pmia.patient_id
	LEFT OUTER JOIN psy_counselors_initial_assessment pcia
		ON c.patient_id = pcia.patient_id
	WHERE pmia.date::date >= c.intake_date AND (pmia.date::date <= c.discharge_date OR c.discharge_date IS NULL) AND pcia.date::date >= c.intake_date AND (pcia.date::date <= c.discharge_date OR c.discharge_date IS NULL)
	ORDER BY c.patient_id, c.intake_encounter_id, c.intake_date, c.discharge_date, pmia.date::date DESC, pcia.date::date DESC),
-- The NCD diagnosis CTEs pivot NCD diagnosis data horizontally from the NCD form. Only the last diagnoses reported per cohort enrollment are present. 
ncd_diagnosis_pivot AS (
	SELECT 
		DISTINCT ON (n.encounter_id, n.patient_id, n.date::date) n.encounter_id, 
		n.patient_id, 
		n.date::date,
		MAX (CASE WHEN d.diagnosis = 'Focal epilepsy' THEN 1 ELSE NULL END) AS focal_epilepsy,
		MAX (CASE WHEN d.diagnosis = 'Generalised epilepsy' THEN 1 ELSE NULL END) AS generalised_epilepsy,
		MAX (CASE WHEN d.diagnosis = 'Unclassified epilepsy' THEN 1 ELSE NULL END) AS unclassified_epilepsy,
		MAX (CASE WHEN d.diagnosis = 'Other' THEN 1 ELSE NULL END) AS other_ncd
	FROM ncd n
	LEFT OUTER JOIN diagnosis d 
		ON d.encounter_id = n.encounter_id AND d.diagnosis IS NOT NULL 
	WHERE d.diagnosis IS NOT NULL 
	GROUP BY n.encounter_id, n.patient_id, n.date::date),
last_ncd_diagnosis AS (
	SELECT 
		DISTINCT ON (c.patient_id, c.intake_encounter_id) c.patient_id,
		c.intake_encounter_id,
		c.intake_date, 
		c.discharge_encounter_id,
		c.discharge_date, 
		ndp.date::date,
		ndp.focal_epilepsy,
		ndp.generalised_epilepsy,
		ndp.unclassified_epilepsy,
		ndp.other_ncd
	FROM cohort c
	LEFT OUTER JOIN ncd_diagnosis_pivot ndp 
		ON c.patient_id = ndp.patient_id AND c.intake_date <= ndp.date AND CASE WHEN c.discharge_date IS NOT NULL THEN c.discharge_date ELSE current_date END >= ndp.date
	WHERE ndp.date IS NOT NULL	
	GROUP BY c.patient_id, c.intake_encounter_id, c.intake_date, c.discharge_encounter_id, c.discharge_date, ndp.date, ndp.focal_epilepsy, ndp.generalised_epilepsy, ndp.unclassified_epilepsy, ndp.other_ncd
	ORDER BY c.patient_id, c.intake_encounter_id, c.intake_date, ndp.date DESC),
-- The Mental Health diagnosis CTE pivots mental health diagnosis data horizontally from the Psychiatrist mhGap initial and follow-up forms. Only the last diagnoses reported per cohort enrollment are present. 
last_mh_main_dx AS (
	SELECT 
		DISTINCT ON (c.patient_id, c.intake_encounter_id) c.patient_id,
		c.intake_encounter_id,
		c.intake_date, 
		c.discharge_encounter_id,
		c.discharge_date,
		mmhd.date,
		mmhd.main_diagnosis AS diagnosis
	FROM cohort c
	LEFT OUTER JOIN (
		SELECT patient_id, date::date, main_diagnosis FROM psychiatrist_mhgap_initial_assessment
		UNION
		SELECT patient_id, date::date, main_diagnosis FROM psychiatrist_mhgap_follow_up) mmhd
		ON c.patient_id = mmhd.patient_id AND c.intake_date <= mmhd.date::date AND CASE WHEN c.discharge_date IS NOT NULL THEN c.discharge_date ELSE current_date END >= mmhd.date::date
	WHERE mmhd.main_diagnosis IS NOT NULL 
	GROUP BY c.patient_id, c.intake_encounter_id, c.intake_date, c.discharge_encounter_id, c.discharge_date, mmhd.date::date,	mmhd.main_diagnosis 
	ORDER BY c.patient_id, c.intake_encounter_id, c.intake_date, mmhd.date::date DESC),
last_mh_sec_dx AS (
	SELECT 
		DISTINCT ON (c.patient_id, c.intake_encounter_id) c.patient_id,
		c.intake_encounter_id,
		c.intake_date, 
		c.discharge_encounter_id,
		c.discharge_date,
		mmhd.date,
		mmhd.secondary_diagnosis AS diagnosis
	FROM cohort c
	LEFT OUTER JOIN (
		SELECT patient_id, date::date, secondary_diagnosis FROM psychiatrist_mhgap_initial_assessment
		UNION
		SELECT patient_id, date::date, secondary_diagnosis FROM psychiatrist_mhgap_follow_up) mmhd
		ON c.patient_id = mmhd.patient_id AND c.intake_date <= mmhd.date::date AND CASE WHEN c.discharge_date IS NOT NULL THEN c.discharge_date ELSE current_date END >= mmhd.date::date
	WHERE mmhd.secondary_diagnosis IS NOT NULL 
	GROUP BY c.patient_id, c.intake_encounter_id, c.intake_date, c.discharge_encounter_id, c.discharge_date, mmhd.date::date, mmhd.secondary_diagnosis 
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
	FROM (SELECT patient_id, intake_encounter_id, diagnosis FROM last_mh_main_dx
	UNION
	SELECT patient_id, intake_encounter_id, diagnosis FROM last_mh_sec_dx) mhdu
	GROUP BY mhdu.patient_id, intake_encounter_id),
-- The NCD form CTE extracts the last NCD visit data per cohort enrollment to look at if there are values reported for pregnancy, hospitalization, missed medication, or seizures since the last visit. 
last_ncd_form AS (
SELECT 
		DISTINCT ON (c.patient_id, c.intake_encounter_id) c.patient_id,
		c.intake_encounter_id,
		c.intake_date, 
		c.discharge_encounter_id,
		c.discharge_date, 
		n.date::date AS last_ncd_date,
		n.visit_type,
		n.currently_pregnant AS pregnant_last_visit,
		n.hospitalised_since_last_visit AS hospitalised_last_visit,
		n.missed_medication_doses_in_last_7_days AS missed_medication_last_visit,
		n.seizures_since_last_visit AS seizures_last_visit
	FROM cohort c
	LEFT OUTER JOIN ncd n
		ON c.patient_id = n.patient_id AND c.intake_date <= n.date::date AND CASE WHEN c.discharge_date IS NOT NULL THEN c.discharge_date ELSE current_date END >= n.date::date
	ORDER BY c.patient_id, c.intake_encounter_id, n.patient_id, n.date::date DESC),		
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
-- The counselor follow-up CTE counts the number of other follow up counselor sessions that took place per cohort enrollment, excludes individual and missed sessions. 
counselor_fu_other AS (
	SELECT 
		DISTINCT ON (c.patient_id, c.intake_encounter_id, c.intake_date, c.discharge_date) c.intake_encounter_id,
		COUNT(*) AS counselor_fu_other_sessions
	FROM cohort c
	LEFT OUTER JOIN psy_counselors_follow_up pcfu 
		ON c.patient_id = pcfu.patient_id
	WHERE pcfu.date::date >= c.intake_date AND (pcfu.date::date <= c.discharge_date OR c.discharge_date IS NULL) AND (pcfu.type_of_activity != 'Individual session' OR pcfu.type_of_activity != 'Missed appointment')
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
psy_fu_other AS (
	SELECT 
		DISTINCT ON (c.patient_id, c.intake_encounter_id, c.intake_date, c.discharge_date) c.intake_encounter_id,
		COUNT(*) AS psychiatrist_fu_other_sessions
	FROM cohort c
	LEFT OUTER JOIN psychiatrist_mhgap_follow_up pmfu 
		ON c.patient_id = pmfu.patient_id
	WHERE pmfu.date::date >= c.intake_date AND (pmfu.date::date <= c.discharge_date OR c.discharge_date IS NULL) AND (pmfu.type_of_activity != 'Individual session' OR pmfu.type_of_activity != 'Missed appointment')
	GROUP BY c.patient_id, c.intake_encounter_id, c.intake_date, c.discharge_date),
-- The NCD CTE counts the number of NCD consultations that took place per cohort entry. Includes initial, follow-up, and discharge consultations. Excludes discharge consultations where the outcome has been recorded as 'lost to follow up' or 'deceased'.
ncd_consultations AS (
	SELECT 
		DISTINCT ON (c.patient_id, c.intake_encounter_id, c.intake_date, c.discharge_date) c.intake_encounter_id,
		COUNT(*) AS ncd_consultations
	FROM cohort c
	LEFT OUTER JOIN ncd n  
		ON c.patient_id = n.patient_id
	WHERE n.date::date >= c.intake_date AND (n.date::date <= c.discharge_date OR c.discharge_date IS NULL) AND (n.patient_outcome IS NULL OR n.patient_outcome != 'Lost to follow up' OR n.patient_outcome != 'Deceased')
	GROUP BY c.patient_id, c.intake_encounter_id, c.intake_date, c.discharge_date),
-- The psychotropic prescription CTE identifies any patient who has had at least one psychotropic prescription while enrolled in the cohort. Both past and active prescriptions are considered.
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
WHERE mdd.start_date >= c.intake_date AND (mdd.start_date <= c.discharge_date OR c.discharge_date IS NULL) AND mdd.coded_drug_name IS NOT NULL AND mdd.coded_drug_name != 'FOLIC acid, 5 mg, tab.'),
-- The visit location CTE finds the last visit location reported across all clinical consultaiton/session forms per cohort enrollment.
last_visit_location AS (	
	SELECT 
		DISTINCT ON (c.patient_id, c.intake_encounter_id, c.intake_date, c.discharge_date) c.intake_encounter_id,
		vl.visit_location AS visit_location
	FROM cohort c
	LEFT OUTER JOIN (
		SELECT n.date::date, n.patient_id, n.visit_location FROM ncd n WHERE n.visit_location IS NOT NULL 
		UNION
		SELECT pcia.date::date, pcia.patient_id, pcia.visit_location FROM psy_counselors_initial_assessment pcia WHERE pcia.visit_location IS NOT NULL 
		UNION 
		SELECT pmia.date::date, pmia.patient_id, pmia.visit_location FROM psychiatrist_mhgap_initial_assessment pmia WHERE pmia.visit_location IS NOT NULL 
		UNION
		SELECT pcfu.date::date, pcfu.patient_id, pcfu.visit_location FROM psy_counselors_follow_up pcfu WHERE pcfu.visit_location IS NOT NULL 
		UNION
		SELECT pmfu.date::date, pmfu.patient_id, pmfu.visit_location FROM psychiatrist_mhgap_follow_up pmfu WHERE pmfu.visit_location IS NOT NULL
		UNION
		SELECT mhd.discharge_date AS date, mhd.patient_id, mhd.location AS visit_location FROM mental_health_discharge mhd WHERE mhd.location IS NOT NULL) vl
		ON c.patient_id = vl.patient_id
	WHERE vl.date >= c.intake_date AND (vl.date <= c.discharge_date OR c.discharge_date IS NULL)
	GROUP BY c.patient_id, c.intake_encounter_id, c.intake_date, c.discharge_date, vl.date, vl.visit_location
	ORDER BY c.patient_id, c.intake_encounter_id, c.intake_date, c.discharge_date, vl.date DESC)
-- Main query --
SELECT
	pi."Patient_Identifier",
	c.patient_id,
	c.intake_encounter_id,
	pa."Patient_code",
	pdd.age AS age_current,
	CASE 
		WHEN pdd.age::int <= 3 THEN '0-3'
		WHEN pdd.age::int >= 4 AND pdd.age::int <= 7 THEN '04-07'
		WHEN pdd.age::int >= 8 AND pdd.age::int <= 14 THEN '08-14'
		WHEN pdd.age::int >= 15 AND pdd.age::int <= 17 THEN '15-17'
		WHEN pdd.age::int >= 18 THEN '18+'
		ELSE NULL
	END AS age_group_current,
	EXTRACT(YEAR FROM (SELECT age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')))) AS age_admission,
	CASE 
		WHEN EXTRACT(YEAR FROM (SELECT age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy'))))::int <= 3 THEN '0-3'
		WHEN EXTRACT(YEAR FROM (SELECT age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy'))))::int >= 4 AND EXTRACT(YEAR FROM (SELECT age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')))) <= 7 THEN '04-07'
		WHEN EXTRACT(YEAR FROM (SELECT age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy'))))::int >= 8 AND EXTRACT(YEAR FROM (SELECT age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')))) <= 14 THEN '08-14'
		WHEN EXTRACT(YEAR FROM (SELECT age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy'))))::int >= 15 AND EXTRACT(YEAR FROM (SELECT age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')))) <= 17 THEN '15-17'
		WHEN EXTRACT(YEAR FROM (SELECT age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy'))))::int >= 18 THEN '18+'
		ELSE NULL
	END AS age_group_admission,
	pdd.gender,
	pa."patientState", 
	pa."Education_level",
	pa."Personal_Situation",
	pa."Living_conditions",
	CASE
		WHEN (ncddx.focal_epilepsy IS NOT NULL OR ncddx.generalised_epilepsy IS NOT NULL OR ncddx.unclassified_epilepsy IS NOT NULL OR ncddx.other_ncd IS NOT NULL) AND 
			mhdx.acute_transient_psychotic_disorder IS NULL AND mhdx.acute_stress_reaction IS NULL AND mhdx.adjustment_disorders IS NULL AND mhdx.anxiety_disorder IS NULL AND mhdx.bipolar_disorder IS NULL AND mhdx.childhood_emotional_disorder IS NULL AND mhdx.conduct_disorders IS NULL AND mhdx.delirium IS NULL AND mhdx.dementia IS NULL AND mhdx.dissociative_conversion_disorder IS NULL AND mhdx.dissociative_convulsions IS NULL AND mhdx.hyperkinetic_disorder IS NULL AND mhdx.intellectual_disability IS NULL AND mhdx.disorders_due_drug_psychoactive_substances IS NULL AND mhdx.disorders_due_alcohol IS NULL AND mhdx.mild_depressive_episode IS NULL AND mhdx.moderate_depressive_episode IS NULL AND mhdx.nonorganic_enuresis IS NULL AND mhdx.obsessive_compulsive_disorder IS NULL AND mhdx.panic_disorder IS NULL AND mhdx.pervasive_developmental_disorder IS NULL AND mhdx.postpartum_depression IS NULL AND mhdx.postpartum_psychosis IS NULL AND mhdx.ptsd IS NULL AND mhdx.schizophrenia IS NULL AND mhdx.severe_depressive_episode_with_psychotic_symptoms IS NULL AND mhdx.severe_depressive_episode_without_psychotic_symptoms IS NULL AND mhdx.somatoform_disorders IS NULL AND mhdx.other_mh IS NULL THEN 'Epilepsy'
		WHEN (mhdx.disorders_due_drug_psychoactive_substances IS NOT NULL OR mhdx.disorders_due_alcohol IS NOT NULL) AND 
			ncddx.focal_epilepsy IS NULL AND ncddx.generalised_epilepsy IS NULL AND ncddx.unclassified_epilepsy IS NULL AND ncddx.other_ncd IS NULL THEN 'Substance use disorders'
		WHEN (mhdx.acute_transient_psychotic_disorder IS NOT NULL OR mhdx.acute_stress_reaction IS NOT NULL OR mhdx.adjustment_disorders IS NOT NULL OR mhdx.anxiety_disorder IS NOT NULL OR mhdx.bipolar_disorder IS NOT NULL OR mhdx.childhood_emotional_disorder IS NOT NULL OR mhdx.conduct_disorders IS NOT NULL OR mhdx.delirium IS NOT NULL OR mhdx.dementia IS NOT NULL OR mhdx.dissociative_conversion_disorder IS NOT NULL OR mhdx.dissociative_convulsions IS NOT NULL OR mhdx.hyperkinetic_disorder IS NOT NULL OR mhdx.intellectual_disability IS NOT NULL OR mhdx.mild_depressive_episode IS NOT NULL OR mhdx.moderate_depressive_episode IS NOT NULL OR mhdx.nonorganic_enuresis IS NOT NULL OR mhdx.obsessive_compulsive_disorder IS NOT NULL OR mhdx.panic_disorder IS NOT NULL OR mhdx.pervasive_developmental_disorder IS NOT NULL OR mhdx.postpartum_depression IS NOT NULL OR mhdx.postpartum_psychosis IS NOT NULL OR mhdx.ptsd IS NOT NULL OR mhdx.schizophrenia IS NOT NULL OR mhdx.severe_depressive_episode_with_psychotic_symptoms IS NOT NULL OR mhdx.severe_depressive_episode_without_psychotic_symptoms IS NOT NULL OR mhdx.somatoform_disorders IS NOT NULL OR mhdx.other_mh IS NOT NULL) AND 
			ncddx.focal_epilepsy IS NULL AND ncddx.generalised_epilepsy IS NULL AND ncddx.unclassified_epilepsy IS NULL AND ncddx.other_ncd IS NULL THEN 'Mental health'
		WHEN (mhdx.acute_transient_psychotic_disorder IS NOT NULL OR mhdx.acute_stress_reaction IS NOT NULL OR mhdx.adjustment_disorders IS NOT NULL OR mhdx.anxiety_disorder IS NOT NULL OR mhdx.bipolar_disorder IS NOT NULL OR mhdx.childhood_emotional_disorder IS NOT NULL OR mhdx.conduct_disorders IS NOT NULL OR mhdx.delirium IS NOT NULL OR mhdx.dementia IS NOT NULL OR mhdx.dissociative_conversion_disorder IS NOT NULL OR mhdx.dissociative_convulsions IS NOT NULL OR mhdx.hyperkinetic_disorder IS NOT NULL OR mhdx.intellectual_disability IS NOT NULL OR mhdx.disorders_due_drug_psychoactive_substances IS NOT NULL OR mhdx.disorders_due_alcohol IS NOT NULL OR mhdx.mild_depressive_episode IS NOT NULL OR mhdx.moderate_depressive_episode IS NOT NULL OR mhdx.nonorganic_enuresis IS NOT NULL OR mhdx.obsessive_compulsive_disorder IS NOT NULL OR mhdx.panic_disorder IS NOT NULL OR mhdx.pervasive_developmental_disorder IS NOT NULL OR mhdx.postpartum_depression IS NOT NULL OR mhdx.postpartum_psychosis IS NOT NULL OR mhdx.ptsd IS NOT NULL OR mhdx.schizophrenia IS NOT NULL OR mhdx.severe_depressive_episode_with_psychotic_symptoms IS NOT NULL OR mhdx.severe_depressive_episode_without_psychotic_symptoms IS NOT NULL OR mhdx.somatoform_disorders IS NOT NULL OR mhdx.other_mh IS NOT NULL) AND 
			(ncddx.focal_epilepsy IS NOT NULL OR ncddx.generalised_epilepsy IS NOT NULL OR ncddx.unclassified_epilepsy IS NOT NULL OR ncddx.other_ncd IS NOT NULL) THEN 'Mental health/epilepsy'
		ELSE NULL 
	END AS cohort,
	c.intake_date as entry_date, 
	c.discharge_date,
	CASE 
		WHEN c.discharge_date IS NULL THEN 'Yes'
		ELSE null
	END AS active,
	c.readmission,
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
	CASE WHEN mhi.stressor_1 = 'Displaced due to conflict' THEN 1 WHEN mhi.stressor_2 = 'Displaced due to conflict' THEN 1 WHEN mhi.stressor_3 = 'Displaced due to conflict' THEN 1 ELSE NULL END AS "stressor: displaced due to conflict",
	CASE WHEN mhi.stressor_1 = 'Distress as a result of Ebola outbreak' THEN 1 WHEN mhi.stressor_2 = 'Distress as a result of Ebola outbreak' THEN 1 WHEN mhi.stressor_3 = 'Distress as a result of Ebola outbreak' THEN 1 ELSE NULL END AS "stressor: distress as a result of ebola outbreak",
	CASE WHEN mhi.stressor_1 = 'Stigma' THEN 1 WHEN mhi.stressor_2 = 'Stigma' THEN 1 WHEN mhi.stressor_3 = 'Stigma' THEN 1 ELSE NULL END AS "stressor: stigma",
	CASE WHEN mhi.stressor_1 = 'Domestic violence' THEN 1 WHEN mhi.stressor_2 = 'Domestic violence' THEN 1 WHEN mhi.stressor_3 = 'Domestic violence' THEN 1 ELSE NULL END AS "stressor: domestic violence",
	CASE WHEN mhi.stressor_1 = 'None' THEN 1 WHEN mhi.stressor_2 = 'None' THEN 1 WHEN mhi.stressor_3 = 'None' THEN 1 ELSE NULL END AS "stressor: none",
	CASE WHEN mhi.stressor_1 IS NOT NULL AND mhi.stressor_1 != 'None' THEN 1 ELSE 0 END +
		CASE WHEN mhi.stressor_2 IS NOT NULL AND mhi.stressor_2 != 'None' THEN 1 ELSE 0 END +
		CASE WHEN mhi.stressor_3 IS NOT NULL AND mhi.stressor_3 != 'None' THEN 1 ELSE 0 END AS stressor_count,
	mhi.risk_factor_present,
	ls.depression AS "synrome: depression",	
	ls.anxiety_disorder AS "synrome: anxiety disorder",
	ls.trauma_related_symptoms AS "synrome: trauma related symptoms",	
	ls.adult_behavioral_substance_problem AS "synrome: adult behavioral substance problem",	
	ls.child_behavioral_problem AS "synrome: child behavioral problem",	
	ls.psychosis AS "synrome: psychosis",	
	ls.psychosomatic_problems AS "synrome: psychosomatic problems",	
	ls.neurocognitive_problem AS "synrome: neurocognitive problem",	
	ls.epilepsy AS "synrome: epilepsy",	
	ls.other_syndrome AS "synrome: other",
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
	ncddx.focal_epilepsy AS "diagnosis: focal epilepsy",
	ncddx.generalised_epilepsy AS "diagnosis: generalised epilepsy",
	ncddx.unclassified_epilepsy AS "diagnosis: unclassified epilepsy",
	ncddx.other_ncd AS "diagnosis: other epilepsy",
	lnf.last_ncd_date,
	lnf.pregnant_last_visit,
	lnf.hospitalised_last_visit,
	lnf.missed_medication_last_visit,
	lnf.seizures_last_visit,
	mhd.location AS discharge_visit_location,
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
	pia.psychiatrist_initial_consultations,
	pfui.psychiatrist_fu_individual_sessions,
	COALESCE(pia.psychiatrist_initial_consultations,0) + COALESCE(pfui.psychiatrist_fu_individual_sessions,0) AS psychiatrist_individual_sessions,
	pfuo.psychiatrist_fu_other_sessions,
	COALESCE(pia.psychiatrist_initial_consultations,0) + COALESCE(pfui.psychiatrist_fu_individual_sessions,0) + COALESCE(pfuo.psychiatrist_fu_other_sessions,0) AS psychiatrist_sessions,
	ncdc.ncd_consultations,
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
LEFT OUTER JOIN mental_health_intake mhi
	ON c.intake_encounter_id = mhi.encounter_id
LEFT OUTER JOIN last_syndrome ls 
	ON c.intake_encounter_id = ls.intake_encounter_id
LEFT OUTER JOIN initial_cgis ic 
	ON c.intake_encounter_id = ic.intake_encounter_id
LEFT OUTER JOIN mental_health_discharge mhd 
	ON c.discharge_encounter_id = mhd.encounter_id
LEFT OUTER JOIN last_ncd_diagnosis ncddx
	ON c.intake_encounter_id = ncddx.intake_encounter_id 
LEFT OUTER JOIN last_mh_diagnosis mhdx
	ON c.intake_encounter_id = mhdx.intake_encounter_id 
LEFT OUTER JOIN last_ncd_form lnf 
	ON c.intake_encounter_id = lnf.intake_encounter_id 
LEFT OUTER JOIN counselor_ia cia 
	ON c.intake_encounter_id = cia.intake_encounter_id
LEFT OUTER JOIN counselor_fu_individual cfui 
	ON c.intake_encounter_id = cfui.intake_encounter_id
LEFT OUTER JOIN counselor_fu_other cfuo 
	ON c.intake_encounter_id = cfuo.intake_encounter_id
LEFT OUTER JOIN psy_ia pia 
	ON c.intake_encounter_id = pia.intake_encounter_id
LEFT OUTER JOIN psy_fu_individual pfui 
	ON c.intake_encounter_id = pfui.intake_encounter_id
LEFT OUTER JOIN psy_fu_other pfuo 
	ON c.intake_encounter_id = pfuo.intake_encounter_id
LEFT OUTER JOIN ncd_consultations ncdc
	ON c.intake_encounter_id = ncdc.intake_encounter_id
LEFT OUTER JOIN psychotropic_prescription pp
	ON c.intake_encounter_id = pp.intake_encounter_id
LEFT OUTER JOIN last_visit_location lvl
	ON c.intake_encounter_id = lvl.intake_encounter_id;