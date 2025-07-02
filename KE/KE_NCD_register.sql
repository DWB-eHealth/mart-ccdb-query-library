-- The first CTEs build the frame for patients entering and exiting the cohort. This frame is based on NCD forms with visit types of 'initial visit' and 'patient outcome'. The query takes all initial visit dates and matches patient outcome visit dates if the patient outcome visit date falls between the initial visit date and the next initial visit date (if present).
WITH initial AS (
	SELECT 
		patient_id, encounter_id AS initial_encounter_id, visit_location AS initial_visit_location, date_of_visit AS initial_visit_date, DENSE_RANK () OVER (PARTITION BY patient_id ORDER BY date_of_visit) AS initial_visit_order, LEAD (date_of_visit) OVER (PARTITION BY patient_id ORDER BY date_of_visit) AS next_initial_visit_date
	FROM ncd_consultations WHERE visit_type = 'Initial visit'),
cohort AS (
	SELECT
		i.patient_id, i.initial_encounter_id, i.initial_visit_location, i.initial_visit_date, CASE WHEN i.initial_visit_order > 1 THEN 'Yes' END readmission, d.encounter_id AS outcome_encounter_id, d.outcome_date2 AS outcome_date, d.patient_outcome_at_end_of_msf_care AS patient_outcome 
	FROM initial i
	LEFT JOIN (SELECT patient_id, encounter_id, date_of_visit::date AS outcome_date2, patient_outcome_at_end_of_msf_care FROM ncd_consultations WHERE visit_type = 'Patient outcome') d 
		ON i.patient_id = d.patient_id AND (d.outcome_date2 IS NULL OR (d.outcome_date2 >= i.initial_visit_date AND (d.outcome_date2 < i.next_initial_visit_date OR i.next_initial_visit_date IS NULL)))),
-- The NCD diagnosis CTEs extract all NCD diagnoses for patients reported between their initial visit and discharge visit. Diagnoses are only reported once. For specific disease groups, the second CTE extracts only the last reported diagnosis among the groups. These groups include types of diabetes, types of epilespy, and hypertension stages. The next CTE pivotes the diagnoses horizontally. The final CTE extracts a list of all diagnoses reported for each patient.
cohort_diagnosis AS (
	SELECT
		c.patient_id, c.initial_encounter_id, COALESCE(d.date_of_diagnosis, n.date_of_visit) AS date_of_diagnosis, d.ncd_cohort_diagnosis AS diagnosis
	FROM ncd_consultations_ncd_diagnosis d 
	LEFT JOIN ncd_consultations n USING(encounter_id)
	LEFT JOIN cohort c ON d.patient_id = c.patient_id AND c.initial_visit_date <= n.date_of_visit AND COALESCE(c.outcome_date::date, CURRENT_DATE) >= n.date_of_visit),
cohort_diagnosis_last AS (
    SELECT
        patient_id, initial_encounter_id, diagnosis, date_of_diagnosis
    FROM (
        SELECT
            cdg.*,
            ROW_NUMBER() OVER (PARTITION BY patient_id, initial_encounter_id, diagnosis_group ORDER BY date_of_diagnosis DESC) AS rn
        FROM (
            SELECT
                cd.*,
                CASE
                    WHEN diagnosis IN ('Asthma', 'Chronic obstructive pulmonary disease', 'Sickle Cell Disease') THEN 'Group1'
                    WHEN diagnosis IN ('Diabetes mellitus, type 1', 'Diabetes mellitus, type 2') THEN 'Group2'
                    WHEN diagnosis IN ('Focal epilepsy', 'Generalised epilepsy', 'Unclassified epilepsy') THEN 'Group3'
					WHEN diagnosis IN ('Hypertension Stage 1', 'Hypertension Stage 2', 'Hypertension Stage 3') THEN 'Group4'
                    ELSE 'Other'
                END AS diagnosis_group
            FROM cohort_diagnosis cd) cdg) foo
    WHERE rn = 1),
ncd_diagnosis_pivot AS (
	SELECT 
		DISTINCT ON (initial_encounter_id, patient_id) initial_encounter_id, 
		patient_id,
		MAX (CASE WHEN diagnosis = 'Asthma' THEN date_of_diagnosis ELSE NULL END) AS asthma,
		MAX (CASE WHEN diagnosis = 'Sickle Cell Disease' THEN date_of_diagnosis ELSE NULL END) AS sickle_cell_disease,
		MAX (CASE WHEN diagnosis = 'Chronic obstructive pulmonary disease' THEN date_of_diagnosis ELSE NULL END) AS copd,
		MAX (CASE WHEN diagnosis = 'Diabetes mellitus, type 1' THEN date_of_diagnosis ELSE NULL END) AS diabetes_type1,
		MAX (CASE WHEN diagnosis = 'Diabetes mellitus, type 2' THEN date_of_diagnosis ELSE NULL END) AS diabetes_type2,
		MAX (CASE WHEN diagnosis = 'Hypertension Stage 1' THEN date_of_diagnosis ELSE NULL END) AS hypertension_stage1,
		MAX (CASE WHEN diagnosis = 'Hypertension Stage 2' THEN date_of_diagnosis ELSE NULL END) AS hypertension_stage2,
		MAX (CASE WHEN diagnosis = 'Hypertension Stage 3' THEN date_of_diagnosis ELSE NULL END) AS hypertension_stage3,
		MAX (CASE WHEN diagnosis = 'Focal epilepsy' THEN date_of_diagnosis ELSE NULL END) AS focal_epilepsy,
		MAX (CASE WHEN diagnosis = 'Generalised epilepsy' THEN date_of_diagnosis ELSE NULL END) AS generalised_epilepsy,
		MAX (CASE WHEN diagnosis = 'Unclassified epilepsy' THEN date_of_diagnosis ELSE NULL END) AS unclassified_epilepsy
	FROM cohort_diagnosis_last
	GROUP BY initial_encounter_id, patient_id),
ncd_diagnosis_list AS (
	SELECT initial_encounter_id, STRING_AGG(diagnosis, ', ') AS diagnosis_list
	FROM cohort_diagnosis_last
	GROUP BY initial_encounter_id),
-- The other diagnosis CTE extract all other diagnoses for patients reported between their initial visit and discharge visit and pivots the data horizontally. For all other diagnosis reported, the last date reported is presented in the pivoted data.
other_diagnosis_pivot AS (
	SELECT 
		DISTINCT ON (initial_encounter_id, patient_id) initial_encounter_id, 
		patient_id,
		MAX (CASE WHEN other_diagnosis = 'Tuberculosis' THEN date_of_other_diagnosis ELSE NULL END) AS tuberculosis,
		MAX (CASE WHEN other_diagnosis = 'Hyperthyroidism' THEN date_of_other_diagnosis ELSE NULL END) AS hyperthyroidism,
		MAX (CASE WHEN other_diagnosis = 'Hypothyroidism' THEN date_of_other_diagnosis ELSE NULL END) AS hypothyroidism,
		MAX (CASE WHEN other_diagnosis = 'Arthritis' THEN date_of_other_diagnosis ELSE NULL END) AS arthritis,
		MAX (CASE WHEN other_diagnosis = 'Liver cirrhosis' THEN date_of_other_diagnosis ELSE NULL END) AS liver_cirrhosis,
		MAX (CASE WHEN other_diagnosis LIKE 'Parkinson' THEN date_of_other_diagnosis ELSE NULL END) AS parkinsons_disease,
		MAX (CASE WHEN other_diagnosis = 'Neoplasm/Cancer' THEN date_of_other_diagnosis ELSE NULL END) AS neoplasm_cancer,
		MAX (CASE WHEN other_diagnosis = 'Benign Prostatic Hyperplasia (BPH)' THEN date_of_other_diagnosis ELSE NULL END) AS bph,
		MAX (CASE WHEN other_diagnosis = 'Malaria' THEN date_of_other_diagnosis ELSE NULL END) AS malaria,
		MAX (CASE WHEN other_diagnosis = 'Other' THEN date_of_other_diagnosis ELSE NULL END) AS other_diagnosis
	FROM (
		SELECT
			c.patient_id, c.initial_encounter_id, COALESCE(d.date_of_other_diagnosis, n.date_of_visit) AS date_of_other_diagnosis, d.other_diagnosis
		FROM ncd_consultations_other_diagnosis d 
		LEFT JOIN ncd_consultations n USING(encounter_id)
		LEFT JOIN cohort c ON d.patient_id = c.patient_id AND c.initial_visit_date <= n.date_of_visit AND COALESCE(c.outcome_date::date, CURRENT_DATE) >= n.date_of_visit) foo
	GROUP BY initial_encounter_id, patient_id),
-- The complication CTE extract all complications for patients reported between their initial visit and discharge visit and pivots the data horizontally. For all complications reported, the last date reported is presented in the pivoted data.
complication_pivot AS (
	SELECT 
		DISTINCT ON (initial_encounter_id, patient_id) initial_encounter_id, 
		patient_id,
		MAX (CASE WHEN complications_related_to_htn_dm_or_sc_diagnosis = 'Heart failure' THEN date_of_visit ELSE NULL END) AS heart_failure,
		MAX (CASE WHEN complications_related_to_htn_dm_or_sc_diagnosis = 'Renal failure' THEN date_of_visit ELSE NULL END) AS renal_failure,
		MAX (CASE WHEN complications_related_to_htn_dm_or_sc_diagnosis = 'Peripheral Vascular Disease' THEN date_of_visit ELSE NULL END) AS peripheral_vascular_disease,
		MAX (CASE WHEN complications_related_to_htn_dm_or_sc_diagnosis = 'Peripheral Neuropathy' THEN date_of_visit ELSE NULL END) AS peripheral_neuropathy,
		MAX (CASE WHEN complications_related_to_htn_dm_or_sc_diagnosis = 'Visual Impairment' THEN date_of_visit ELSE NULL END) AS visual_impairment,
		MAX (CASE WHEN complications_related_to_htn_dm_or_sc_diagnosis = 'Foot Ulcers/deformities' THEN date_of_visit ELSE NULL END) AS foot_ulcers_deformities,
		MAX (CASE WHEN complications_related_to_htn_dm_or_sc_diagnosis = 'Erectile dysfunction' THEN date_of_visit ELSE NULL END) AS erectile_dysfunction_priapism,
		MAX (CASE WHEN complications_related_to_htn_dm_or_sc_diagnosis = 'Stroke' THEN date_of_visit ELSE NULL END) AS stroke,
		MAX (CASE WHEN complications_related_to_htn_dm_or_sc_diagnosis = 'Vasoocclusive pain crisis' THEN date_of_visit ELSE NULL END) AS vasoocclusive_pain_crisis,
		MAX (CASE WHEN complications_related_to_htn_dm_or_sc_diagnosis = 'Infection' THEN date_of_visit ELSE NULL END) AS infection,
		MAX (CASE WHEN complications_related_to_htn_dm_or_sc_diagnosis = 'Acute chest syndrome' THEN date_of_visit ELSE NULL END) AS acute_chest_syndrome,
		MAX (CASE WHEN complications_related_to_htn_dm_or_sc_diagnosis = 'Splenic sequestration' THEN date_of_visit ELSE NULL END) AS splenic_sequestration,
		MAX (CASE WHEN complications_related_to_htn_dm_or_sc_diagnosis = 'Other acute anemia' THEN date_of_visit ELSE NULL END) AS other_acute_anemia,
		MAX (CASE WHEN complications_related_to_htn_dm_or_sc_diagnosis = 'Osteomyelitis' THEN date_of_visit ELSE NULL END) AS osteomyelitis,
		MAX (CASE WHEN complications_related_to_htn_dm_or_sc_diagnosis = 'Dactylitis' THEN date_of_visit ELSE NULL END) AS dactylitis,
		MAX (CASE WHEN complications_related_to_htn_dm_or_sc_diagnosis = 'Other' THEN date_of_visit ELSE NULL END) AS other_complication,
		MAX (CASE WHEN complications_related_to_htn_dm_or_sc_diagnosis IS NOT NULL THEN date_of_visit ELSE NULL END) AS last_complication_date
	FROM (
		SELECT
			c.patient_id, c.initial_encounter_id, n.date_of_visit, comp.complications_related_to_htn_dm_or_sc_diagnosis
		FROM ncd_consultations_complications_related_to_htn_dm_sc comp 
		LEFT JOIN ncd_consultations n USING(encounter_id)
		LEFT JOIN cohort c 
			ON comp.patient_id = c.patient_id AND c.initial_visit_date <= n.date_of_visit AND COALESCE(c.outcome_date::date, CURRENT_DATE) >= n.date_of_visit) foo
	GROUP BY initial_encounter_id, patient_id),
-- The risk factor CTEs pivot risk factor data horizontally from the NCD form. Only the last risk factors are reported per cohort enrollment are present. 
current_lifestyle_pivot AS (
	SELECT 
		DISTINCT ON (n.encounter_id, n.patient_id, n.date_of_visit::date) n.encounter_id, 
		n.patient_id, 
		n.date_of_visit::date,
		MAX (CASE WHEN cl.current_lifestyle = 'Adequate physical activity' THEN 1 ELSE NULL END) AS adequate_physical_activity,
		MAX (CASE WHEN cl.current_lifestyle = 'Tobacco use' THEN 1 ELSE NULL END) AS tobacco_use,
		MAX (CASE WHEN cl.current_lifestyle = 'Healthy diet' THEN 1 ELSE NULL END) AS healthy_diet,
		MAX (CASE WHEN cl.current_lifestyle = 'Alcohol use' THEN 1 ELSE NULL END) AS alcohol_use
	FROM ncd_consultations n
	LEFT OUTER JOIN current_lifestyle cl 
		ON cl.encounter_id = n.encounter_id AND cl.current_lifestyle IS NOT NULL 
	WHERE cl.current_lifestyle IS NOT NULL 
	GROUP BY n.encounter_id, n.patient_id, n.date_of_visit::date),
last_current_lifestyle AS (
	SELECT 
		DISTINCT ON (c.patient_id, c.initial_encounter_id) c.patient_id,
		c.initial_encounter_id,
		c.initial_visit_date, 
		c.outcome_encounter_id,
		c.outcome_date, 
		clp.date_of_visit::date,
		clp.tobacco_use,
		clp.alcohol_use,
		clp.adequate_physical_activity,
		clp.healthy_diet
	FROM cohort c
	LEFT OUTER JOIN current_lifestyle_pivot clp 
		ON c.patient_id = clp.patient_id AND c.initial_visit_date <= clp.date_of_visit AND COALESCE(c.outcome_date, CURRENT_DATE) >= clp.date_of_visit
	WHERE clp.date_of_visit IS NOT NULL	
	GROUP BY c.patient_id, c.initial_encounter_id, c.initial_visit_date, c.outcome_encounter_id, c.outcome_date, clp.date_of_visit, clp.tobacco_use, clp.alcohol_use, clp.adequate_physical_activity,
		clp.healthy_diet
	ORDER BY c.patient_id, c.initial_encounter_id, c.initial_visit_date, clp.date_of_visit DESC),
-- The hospitalised CTE checks there is a hospitlisation reported in visits taking place in the last 6 months. 
hospitalisation_last_6m AS (
	SELECT DISTINCT ON (c.patient_id, c.initial_encounter_id) c.patient_id,	c.initial_encounter_id, COUNT(n.hospitalised_since_last_visit) AS nb_hospitalised_last_6m, CASE WHEN n.hospitalised_since_last_visit IS NOT NULL THEN 'Yes' ELSE 'No' END AS hospitalised_last_6m
		FROM cohort c
		LEFT OUTER JOIN ncd_consultations n
			ON c.patient_id = n.patient_id AND c.initial_visit_date <= n.date_of_visit::date AND COALESCE(c.outcome_date, CURRENT_DATE) >= n.date_of_visit::date
		WHERE n.hospitalised_since_last_visit = 'Yes' and n.date_of_visit <= current_date and n.date_of_visit >= current_date - interval '6 months'
		GROUP BY c.patient_id, c.initial_encounter_id, n.hospitalised_since_last_visit),
-- The last foot exam CTE extracts the date of the last foot exam performed per cohort enrollment.
last_foot_exam AS (
	SELECT 
		DISTINCT ON (c.patient_id, c.initial_encounter_id) c.patient_id,
		c.initial_encounter_id,
		c.initial_visit_date, 
		c.outcome_encounter_id,
		c.outcome_date, 
		n.foot_assessment_outcome,
		n.date_of_visit::date AS last_foot_exam_date
	FROM cohort c
	LEFT OUTER JOIN ncd_consultations n
		ON c.patient_id = n.patient_id AND c.initial_visit_date <= n.date_of_visit::date AND COALESCE(c.outcome_date, CURRENT_DATE) >= n.date_of_visit::date
	WHERE n.foot_assessment_outcome IS NOT NULL
	ORDER BY c.patient_id, c.initial_encounter_id, n.patient_id, n.date_of_visit::date DESC),
-- The asthma exacerbation level CTE extracts the last asthma exacerbation level reported per cohort enrollment.
asthma_exacerbation AS (
	SELECT 
		DISTINCT ON (c.patient_id, c.initial_encounter_id) c.patient_id,
		c.initial_encounter_id,
		c.initial_visit_date, 
		c.outcome_encounter_id,
		c.outcome_date, 
		n.date_of_visit::date,
		n.asthma_exacerbation_level AS last_asthma_exacerbation_level
	FROM cohort c
	LEFT OUTER JOIN ncd_consultations n
		ON c.patient_id = n.patient_id AND c.initial_visit_date <= n.date_of_visit::date AND COALESCE(c.outcome_date, CURRENT_DATE) >= n.date_of_visit::date
	WHERE n.asthma_exacerbation_level IS NOT NULL
	ORDER BY c.patient_id, c.initial_encounter_id, n.patient_id, n.date_of_visit::date DESC),
-- The COPD exacerbation level CTE extracts the last COPD exacerbation level reported per cohort enrollment.
copd_exacerbation AS (
	SELECT 
		DISTINCT ON (c.patient_id, c.initial_encounter_id) c.patient_id,
		c.initial_encounter_id,
		c.initial_visit_date, 
		c.outcome_encounter_id,
		c.outcome_date, 
		n.date_of_visit::date,
		n.copd_exacerbation_level AS last_copd_exacerbation_level
	FROM cohort c
	LEFT OUTER JOIN ncd_consultations n
		ON c.patient_id = n.patient_id AND c.initial_visit_date <= n.date_of_visit::date AND COALESCE(c.outcome_date, CURRENT_DATE) >= n.date_of_visit::date
	WHERE n.copd_exacerbation_level IS NOT NULL
	ORDER BY c.patient_id, c.initial_encounter_id, n.patient_id, n.date_of_visit::date DESC),
-- The TB x-ray screening CTE extracts the last TB x-ray screening reported per cohort enrollment.
tb_xray AS (
	SELECT 
		DISTINCT ON (c.patient_id, c.initial_encounter_id) c.patient_id,
		c.initial_encounter_id,
		c.initial_visit_date, 
		c.outcome_encounter_id,
		c.outcome_date, 
		n.date_of_visit::date,
		n.tb_xray_screening AS last_tb_xray
	FROM cohort c
	LEFT OUTER JOIN ncd_consultations n
		ON c.patient_id = n.patient_id AND c.initial_visit_date <= n.date_of_visit::date AND COALESCE(c.outcome_date, CURRENT_DATE) >= n.date_of_visit::date
	WHERE n.tb_xray_screening IS NOT NULL
	ORDER BY c.patient_id, c.initial_encounter_id, n.patient_id, n.date_of_visit::date DESC),
-- The TB x-ray screening CTE extracts the last TB x-ray screening reported per cohort enrollment.
tb_symptom AS (
	SELECT 
		DISTINCT ON (c.patient_id, c.initial_encounter_id) c.patient_id,
		c.initial_encounter_id,
		c.initial_visit_date, 
		c.outcome_encounter_id,
		c.outcome_date, 
		n.date_of_visit::date,
		n.tb_symptom_screening AS last_tb_symptom
	FROM cohort c
	LEFT OUTER JOIN ncd_consultations n
		ON c.patient_id = n.patient_id AND c.initial_visit_date <= n.date_of_visit::date AND COALESCE(c.outcome_date, CURRENT_DATE) >= n.date_of_visit::date
	WHERE n.tb_symptom_screening IS NOT NULL
	ORDER BY c.patient_id, c.initial_encounter_id, n.patient_id, n.date_of_visit::date DESC),
-- The last NCD visit CTE extracts the last NCD visit data per cohort enrollment to look at if there are values reported for pregnancy, family planning, hospitalization, missed medication, seizures, or asthma/COPD exacerbations repoted at the last visit. 
last_ncd_form AS (
	SELECT 
		DISTINCT ON (c.patient_id, c.initial_encounter_id) c.patient_id,
		c.initial_encounter_id,
		c.initial_visit_date, 
		c.outcome_encounter_id,
		c.outcome_date, 
		n.encounter_id,
		n.date_of_visit::date AS last_ncd_form_date,
		DATE_PART('day',(now())-(n.date_of_visit::timestamp))::int AS days_since_last_visit,
		n.visit_type AS last_ncd_form_type,
		n.visit_location AS last_ncd_form_location,
		n.type_of_service_package_provided AS last_service_package,
		n.dsdm_group_name_and_number AS last_dsdm_group,
		CASE WHEN n.scheduled_visit = 'No' THEN 'Yes' END AS last_visit_unscheduled,
		CASE WHEN n.currently_pregnant = 'Yes' THEN 'Yes' END AS pregnant_last_visit,
		CASE WHEN n.hospitalised_since_last_visit = 'Yes' THEN 'Yes' END AS hospitalised_last_visit,
		CASE WHEN n.seizure_since_last_visit = 'Yes' THEN 'Yes' END AS seizures_last_visit,
		CASE WHEN n.referred_for_tb_management = 'Yes' THEN 'Yes' END AS referred_tb_last_visit,
		n.patient_s_stability AS last_stability,
		n.provider AS last_provider,
		n.dsdm_visit_status AS last_dsdm_visit_status
	FROM cohort c
	LEFT OUTER JOIN ncd_consultations n
		ON c.patient_id = n.patient_id AND c.initial_visit_date <= n.date_of_visit::date AND COALESCE(c.outcome_date, CURRENT_DATE) >= n.date_of_visit::date
	ORDER BY c.patient_id, c.initial_encounter_id, n.patient_id, n.date_of_visit::date DESC),
-- THe reason unscheduled CTE pivots the reason for unscheduled visit from the last NCD form.
reason_unscheduled AS (
	SELECT
		lcf.initial_encounter_id,
		lcf.patient_id,
		CASE WHEN ruv.reason_for_unscheduled_visit = 'Medical attention' THEN 'Yes' ELSE NULL END AS medical_attention,
		CASE WHEN ruv.reason_for_unscheduled_visit = 'Forgot' THEN 'Yes' ELSE NULL END AS forgot,
		CASE WHEN ruv.reason_for_unscheduled_visit = 'Financial Constraints' THEN 'Yes' ELSE NULL END AS financial_constraints,
		CASE WHEN ruv.reason_for_unscheduled_visit = 'Fear/Anxiety' THEN 'Yes' ELSE NULL END AS fear_anxiety,
		CASE WHEN ruv.reason_for_unscheduled_visit = 'Family/Personal Issues' THEN 'Yes' ELSE NULL END AS family_personal_issues,
		CASE WHEN ruv.reason_for_unscheduled_visit = 'Other Obligations' THEN 'Yes' ELSE NULL END AS other_obligations,
		CASE WHEN ruv.reason_for_unscheduled_visit = 'Stigma' THEN 'Yes' ELSE NULL END AS stigma,
		CASE WHEN ruv.reason_for_unscheduled_visit = 'Myths and misconceptions' THEN 'Yes' ELSE NULL END AS myths_misconceptions,
		CASE WHEN ruv.reason_for_unscheduled_visit = 'Alternative treatment options' THEN 'Yes' ELSE NULL END AS alternative_treatment_options,
		CASE WHEN ruv.reason_for_unscheduled_visit = 'Insecurity' THEN 'Yes' ELSE NULL END AS insecurity,
		CASE WHEN ruv.reason_for_unscheduled_visit = 'Unknown' THEN 'Yes' ELSE NULL END AS unknown
	FROM last_ncd_form lcf
	LEFT OUTER JOIN reason_for_unscheduled_visit ruv USING(encounter_id)),
-- The last BP CTE extracts the last complete blood pressure measurements reported per cohort enrollment. Uses date reported on form. If no date is present, uses date of sample collection. If neither date or date of sample collection are present, results are not considered. The table considers both BP readings and takes the lowest of the two readings. The logic for the lowest reading, first considers the systolic reading and then if the systolic is the same between the two readings, considers the diastolic reading.
last_bp AS (
	SELECT 
		DISTINCT ON (c.patient_id, c.initial_encounter_id) c.patient_id,
		c.initial_encounter_id,
		c.initial_visit_date, 
		c.outcome_encounter_id,
		c.outcome_date, 
		vli.date_vitals_taken AS last_bp_date,
		vli.systolic_blood_pressure AS last_systolic_first_reading,
		vli.diastolic_blood_pressure AS last_diastolic_first_reading,
		vli.systolic_blood_pressure_second_reading AS last_systolic_second_reading,
		vli.diastolic_blood_pressure_second_reading AS last_diastolic_second_reading,
		CASE
			WHEN COALESCE(vli.systolic_blood_pressure, 999) < COALESCE(vli.systolic_blood_pressure_second_reading, 999) THEN vli.systolic_blood_pressure
			WHEN COALESCE(vli.systolic_blood_pressure, 999) > COALESCE(vli.systolic_blood_pressure_second_reading, 999) THEN vli.systolic_blood_pressure_second_reading
			WHEN vli.systolic_blood_pressure = vli.systolic_blood_pressure_second_reading AND COALESCE(vli.diastolic_blood_pressure, 999) < COALESCE(vli.diastolic_blood_pressure_second_reading, 999) THEN vli.systolic_blood_pressure
			WHEN vli.systolic_blood_pressure = vli.systolic_blood_pressure_second_reading AND COALESCE(vli.diastolic_blood_pressure, 999) > COALESCE(vli.diastolic_blood_pressure_second_reading, 999) THEN vli.systolic_blood_pressure_second_reading
			WHEN vli.systolic_blood_pressure = vli.systolic_blood_pressure_second_reading AND vli.diastolic_blood_pressure = vli.diastolic_blood_pressure_second_reading THEN vli.systolic_blood_pressure_second_reading
		END AS last_systolic_lowest,
		CASE
			WHEN COALESCE(vli.systolic_blood_pressure, 999) < COALESCE(vli.systolic_blood_pressure_second_reading, 999) THEN vli.diastolic_blood_pressure
			WHEN COALESCE(vli.systolic_blood_pressure, 999) > COALESCE(vli.systolic_blood_pressure_second_reading, 999) THEN vli.diastolic_blood_pressure_second_reading
			WHEN vli.systolic_blood_pressure = vli.systolic_blood_pressure_second_reading AND COALESCE(vli.diastolic_blood_pressure, 999) < COALESCE(vli.diastolic_blood_pressure_second_reading, 999) THEN vli.diastolic_blood_pressure
			WHEN vli.systolic_blood_pressure = vli.systolic_blood_pressure_second_reading AND COALESCE(vli.diastolic_blood_pressure, 999) > COALESCE(vli.diastolic_blood_pressure_second_reading, 999) THEN vli.diastolic_blood_pressure_second_reading
			WHEN vli.systolic_blood_pressure = vli.systolic_blood_pressure_second_reading AND vli.diastolic_blood_pressure = vli.diastolic_blood_pressure_second_reading THEN vli.diastolic_blood_pressure_second_reading
		END AS last_diastolic_lowest
	FROM cohort c
	LEFT OUTER JOIN vitals_and_laboratory_information vli
		ON c.patient_id = vli.patient_id AND c.initial_visit_date <= vli.date_vitals_taken AND COALESCE(c.outcome_date, CURRENT_DATE) >= vli.date_vitals_taken 
	WHERE vli.date_vitals_taken IS NOT NULL AND ((vli.systolic_blood_pressure IS NOT NULL AND vli.diastolic_blood_pressure IS NOT NULL) OR (vli.systolic_blood_pressure_second_reading IS NOT NULL AND vli.diastolic_blood_pressure_second_reading IS NOT NULL))
	ORDER BY c.patient_id, c.initial_encounter_id, vli.patient_id, vli.date_vitals_taken DESC),
-- The last BMI CTE extracts the last BMI measurement reported per cohort enrollment. Uses date reported on form. If no date is present, uses date of sample collection. If neither date or date of sample collection are present, results are not considered. 
last_bmi AS (
	SELECT 
		DISTINCT ON (c.patient_id, c.initial_encounter_id) c.patient_id,
		c.initial_encounter_id,
		c.initial_visit_date, 
		c.outcome_encounter_id,
		c.outcome_date, 
		vli.date_vitals_taken AS last_bmi_date,
		vli.bmi_kg_m2 AS last_bmi
	FROM cohort c
	LEFT OUTER JOIN vitals_and_laboratory_information vli
		ON c.patient_id = vli.patient_id AND c.initial_visit_date <= vli.date_vitals_taken AND COALESCE(c.outcome_date, CURRENT_DATE) >= vli.date_vitals_taken 
	WHERE vli.date_vitals_taken  IS NOT NULL AND vli.bmi_kg_m2 IS NOT NULL
	ORDER BY c.patient_id, c.initial_encounter_id, vli.patient_id, vli.date_vitals_taken  DESC),
-- The last fasting blood glucose CTE extracts the last fasting blood glucose measurement reported per cohort enrollment. Uses date of sample collection reported on form. If no date of sample collection is present, uses date of form. If neither date or date of sample collection are present, results are not considered. 
last_fbg AS (
	SELECT 
		DISTINCT ON (c.patient_id, c.initial_encounter_id) c.patient_id,
		c.initial_encounter_id,
		c.initial_visit_date, 
		c.outcome_encounter_id,
		c.outcome_date, 
		vli.date_of_sample_collection AS last_fbg_date,
		vli.fasting_blood_glucose AS last_fbg
	FROM cohort c
	LEFT OUTER JOIN vitals_and_laboratory_information vli
		ON c.patient_id = vli.patient_id AND c.initial_visit_date <= vli.date_of_sample_collection AND COALESCE(c.outcome_date, CURRENT_DATE) >= vli.date_of_sample_collection
	WHERE vli.date_of_sample_collection IS NOT NULL AND vli.fasting_blood_glucose IS NOT NULL
	ORDER BY c.patient_id, c.initial_encounter_id, vli.patient_id, vli.date_of_sample_collection DESC),
-- The last HbA1c CTE extracts the last fasting blood glucose measurement reported per cohort enrollment. Uses date of sample collection reported on form. If no date of sample collection is present, uses date of form. If neither date or date of sample collection are present, results are not considered. 
last_hba1c AS (
	SELECT 
		DISTINCT ON (c.patient_id, c.initial_encounter_id) c.patient_id,
		c.initial_encounter_id,
		c.initial_visit_date, 
		c.outcome_encounter_id,
		c.outcome_date, 
		vli.date_of_sample_collection AS last_hba1c_date, 
		vli.hba1c AS last_hba1c
	FROM cohort c
	LEFT OUTER JOIN vitals_and_laboratory_information vli
		ON c.patient_id = vli.patient_id AND c.initial_visit_date <= vli.date_of_sample_collection AND COALESCE(c.outcome_date, CURRENT_DATE) >= vli.date_of_sample_collection
	WHERE vli.date_of_sample_collection IS NOT NULL AND vli.hba1c IS NOT NULL
	ORDER BY c.patient_id, c.initial_encounter_id, vli.patient_id, vli.date_of_sample_collection DESC),
-- The last GFR CTE extracts the last GFR measurement reported per cohort enrollment. Uses date of sample collection reported on form. If no date of sample collection is present, uses date of form. If neither date or date of sample collection are present, results are not considered. 
last_gfr AS (
	SELECT 
		DISTINCT ON (c.patient_id, c.initial_encounter_id) c.patient_id,
		c.initial_encounter_id,
		c.initial_visit_date, 
		c.outcome_encounter_id,
		c.outcome_date, 
		vli.date_of_sample_collection AS last_gfr_date, 
		vli.gfr AS last_gfr
	FROM cohort c
	LEFT OUTER JOIN vitals_and_laboratory_information vli
		ON c.patient_id = vli.patient_id AND c.initial_visit_date <= vli.date_of_sample_collection AND COALESCE(c.outcome_date, CURRENT_DATE) >= vli.date_of_sample_collection
	WHERE vli.date_of_sample_collection IS NOT NULL AND vli.gfr IS NOT NULL
	ORDER BY c.patient_id, c.initial_encounter_id, vli.patient_id, vli.date_of_sample_collection DESC),
-- The last creatinine CTE extracts the last creatinine measurement reported per cohort enrollment. Uses date of sample collection reported on form. If no date of sample collection is present, uses date of form. If neither date or date of sample collection are present, results are not considered. 
last_creatinine AS (
	SELECT 
		DISTINCT ON (c.patient_id, c.initial_encounter_id) c.patient_id,
		c.initial_encounter_id,
		c.initial_visit_date, 
		c.outcome_encounter_id,
		c.outcome_date, 
		vli.date_of_sample_collection AS last_creatinine_date, 
		vli.creatinine AS last_creatinine
	FROM cohort c
	LEFT OUTER JOIN vitals_and_laboratory_information vli
		ON c.patient_id = vli.patient_id AND c.initial_visit_date <= vli.date_of_sample_collection AND COALESCE(c.outcome_date, CURRENT_DATE) >= vli.date_of_sample_collection
	WHERE vli.date_of_sample_collection IS NOT NULL AND vli.creatinine IS NOT NULL
	ORDER BY c.patient_id, c.initial_encounter_id, vli.patient_id, vli.date_of_sample_collection DESC),
-- The last ASAT CTE extracts the last ASAT measurement reported per cohort enrollment. Uses date of sample collection reported on form. If no date of sample collection is present, uses date of form. If neither date or date of sample collection are present, results are not considered. 
last_asat AS (
	SELECT 
		DISTINCT ON (c.patient_id, c.initial_encounter_id) c.patient_id,
		c.initial_encounter_id,
		c.initial_visit_date, 
		c.outcome_encounter_id,
		c.outcome_date, 
		vli.date_of_sample_collection AS last_asat_date, 
		vli.asat AS last_asat
	FROM cohort c
	LEFT OUTER JOIN vitals_and_laboratory_information vli
		ON c.patient_id = vli.patient_id AND c.initial_visit_date <= vli.date_of_sample_collection AND COALESCE(c.outcome_date, CURRENT_DATE) >= vli.date_of_sample_collection
	WHERE vli.date_of_sample_collection IS NOT NULL AND vli.asat IS NOT NULL
	ORDER BY c.patient_id, c.initial_encounter_id, vli.patient_id, vli.date_of_sample_collection DESC),
-- The last ALAT CTE extracts the last ALAT measurement reported per cohort enrollment. Uses date of sample collection reported on form. If no date of sample collection is present, uses date of form. If neither date or date of sample collection are present, results are not considered. 
last_alat AS (
	SELECT 
		DISTINCT ON (c.patient_id, c.initial_encounter_id) c.patient_id,
		c.initial_encounter_id,
		c.initial_visit_date, 
		c.outcome_encounter_id,
		c.outcome_date, 
		vli.date_of_sample_collection AS last_alat_date, 
		vli.alat AS last_alat
	FROM cohort c
	LEFT OUTER JOIN vitals_and_laboratory_information vli
		ON c.patient_id = vli.patient_id AND c.initial_visit_date <= vli.date_of_sample_collection AND COALESCE(c.outcome_date, CURRENT_DATE) >= vli.date_of_sample_collection
	WHERE vli.date_of_sample_collection IS NOT NULL AND vli.alat IS NOT NULL
	ORDER BY c.patient_id, c.initial_encounter_id, vli.patient_id, vli.date_of_sample_collection DESC),
-- The last HIV test CTE extracts the last HIV test result reported per cohort enrollment. Result is either from the NCD consultation form or the Vitals and lab form, which ever test is reported most recently. In the NCD consultation form, the HIV test result and HIV test result date (or visit date is test result date is incomplete) need to both be completed. In the Vitals and Lab form, the HIV test result and HIV test date both need to be complted.
last_hiv AS (
	SELECT 
		DISTINCT ON (c.patient_id, c.initial_encounter_id) c.patient_id,
		c.initial_encounter_id,
		c.initial_visit_date, 
		c.outcome_encounter_id,
		c.outcome_date, 
		vli.date_of_hiv_test AS last_hiv_date, 
		vli.hiv_test_result AS last_hiv,
		vli.form_field_path AS hiv_result_source
	FROM cohort c
	LEFT OUTER JOIN (
		SELECT patient_id, date_of_hiv_test, hiv_test_result, form_field_path FROM vitals_and_laboratory_information WHERE date_of_hiv_test IS NOT NULL AND hiv_test_result IS NOT NULL 
		UNION
		SELECT patient_id, COALESCE(date_last_tested_for_hiv, date_of_visit) AS date_of_hiv_test, hiv_status_at_initial_visit AS hiv_test_result, form_field_path FROM ncd_consultations WHERE COALESCE(date_last_tested_for_hiv, date_of_visit) IS NOT NULL AND hiv_status_at_initial_visit IS NOT NULL) vli
		ON c.patient_id = vli.patient_id AND c.initial_visit_date <= vli.date_of_hiv_test AND COALESCE(c.outcome_date, CURRENT_DATE) >= vli.date_of_hiv_test
	ORDER BY c.patient_id, c.initial_encounter_id, vli.patient_id, vli.date_of_hiv_test DESC)
-- Main query --
SELECT
	pi."Patient_Identifier",
	c.patient_id,
	c.initial_encounter_id,
	pa."patientFileNumber",
	pa."Patient_Unique_ID_CCC_No",
	pa."facilityForArt",
	pdd.age AS age_current,
	CASE 
		WHEN pdd.age::int <= 4 THEN '0-4'
		WHEN pdd.age::int >= 5 AND pdd.age::int <= 14 THEN '05-14'
		WHEN pdd.age::int >= 15 AND pdd.age::int <= 24 THEN '15-24'
		WHEN pdd.age::int >= 25 AND pdd.age::int <= 34 THEN '25-34'
		WHEN pdd.age::int >= 35 AND pdd.age::int <= 44 THEN '35-44'
		WHEN pdd.age::int >= 45 AND pdd.age::int <= 54 THEN '45-54'
		WHEN pdd.age::int >= 55 AND pdd.age::int <= 64 THEN '55-64'
		WHEN pdd.age::int >= 65 THEN '65+'
		ELSE NULL
	END AS age_group_current,
	EXTRACT(YEAR FROM age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy'))) AS age_admission,
	CASE 
		WHEN EXTRACT(YEAR FROM age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')))::int <= 4 THEN '0-4'
		WHEN EXTRACT(YEAR FROM age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')))::int >= 5 AND EXTRACT(YEAR FROM age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy'))) <= 14 THEN '05-14'
		WHEN EXTRACT(YEAR FROM age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')))::int >= 15 AND EXTRACT(YEAR FROM age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy'))) <= 24 THEN '15-24'
		WHEN EXTRACT(YEAR FROM age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')))::int >= 25 AND EXTRACT(YEAR FROM age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy'))) <= 34 THEN '25-34'
		WHEN EXTRACT(YEAR FROM age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')))::int >= 35 AND EXTRACT(YEAR FROM age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy'))) <= 44 THEN '35-44'
		WHEN EXTRACT(YEAR FROM age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')))::int >= 45 AND EXTRACT(YEAR FROM age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy'))) <= 54 THEN '45-54'
		WHEN EXTRACT(YEAR FROM age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')))::int >= 55 AND EXTRACT(YEAR FROM age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy'))) <= 64 THEN '55-64'
		WHEN EXTRACT(YEAR FROM age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')))::int >= 65 THEN '65+'
		ELSE NULL
	END AS age_group_admission,
	pdd.gender,
	pad.city_village AS county,
	pad.state_province AS sub_county,
	pad.county_district AS ward,
	pad.address2 AS village,
	pa."Occupation",
	c.initial_visit_date AS enrollment_date,
	CASE WHEN c.outcome_date IS NULL THEN 'Yes' END AS in_cohort,
	CASE WHEN ((DATE_PART('year', CURRENT_DATE) - DATE_PART('year', c.initial_visit_date)) * 12 + (DATE_PART('month', CURRENT_DATE) - DATE_PART('month', c.initial_visit_date))) >= 6 AND c.outcome_date IS NULL THEN 'Yes' END AS in_cohort_6m,
	CASE WHEN ((DATE_PART('year', CURRENT_DATE) - DATE_PART('year', c.initial_visit_date)) * 12 + (DATE_PART('month', CURRENT_DATE) - DATE_PART('month', c.initial_visit_date))) >= 12 AND c.outcome_date IS NULL THEN 'Yes' END AS in_cohort_12m,
	c.readmission,
	c.initial_visit_location,
	lnf.last_ncd_form_location,
	lnf.last_ncd_form_date,
	lnf.last_ncd_form_type,	
	lnf.last_provider,
	lnf.days_since_last_visit,
	lnf.last_service_package,
	lnf.last_dsdm_group,
	lnf.last_dsdm_visit_status,
	lnf.last_visit_unscheduled,
	-- last dsdm visit and last non-dsdm visit dates, how long been in dsdm if currently in dsdm without normal/facility care
	-- add first DSDM to register
	ru.medical_attention,
	ru.forgot,
	ru.financial_constraints,
	ru.fear_anxiety,
	ru.family_personal_issues,
	ru.other_obligations,
	ru.stigma,
	ru.myths_misconceptions,
	ru.alternative_treatment_options,
	ru.insecurity,
	ru.unknown AS unscheduled_reason_unknown,
	lnf.last_stability,
	c.outcome_date,
	c.patient_outcome,
	lnf.pregnant_last_visit,
	lnf.hospitalised_last_visit,
	lnf.seizures_last_visit,
	-- last time seizures reported
	-- add seizure free in the last 2 years (no seizures reported between today and the last 2 years, enrollment date has to be 2+ years ago)
	lnf.referred_tb_last_visit,
	-- ^ change referred tb last visit to last referral for TB
	h6m.nb_hospitalised_last_6m,
	h6m.hospitalised_last_6m,
	lfe.last_foot_exam_date,
	lfe.foot_assessment_outcome,
	ae.last_asthma_exacerbation_level,
	ce.last_copd_exacerbation_level,
	tx.last_tb_xray,
	ts.last_tb_symptom,
	-- add geneX result, add combined column if any TB screening (xray + symptom + genX) >> case([NCD Consultations - Patient → TB Symptom Screening] = "Yes positive", "Yes", [NCD Consultations - Patient → TB Sputum Screening] = "Yes positive", "Yes", [NCD Consultations - Patient → Tb Xray Screening] = "Yes positive", "Yes", [NCD Consultations - Patient → Tb Genexpert Screening] = "Yes positive", "Yes")
	lbp.last_systolic_first_reading,
	lbp.last_diastolic_first_reading,
	CASE WHEN lbp.last_systolic_first_reading IS NOT NULL AND lbp.last_diastolic_first_reading IS NOT NULL THEN CONCAT(lbp.last_systolic_first_reading,'/',lbp.last_diastolic_first_reading) END AS last_bp_first_reading,
	lbp.last_systolic_second_reading,
	lbp.last_diastolic_second_reading,
	CASE WHEN lbp.last_systolic_second_reading IS NOT NULL AND lbp.last_diastolic_second_reading IS NOT NULL THEN CONCAT(lbp.last_systolic_second_reading,'/',lbp.last_diastolic_second_reading) END AS last_bp_second_reading,
	lbp.last_systolic_lowest,
	lbp.last_diastolic_lowest,
	CASE WHEN lbp.last_systolic_lowest IS NOT NULL AND lbp.last_diastolic_lowest IS NOT NULL THEN CONCAT(lbp.last_systolic_lowest,'/',lbp.last_diastolic_lowest) END AS last_bp_lowest,
	CASE WHEN lbp.last_systolic_lowest <= 140 AND lbp.last_diastolic_lowest <= 90 THEN 'Yes' WHEN lbp.last_systolic_lowest > 140 OR lbp.last_diastolic_lowest > 90 THEN 'No' END AS last_bp_control_htn,
	CASE WHEN lbp.last_systolic_lowest <= 130 AND lbp.last_diastolic_lowest <= 90 THEN 'Yes' WHEN lbp.last_systolic_lowest > 130 OR lbp.last_diastolic_lowest > 90 THEN 'No' END AS last_bp_control_dm,
	lbp.last_bp_date,
	lbmi.last_bmi,
	lbmi.last_bmi_date,
	lfbg.last_fbg,
	lfbg.last_fbg_date,
	-- add baseline HbA1c
	lbg.last_hba1c,
	CASE WHEN lbg.last_hba1c <= 6.5 THEN '0-6.5%' WHEN lbg.last_hba1c BETWEEN 6.6 AND 8 THEN '6.6-8.0%' WHEN lbg.last_hba1c > 8 THEN '>8%' END AS last_hba1c_grouping, 
	lbg.last_hba1c_date,
	CASE WHEN lbg.last_hba1c < 8 THEN 'Yes' WHEN lbg.last_hba1c >= 8 THEN 'No' WHEN lbg.last_hba1c IS NULL AND lfbg.last_fbg < 150 THEN 'Yes' WHEN lbg.last_hba1c IS NULL AND lfbg.last_fbg >= 150 THEN 'No' END AS diabetes_control,
	lgfr.last_gfr,
	lgfr.last_gfr_date,
	CASE WHEN lgfr.last_gfr < 30 THEN 'Yes' WHEN lgfr.last_gfr >= 30 THEN 'No' END AS gfr_control,
	lc.last_creatinine,
	lc.last_creatinine_date,
	lal.last_alat,
	lal.last_alat_date,
	las.last_asat,
	las.last_asat_date,
	lh.last_hiv,
	lh.last_hiv_date,
	lh.hiv_result_source,
	ndx.asthma,
	ndx.sickle_cell_disease,
	ndx.copd,
	ndx.diabetes_type1,
	ndx.diabetes_type2,
	CASE WHEN ndx.diabetes_type1 IS NOT NULL OR ndx.diabetes_type2 IS NOT NULL THEN 1 END AS diabetes_any,
	ndx.hypertension_stage1,
	ndx.hypertension_stage2,
	ndx.hypertension_stage3,
	CASE WHEN ndx.hypertension_stage1 IS NOT NULL OR ndx.hypertension_stage2 IS NOT NULL OR ndx.hypertension_stage3 IS NOT NULL THEN 1 END AS hypertension_any,
	ndx.focal_epilepsy,
	ndx.generalised_epilepsy,
	ndx.unclassified_epilepsy,
	ndl.diagnosis_list,
	odp.tuberculosis,
	odp.hyperthyroidism,
	odp.hypothyroidism,
	odp.arthritis,
	odp.liver_cirrhosis,
	odp.parkinsons_disease,
	odp.neoplasm_cancer,
	odp.bph,
	odp.malaria,
	odp.other_diagnosis,
	cp.heart_failure,
	cp.renal_failure,
	cp.peripheral_vascular_disease,
	cp.peripheral_neuropathy,
	cp.visual_impairment,
	cp.foot_ulcers_deformities,
	cp.erectile_dysfunction_priapism,
	cp.stroke,
	cp.vasoocclusive_pain_crisis,
	cp.infection,
	cp.acute_chest_syndrome,
	cp.splenic_sequestration,
	cp.other_acute_anemia,
	cp.osteomyelitis,
	cp.dactylitis,
	cp.other_complication,
	cp.last_complication_date,
	lrf.tobacco_use,
	lrf.alcohol_use,
	lrf.adequate_physical_activity,
	lrf.healthy_diet
	--add 2 columns for medications (ever prescribed, currently active)
FROM cohort c
LEFT OUTER JOIN patient_identifier pi
	ON c.patient_id = pi.patient_id
LEFT OUTER JOIN person_attributes pa
	ON c.patient_id = pa.person_id
LEFT OUTER JOIN person_details_default pdd 
	ON c.patient_id = pdd.person_id
LEFT OUTER JOIN person_address_default pad 
	ON c.patient_id = pad.person_id
LEFT OUTER JOIN patient_encounter_details_default ped 
	ON c.initial_encounter_id = ped.encounter_id
LEFT OUTER JOIN ncd_diagnosis_pivot ndx
	ON c.initial_encounter_id = ndx.initial_encounter_id
LEFT OUTER JOIN ncd_diagnosis_list ndl
	ON c.initial_encounter_id = ndl.initial_encounter_id
LEFT OUTER JOIN other_diagnosis_pivot odp 
	ON c.initial_encounter_id = odp.initial_encounter_id
LEFT OUTER JOIN complication_pivot cp 
	ON c.initial_encounter_id = cp.initial_encounter_id
LEFT OUTER JOIN last_current_lifestyle lrf
	ON c.initial_encounter_id = lrf.initial_encounter_id
LEFT OUTER JOIN last_ncd_form lnf
	ON c.initial_encounter_id = lnf.initial_encounter_id
LEFT OUTER JOIN reason_unscheduled ru
	ON c.initial_encounter_id = ru.initial_encounter_id
LEFT OUTER JOIN hospitalisation_last_6m h6m
	ON c.initial_encounter_id = h6m.initial_encounter_id
LEFT OUTER JOIN last_foot_exam lfe
	ON c.initial_encounter_id = lfe.initial_encounter_id
LEFT OUTER JOIN asthma_exacerbation ae
	ON c.initial_encounter_id = ae.initial_encounter_id
LEFT OUTER JOIN copd_exacerbation ce 
	ON c.initial_encounter_id = ce.initial_encounter_id
LEFT OUTER JOIN tb_xray tx
	ON c.initial_encounter_id = tx.initial_encounter_id
LEFT OUTER JOIN tb_symptom ts
	ON c.initial_encounter_id = ts.initial_encounter_id
LEFT OUTER JOIN last_bp lbp
	ON c.initial_encounter_id = lbp.initial_encounter_id
LEFT OUTER JOIN last_bmi lbmi
	ON c.initial_encounter_id = lbmi.initial_encounter_id
LEFT OUTER JOIN last_fbg lfbg
	ON c.initial_encounter_id = lfbg.initial_encounter_id
LEFT OUTER JOIN last_hba1c lbg
	ON c.initial_encounter_id = lbg.initial_encounter_id
LEFT OUTER JOIN last_gfr lgfr
	ON c.initial_encounter_id = lgfr.initial_encounter_id
LEFT OUTER JOIN last_creatinine lc
	ON c.initial_encounter_id = lc.initial_encounter_id
LEFT OUTER JOIN last_alat lal
	ON c.initial_encounter_id = lal.initial_encounter_id
LEFT OUTER JOIN last_asat las
	ON c.initial_encounter_id = las.initial_encounter_id
LEFT OUTER JOIN last_hiv lh
	ON c.initial_encounter_id = lh.initial_encounter_id;