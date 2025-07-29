-- *COHORT FRAME
-- The cohort CTE builds the enrollment window for patients entering and exiting the NCD cohort. The enrollment window is based on NCD forms with visit types of 'initial visit' and 'patient outcome', matching all initial visit forms to the cooresponding patient outcome visit form, if present. If an individual patient has more than one initial visit form, then the patient outcome visit form will be matched to the initial visit form where the patient outcome visit form date falls between the first initial visit date and the following initial visit date.
WITH cohort AS (
	SELECT
		i.patient_id,
		i.initial_encounter_id,
		i.initial_visit_location,
		i.initial_visit_date,
		CASE
			WHEN i.initial_visit_order > 1 THEN 'Yes'
		END readmission,
		d.encounter_id AS outcome_encounter_id,
		d.outcome_date2 AS outcome_date,
		d.patient_outcome_at_end_of_msf_care AS patient_outcome
	FROM
		(
			SELECT
				patient_id,
				encounter_id AS initial_encounter_id,
				visit_location AS initial_visit_location,
				date_of_visit AS initial_visit_date,
				DENSE_RANK () OVER (
					PARTITION BY patient_id
					ORDER BY
						date_of_visit
				) AS initial_visit_order,
				LEAD (date_of_visit) OVER (
					PARTITION BY patient_id
					ORDER BY
						date_of_visit
				) AS next_initial_visit_date
			FROM
				ncd_consultations
			WHERE
				visit_type = 'Initial visit'
		) i
		LEFT JOIN (
			SELECT
				patient_id,
				encounter_id,
				date_of_visit AS outcome_date2,
				patient_outcome_at_end_of_msf_care
			FROM
				ncd_consultations
			WHERE
				visit_type = 'Patient outcome'
		) d ON i.patient_id = d.patient_id
		AND (
			d.outcome_date2 IS NULL
			OR (
				d.outcome_date2 >= i.initial_visit_date
				AND (
					d.outcome_date2 < i.next_initial_visit_date
					OR i.next_initial_visit_date IS NULL
				)
			)
		)
),
-- *VISIT INFORMATION: The visit tables extract and organize visit information reported during the patient's enrollment window (defined by the cohort CTE). 
-- The visits_ordered CTE extracts the NCD visit information and ranks the visits in order, matching prior and following service packages. 
visits_ordered AS (
	SELECT
		n.patient_id,
		c.initial_encounter_id,
		n.encounter_id,
		n.date_of_visit,
		n.visit_type,
		n.visit_location,
		n.type_of_service_package_provided AS service_package,
		LAG(n.type_of_service_package_provided) OVER (
			PARTITION BY c.initial_encounter_id
			ORDER BY
				n.date_of_visit
		) AS prev_service_package,
		LEAD(n.type_of_service_package_provided) OVER (
			PARTITION BY c.initial_encounter_id
			ORDER BY
				n.date_of_visit
		) AS next_service_package,
		n.dsdm_group_name_and_number,
		CASE
			WHEN n.scheduled_visit = 'No' THEN 'Yes'
		END AS visit_unscheduled,
		CASE
			WHEN n.currently_pregnant = 'Yes' THEN 'Yes'
		END AS pregnant,
		CASE
			WHEN n.hospitalised_since_last_visit = 'Yes' THEN 'Yes'
		END AS hospitalised,
		CASE
			WHEN n.seizure_since_last_visit = 'Yes' THEN 'Yes'
		END AS seizures,
		n.foot_assessment_outcome,
		n.tb_symptom_screening,
		n.tb_sputum_screening,
		n.tb_xray_screening,
		n.tb_genexpert_screening,
		n.referred_for_tb_management,
		n.asthma_exacerbation_level,
		n.copd_exacerbation_level,
		n.patient_s_stability,
		n.hiv_status_at_initial_visit,
		n.date_last_tested_for_hiv,
		n.provider,
		n.dsdm_visit_status,
		n.date_of_next_appointment,
		ROW_NUMBER() OVER (
			PARTITION BY c.initial_encounter_id
			ORDER BY
				n.date_of_visit DESC
		) AS rn_desc,
		ROW_NUMBER() OVER (
			PARTITION BY c.initial_encounter_id
			ORDER BY
				n.date_of_visit
		) AS rn_asc
	FROM
		ncd_consultations n
		LEFT OUTER JOIN cohort c ON n.patient_id = c.patient_id
		AND n.date_of_visit >= c.initial_visit_date
		AND n.date_of_visit <= COALESCE(c.outcome_date, CURRENT_DATE)
),
-- The reason_unscheduled_list CTE combines the last reasons for unscheduled visit into a single string, separated by commas.
reason_unscheduled_list AS (
	SELECT
		vo.initial_encounter_id,
		STRING_AGG(
			ruv.reason_for_unscheduled_visit,
			', '
			ORDER BY
				ruv.reason_for_unscheduled_visit
		) AS reason_unscheduled
	FROM
		visits_ordered vo
		LEFT JOIN reason_for_unscheduled_visit ruv USING(encounter_id)
	WHERE
		vo.rn_desc = 1
	GROUP BY
		vo.initial_encounter_id
),
-- The lifestyle_list CTEs combines the last lifestyle information into a single string, separated by commas.
current_lifestyle_list AS (
	SELECT
		vo.initial_encounter_id,
		STRING_AGG(
			cl.current_lifestyle,
			', '
			ORDER BY
				cl.current_lifestyle
		) AS current_lifestyle_list
	FROM
		visits_ordered vo
		LEFT JOIN current_lifestyle cl USING(encounter_id)
	WHERE
		vo.rn_desc = 1
	GROUP BY
		vo.initial_encounter_id
),
-- The dsd_status CTE extracts and organizes visit information related to the DSD program.
dsd_status AS (
	SELECT
		initial_encounter_id,
		MAX(
			CASE
				WHEN rn_desc = 1
				AND visit_type != 'Patient outcome' THEN service_package
				WHEN rn_desc = 1
				AND visit_type = 'Patient outcome' THEN prev_service_package
			END
		) AS latest_service_package,
		BOOL_OR(
			service_package IN (
				'Standard of Care',
				'Facility Fast-Track',
				'Facility based DSDM',
				'Community based DSDM'
			)
		) AS ever_dsd,
		BOOL_OR(
			prev_service_package IN (
				'Standard of Care',
				'Facility Fast-Track',
				'Facility based DSDM',
				'Community based DSDM'
			)
			AND service_package = 'Routine Care Visit'
			AND next_service_package IN (
				'Standard of Care',
				'Facility Fast-Track',
				'Facility based DSDM',
				'Community based DSDM'
			)
		) AS interrupted_dsd
	FROM
		visits_ordered
	GROUP BY
		initial_encounter_id
),
-- The current_dsd_streak CTE identifies the last start date for a patient who are recieve the DSD service package.
current_dsd_streak AS (
	SELECT
		vo.initial_encounter_id,
		MIN(vo.date_of_visit) AS current_dsd_start_date
	FROM
		visits_ordered vo
		JOIN dsd_status ds ON vo.initial_encounter_id = ds.initial_encounter_id
	WHERE
		vo.service_package IN (
			'Standard of Care',
			'Facility Fast-Track',
			'Facility based DSDM',
			'Community based DSDM'
		)
		AND ds.latest_service_package IN (
			'Standard of Care',
			'Facility Fast-Track',
			'Facility based DSDM',
			'Community based DSDM'
		)
		AND NOT EXISTS (
			SELECT
				1
			FROM
				ncd_consultations n
			WHERE
				n.patient_id = vo.patient_id
				AND n.date_of_visit > vo.date_of_visit
				AND n.type_of_service_package_provided = 'Routine Care Visit'
		)
	GROUP BY
		vo.initial_encounter_id
),
-- *DISAGNOSIS AND COMPLICATIONS: The NCD diagnosis and compliation CTEs extract and organize diagnosis, other diagnosis, and complication information reported during the patient's enrollment window (defined by the cohort CTE). 
-- In the cohort_diagnosis CTE, each diagnosis is only counted once per patient and the date for the diagnosis reported is the earliest date that the diagnosis is made. For specific diagnosis groups, only the last reported diagnosis is extraCTEd. These groups are diabetes (includeing Diabetes mellitus, type 1 and Diabetes mellitus, type 2), epilepsy (including Focal epilepsy, Generalised epilepsy, Unclassified epilepsy), and hypertension (including Hypertension Stage 1, Hypertension Stage 2, Hypertension Stage 3).
cohort_diagnosis AS (
	SELECT
		patient_id,
		initial_encounter_id,
		diagnosis,
		MIN(date_of_diagnosis) AS date_of_diagnosis
	FROM
		(
			SELECT
				cd.patient_id,
				cd.initial_encounter_id,
				cd.diagnosis,
				cd.date_of_diagnosis,
				CASE
					WHEN cd.diagnosis_group IN ('Group2', 'Group3', 'Group4') THEN FIRST_VALUE(cd.diagnosis) OVER (
						PARTITION BY cd.patient_id,
						cd.initial_encounter_id,
						cd.diagnosis_group
						ORDER BY
							cd.date_of_diagnosis DESC
					)
					ELSE cd.diagnosis
				END AS selected_diagnosis
			FROM
				(
					SELECT
						c.patient_id,
						c.initial_encounter_id,
						COALESCE(d.date_of_diagnosis, n.date_of_visit) AS date_of_diagnosis,
						d.ncd_cohort_diagnosis AS diagnosis,
						CASE
							WHEN d.ncd_cohort_diagnosis IN (
								'Asthma',
								'Chronic obstructive pulmonary disease',
								'Sickle Cell Disease'
							) THEN 'Group1'
							WHEN d.ncd_cohort_diagnosis IN (
								'Diabetes mellitus, type 1',
								'Diabetes mellitus, type 2'
							) THEN 'Group2'
							WHEN d.ncd_cohort_diagnosis IN (
								'Focal epilepsy',
								'Generalised epilepsy',
								'Unclassified epilepsy'
							) THEN 'Group3'
							WHEN d.ncd_cohort_diagnosis IN (
								'Hypertension Stage 1',
								'Hypertension Stage 2',
								'Hypertension Stage 3'
							) THEN 'Group4'
							ELSE 'Other'
						END AS diagnosis_group
					FROM
						ncd_consultations_ncd_diagnosis d
						LEFT JOIN ncd_consultations n USING(encounter_id)
						JOIN cohort c ON d.patient_id = c.patient_id
						AND c.initial_visit_date <= n.date_of_visit
						AND COALESCE(c.outcome_date, CURRENT_DATE) >= n.date_of_visit
				) cd
		) ranked
	WHERE
		diagnosis = selected_diagnosis
	GROUP BY
		patient_id,
		initial_encounter_id,
		diagnosis
),
-- The ncd_diagnosis_pivot CTE pivots the diagnoses reported in the cohort_diagnosis CTE horizontally. The pivoted data includes the earliest date of diagnosis for each diagnosis reported. If a diagnosis is not reported, the date will be NULL.
ncd_diagnosis_pivot AS (
	SELECT
		initial_encounter_id,
		patient_id,
		MAX (
			CASE
				WHEN diagnosis = 'Asthma' THEN date_of_diagnosis
			END
		) AS asthma,
		MAX (
			CASE
				WHEN diagnosis = 'Sickle Cell Disease' THEN date_of_diagnosis
			END
		) AS sickle_cell_disease,
		MAX (
			CASE
				WHEN diagnosis = 'Chronic obstructive pulmonary disease' THEN date_of_diagnosis
			END
		) AS copd,
		MAX (
			CASE
				WHEN diagnosis = 'Diabetes mellitus, type 1' THEN date_of_diagnosis
			END
		) AS diabetes_type1,
		MAX (
			CASE
				WHEN diagnosis = 'Diabetes mellitus, type 2' THEN date_of_diagnosis
			END
		) AS diabetes_type2,
		MAX (
			CASE
				WHEN diagnosis = 'Hypertension Stage 1' THEN date_of_diagnosis
			END
		) AS hypertension_stage1,
		MAX (
			CASE
				WHEN diagnosis = 'Hypertension Stage 2' THEN date_of_diagnosis
			END
		) AS hypertension_stage2,
		MAX (
			CASE
				WHEN diagnosis = 'Hypertension Stage 3' THEN date_of_diagnosis
			END
		) AS hypertension_stage3,
		MAX (
			CASE
				WHEN diagnosis = 'Focal epilepsy' THEN date_of_diagnosis
			END
		) AS focal_epilepsy,
		MAX (
			CASE
				WHEN diagnosis = 'Generalised epilepsy' THEN date_of_diagnosis
			END
		) AS generalised_epilepsy,
		MAX (
			CASE
				WHEN diagnosis = 'Unclassified epilepsy' THEN date_of_diagnosis
			END
		) AS unclassified_epilepsy
	FROM
		cohort_diagnosis
	GROUP BY
		initial_encounter_id,
		patient_id
),
-- The ncd_diagnosis_list CTE combines all diagnosis from in the cohort_diagnosis CTE into a single string, separated by commas.
ncd_diagnosis_list AS (
	SELECT
		initial_encounter_id,
		STRING_AGG(
			diagnosis,
			', '
			ORDER BY
				diagnosis
		) AS diagnosis_list
	FROM
		cohort_diagnosis
	GROUP BY
		initial_encounter_id
),
-- The other_diagnosis_list CTE combines other diagnoses ever reported into a single string, separated by commas.
other_diagnosis_list AS (
	SELECT
		initial_encounter_id,
		STRING_AGG(
			other_diagnosis,
			', '
			ORDER BY
				other_diagnosis
		) AS other_diagnosis_list
	FROM
		(
			SELECT
				c.patient_id,
				c.initial_encounter_id,
				COALESCE(d.date_of_other_diagnosis, n.date_of_visit) AS date_of_diagnosis,
				d.other_diagnosis
			FROM
				ncd_consultations_other_diagnosis d
				LEFT JOIN ncd_consultations n USING(encounter_id)
				JOIN cohort c ON d.patient_id = c.patient_id
				AND c.initial_visit_date <= n.date_of_visit
				AND COALESCE(c.outcome_date :: DATE, CURRENT_DATE) >= n.date_of_visit
		) od
	GROUP BY
		initial_encounter_id
),
-- The complication_pivot CTE pivots complications horizontally and reports the last visit date where each complication was reported is presented.
complication_pivot AS (
	SELECT
		initial_encounter_id,
		patient_id,
		MAX (
			CASE
				WHEN complication = 'Heart failure' THEN date_of_visit
				ELSE NULL
			END
		) AS heart_failure,
		MAX (
			CASE
				WHEN complication = 'Renal failure' THEN date_of_visit
				ELSE NULL
			END
		) AS renal_failure,
		MAX (
			CASE
				WHEN complication = 'Peripheral Vascular Disease' THEN date_of_visit
				ELSE NULL
			END
		) AS peripheral_vascular_disease,
		MAX (
			CASE
				WHEN complication = 'Peripheral Neuropathy' THEN date_of_visit
				ELSE NULL
			END
		) AS peripheral_neuropathy,
		MAX (
			CASE
				WHEN complication = 'Visual Impairment' THEN date_of_visit
				ELSE NULL
			END
		) AS visual_impairment,
		MAX (
			CASE
				WHEN complication = 'Foot Ulcers/deformities' THEN date_of_visit
				ELSE NULL
			END
		) AS foot_ulcers_deformities,
		MAX (
			CASE
				WHEN complication = 'Erectile dysfunction' THEN date_of_visit
				ELSE NULL
			END
		) AS erectile_dysfunction_priapism,
		MAX (
			CASE
				WHEN complication = 'Stroke' THEN date_of_visit
				ELSE NULL
			END
		) AS stroke,
		MAX (
			CASE
				WHEN complication = 'Vasoocclusive pain crisis' THEN date_of_visit
				ELSE NULL
			END
		) AS vasoocclusive_pain_crisis,
		MAX (
			CASE
				WHEN complication = 'Infection' THEN date_of_visit
				ELSE NULL
			END
		) AS infection,
		MAX (
			CASE
				WHEN complication = 'Acute chest syndrome' THEN date_of_visit
				ELSE NULL
			END
		) AS acute_chest_syndrome,
		MAX (
			CASE
				WHEN complication = 'Splenic sequestration' THEN date_of_visit
				ELSE NULL
			END
		) AS splenic_sequestration,
		MAX (
			CASE
				WHEN complication = 'Other acute anemia' THEN date_of_visit
				ELSE NULL
			END
		) AS other_acute_anemia,
		MAX (
			CASE
				WHEN complication = 'Osteomyelitis' THEN date_of_visit
				ELSE NULL
			END
		) AS osteomyelitis,
		MAX (
			CASE
				WHEN complication = 'Dactylitis' THEN date_of_visit
				ELSE NULL
			END
		) AS dactylitis,
		MAX (
			CASE
				WHEN complication = 'Other' THEN date_of_visit
				ELSE NULL
			END
		) AS other_complication,
		MAX (
			CASE
				WHEN complication IS NOT NULL THEN date_of_visit
				ELSE NULL
			END
		) AS last_complication_date
	FROM
		(
			SELECT
				c.patient_id,
				c.initial_encounter_id,
				n.date_of_visit,
				comp.complications_related_to_htn_dm_or_sc_diagnosis AS complication
			FROM
				ncd_consultations_complications_related_to_htn_dm_sc comp
				LEFT JOIN ncd_consultations n USING(encounter_id)
				JOIN cohort c ON comp.patient_id = c.patient_id
				AND c.initial_visit_date <= n.date_of_visit
				AND COALESCE(c.outcome_date :: DATE, CURRENT_DATE) >= n.date_of_visit
		) comp
	GROUP BY
		initial_encounter_id,
		patient_id
),
-- The complication_list CTE lists all complications ever reported for each patient (even is some complications have been resolved).
complication_list AS (
	SELECT
		initial_encounter_id,
		STRING_AGG(
			complication,
			', '
			ORDER BY
				complication
		) AS complication_list
	FROM
		(
			SELECT
				c.patient_id,
				c.initial_encounter_id,
				n.date_of_visit,
				comp.complications_related_to_htn_dm_or_sc_diagnosis AS complication
			FROM
				ncd_consultations_complications_related_to_htn_dm_sc comp
				LEFT JOIN ncd_consultations n USING(encounter_id)
				JOIN cohort c ON comp.patient_id = c.patient_id
				AND c.initial_visit_date <= n.date_of_visit
				AND COALESCE(c.outcome_date :: DATE, CURRENT_DATE) >= n.date_of_visit
		) comp
	GROUP BY
		initial_encounter_id
),
-- The complication_list_baseline CTE lists all complications reported at the initial visit for each patient.
complication_list_baseline AS (
	SELECT
		initial_encounter_id,
		STRING_AGG(
			complication,
			', '
			ORDER BY
				complication
		) AS complication_list
	FROM
		(
			SELECT
				c.patient_id,
				c.initial_encounter_id,
				n.date_of_visit,
				comp.complications_related_to_htn_dm_or_sc_diagnosis AS complication
			FROM
				ncd_consultations_complications_related_to_htn_dm_sc comp
				LEFT JOIN ncd_consultations n USING(encounter_id)
				JOIN cohort c ON comp.patient_id = c.patient_id
				AND c.initial_visit_date <= n.date_of_visit
				AND COALESCE(c.outcome_date :: DATE, CURRENT_DATE) >= n.date_of_visit
			WHERE
				n.visit_type = 'Initial visit'
		) comp_bl
	GROUP BY
		initial_encounter_id
),
-- *BASELINE: The baseline CTEs extract and organize information recorded at the initial visit or at the start of the patient care in the cohort reported during the patient's enrollment window (defined by the cohort CTE). 
-- The first_bp CTE extracts the first completed blood pressure measurement. Date is determined by the vitals date of the vitals and laboratory form. If no vitals date is present, results are not considered. The table considers both BP readings and takes the lowest of the two readings. The logic for the lowest reading, first considers the systolic reading and then if the systolic is the same between the two readings, considers the diastolic reading.
first_bp AS (
	SELECT
		initial_encounter_id,
		first_bp_date,
		systolic_blood_pressure AS first_systolic_first_reading,
		diastolic_blood_pressure AS first_diastolic_first_reading,
		systolic_blood_pressure_second_reading AS first_systolic_second_reading,
		diastolic_blood_pressure_second_reading AS first_diastolic_second_reading,
		CASE
			WHEN COALESCE(systolic_blood_pressure, 999) < COALESCE(systolic_blood_pressure_second_reading, 999) THEN systolic_blood_pressure
			WHEN COALESCE(systolic_blood_pressure, 999) > COALESCE(systolic_blood_pressure_second_reading, 999) THEN systolic_blood_pressure_second_reading
			WHEN systolic_blood_pressure = systolic_blood_pressure_second_reading
			AND COALESCE(diastolic_blood_pressure, 999) < COALESCE(diastolic_blood_pressure_second_reading, 999) THEN systolic_blood_pressure
			WHEN systolic_blood_pressure = systolic_blood_pressure_second_reading
			AND COALESCE(diastolic_blood_pressure, 999) > COALESCE(diastolic_blood_pressure_second_reading, 999) THEN systolic_blood_pressure_second_reading
			WHEN systolic_blood_pressure = systolic_blood_pressure_second_reading
			AND diastolic_blood_pressure = diastolic_blood_pressure_second_reading THEN systolic_blood_pressure_second_reading
		END AS first_systolic_lowest,
		CASE
			WHEN COALESCE(systolic_blood_pressure, 999) < COALESCE(systolic_blood_pressure_second_reading, 999) THEN diastolic_blood_pressure
			WHEN COALESCE(systolic_blood_pressure, 999) > COALESCE(systolic_blood_pressure_second_reading, 999) THEN diastolic_blood_pressure_second_reading
			WHEN systolic_blood_pressure = systolic_blood_pressure_second_reading
			AND COALESCE(diastolic_blood_pressure, 999) < COALESCE(diastolic_blood_pressure_second_reading, 999) THEN diastolic_blood_pressure
			WHEN systolic_blood_pressure = systolic_blood_pressure_second_reading
			AND COALESCE(diastolic_blood_pressure, 999) > COALESCE(diastolic_blood_pressure_second_reading, 999) THEN diastolic_blood_pressure_second_reading
			WHEN systolic_blood_pressure = systolic_blood_pressure_second_reading
			AND diastolic_blood_pressure = diastolic_blood_pressure_second_reading THEN diastolic_blood_pressure_second_reading
		END AS first_diastolic_lowest
	FROM
		(
			SELECT
				c.patient_id,
				c.initial_encounter_id,
				vli.date_vitals_taken AS first_bp_date,
				vli.systolic_blood_pressure,
				vli.diastolic_blood_pressure,
				vli.systolic_blood_pressure_second_reading,
				vli.diastolic_blood_pressure_second_reading,
				ROW_NUMBER() OVER (
					PARTITION BY c.initial_encounter_id
					ORDER BY
						vli.date_vitals_taken,
						vli.encounter_id
				) AS rn
			FROM
				cohort c
				JOIN vitals_and_laboratory_information vli ON c.patient_id = vli.patient_id
				AND c.initial_visit_date <= vli.date_vitals_taken
				AND COALESCE(c.outcome_date, CURRENT_DATE) >= vli.date_vitals_taken
			WHERE
				vli.date_vitals_taken IS NOT NULL
				AND (
					(
						vli.systolic_blood_pressure IS NOT NULL
						AND vli.diastolic_blood_pressure IS NOT NULL
					)
					OR (
						vli.systolic_blood_pressure_second_reading IS NOT NULL
						AND vli.diastolic_blood_pressure_second_reading IS NOT NULL
					)
				)
		) bp
	WHERE
		rn = 1
),
-- The first_hba1c CTE extracts the first HbA1c measurement. Date is determined by the date of sample collection. If no date of sample collection is present, the vitals date is used. If neither date of sample collection or vitals date are present, results are not considered. 
first_hba1c AS (
	SELECT
		initial_encounter_id,
		first_hba1c_date,
		first_hba1c
	FROM
		(
			SELECT
				c.patient_id,
				c.initial_encounter_id,
				vli.date_of_sample_collection AS first_hba1c_date,
				vli.hba1c AS first_hba1c,
				ROW_NUMBER() OVER (
					PARTITION BY c.initial_encounter_id
					ORDER BY
						vli.date_of_sample_collection,
						vli.encounter_id
				) AS rn
			FROM
				cohort c
				JOIN vitals_and_laboratory_information vli ON c.patient_id = vli.patient_id
				AND c.initial_visit_date <= vli.date_of_sample_collection
				AND COALESCE(c.outcome_date, CURRENT_DATE) >= vli.date_of_sample_collection
			WHERE
				vli.date_of_sample_collection IS NOT NULL
				AND vli.hba1c IS NOT NULL
		) hba1c
	WHERE
		rn = 1
),
-- *LATEST STATUS: The latest status CTEs extracts and organizes information the last time it was reported during the patient's enrollment window (defined by the cohort CTE).
-- The hospitalised_last_6m CTE checks if patient reported any hospitlisations during an NCD consultation in the last 6 months. 
hospitalisation_last_6m AS (
	SELECT
		initial_encounter_id,
		COUNT(hospitalised) AS nb_hospitalised_last_6m
	FROM
		visits_ordered vo
	WHERE
		hospitalised IS NOT NULL
		AND date_of_visit >= CURRENT_DATE - INTERVAL '6 months'
	GROUP BY
		initial_encounter_id
),
-- The last_foot_exam CTE extracts the date and outcome of the last foot exam performed.
last_foot_assessment AS (
	SELECT
		initial_encounter_id,
		last_foot_assessment_date,
		last_foot_assessment_outcome
	FROM
		(
			SELECT
				initial_encounter_id,
				date_of_visit AS last_foot_assessment_date,
				foot_assessment_outcome AS last_foot_assessment_outcome,
				ROW_NUMBER() OVER (
					PARTITION BY initial_encounter_id
					ORDER BY
						date_of_visit DESC
				) AS rn
			FROM
				visits_ordered vo
			WHERE
				foot_assessment_outcome IS NOT NULL
		) foot
	WHERE
		rn = 1
),
-- The last_seizure CTE extracts the date of last consultation where the patient reported having seizures.
last_seizure AS (
	SELECT
		initial_encounter_id,
		MAX(date_of_visit) AS last_seizure_date
	FROM
		visits_ordered vo
	WHERE
		seizures IS NOT NULL
	GROUP BY
		initial_encounter_id
),
-- The TB screening CTE extracts the last TB screenings and whether the patient was referred for TB managmement reported per cohort enrollment.
tb_screening AS (
	SELECT
		initial_encounter_id,
		last_tb_screening_date,
		last_tb_symptom,
		last_tb_sputum,
		last_tb_xray,
		last_tb_genexpert,
		CASE
			WHEN last_tb_symptom = 'Yes positive'
			OR last_tb_sputum = 'Yes positive'
			OR last_tb_xray = 'Yes positive'
			OR last_tb_genexpert = 'Yes positive' THEN 'Yes'
		END AS last_tb_screening_positive,
		last_tb_referral
	FROM
		(
			SELECT
				initial_encounter_id,
				date_of_visit AS last_tb_screening_date,
				tb_symptom_screening AS last_tb_symptom,
				tb_sputum_screening AS last_tb_sputum,
				tb_xray_screening AS last_tb_xray,
				tb_genexpert_screening AS last_tb_genexpert,
				referred_for_tb_management AS last_tb_referral,
				ROW_NUMBER() OVER (
					PARTITION BY initial_encounter_id
					ORDER BY
						date_of_visit DESC
				) AS rn
			FROM
				visits_ordered vo
			WHERE
				COALESCE(
					tb_symptom_screening,
					tb_sputum_screening,
					tb_xray_screening,
					tb_genexpert_screening
				) IS NOT NULL
		) tb
	WHERE
		rn = 1
),
-- The asthma_exacerbation CTE extracts the last asthma exacerbation level reported.
asthma_exacerbation AS (
	SELECT
		initial_encounter_id,
		last_asthma_exacerbation_date,
		last_asthma_exacerbation_level
	FROM
		(
			SELECT
				initial_encounter_id,
				date_of_visit AS last_asthma_exacerbation_date,
				asthma_exacerbation_level AS last_asthma_exacerbation_level,
				ROW_NUMBER() OVER (
					PARTITION BY initial_encounter_id
					ORDER BY
						date_of_visit DESC
				) AS rn
			FROM
				visits_ordered vo
			WHERE
				asthma_exacerbation_level IS NOT NULL
		) asthma
	WHERE
		rn = 1
),
-- The copd_exacerbation CTE extracts the last COPD exacerbation level reported.
copd_exacerbation AS (
	SELECT
		initial_encounter_id,
		last_copd_exacerbation_date,
		last_copd_exacerbation_level
	FROM
		(
			SELECT
				initial_encounter_id,
				date_of_visit AS last_copd_exacerbation_date,
				copd_exacerbation_level AS last_copd_exacerbation_level,
				ROW_NUMBER() OVER (
					PARTITION BY initial_encounter_id
					ORDER BY
						date_of_visit DESC
				) AS rn
			FROM
				visits_ordered vo
			WHERE
				copd_exacerbation_level IS NOT NULL
		) copd
	WHERE
		rn = 1
),
-- *RECENT VITALS & LAB RESULTS: The latest vitals and laboratory result CTEs extracts and organizes information from the last results during the patient's enrollment window (defined by the cohort CTE). 
-- The last_bp CTE extracts the last completed blood pressure measurement. Date is determined by the vitals date of the vitals and laboratory form. If no vitals date is present, results are not considered. The table considers both BP readings and takes the lowest of the two readings. The logic for the lowest reading, first considers the systolic reading and then if the systolic is the same between the two readings, considers the diastolic reading.
last_bp AS (
	SELECT
		initial_encounter_id,
		last_bp_date,
		systolic_blood_pressure AS last_systolic_first_reading,
		diastolic_blood_pressure AS last_diastolic_first_reading,
		systolic_blood_pressure_second_reading AS last_systolic_second_reading,
		diastolic_blood_pressure_second_reading AS last_diastolic_second_reading,
		CASE
			WHEN COALESCE(systolic_blood_pressure, 999) < COALESCE(systolic_blood_pressure_second_reading, 999) THEN systolic_blood_pressure
			WHEN COALESCE(systolic_blood_pressure, 999) > COALESCE(systolic_blood_pressure_second_reading, 999) THEN systolic_blood_pressure_second_reading
			WHEN systolic_blood_pressure = systolic_blood_pressure_second_reading
			AND COALESCE(diastolic_blood_pressure, 999) < COALESCE(diastolic_blood_pressure_second_reading, 999) THEN systolic_blood_pressure
			WHEN systolic_blood_pressure = systolic_blood_pressure_second_reading
			AND COALESCE(diastolic_blood_pressure, 999) > COALESCE(diastolic_blood_pressure_second_reading, 999) THEN systolic_blood_pressure_second_reading
			WHEN systolic_blood_pressure = systolic_blood_pressure_second_reading
			AND diastolic_blood_pressure = diastolic_blood_pressure_second_reading THEN systolic_blood_pressure_second_reading
		END AS last_systolic_lowest,
		CASE
			WHEN COALESCE(systolic_blood_pressure, 999) < COALESCE(systolic_blood_pressure_second_reading, 999) THEN diastolic_blood_pressure
			WHEN COALESCE(systolic_blood_pressure, 999) > COALESCE(systolic_blood_pressure_second_reading, 999) THEN diastolic_blood_pressure_second_reading
			WHEN systolic_blood_pressure = systolic_blood_pressure_second_reading
			AND COALESCE(diastolic_blood_pressure, 999) < COALESCE(diastolic_blood_pressure_second_reading, 999) THEN diastolic_blood_pressure
			WHEN systolic_blood_pressure = systolic_blood_pressure_second_reading
			AND COALESCE(diastolic_blood_pressure, 999) > COALESCE(diastolic_blood_pressure_second_reading, 999) THEN diastolic_blood_pressure_second_reading
			WHEN systolic_blood_pressure = systolic_blood_pressure_second_reading
			AND diastolic_blood_pressure = diastolic_blood_pressure_second_reading THEN diastolic_blood_pressure_second_reading
		END AS last_diastolic_lowest
	FROM
		(
			SELECT
				c.initial_encounter_id,
				vli.date_vitals_taken AS last_bp_date,
				vli.systolic_blood_pressure,
				vli.diastolic_blood_pressure,
				vli.systolic_blood_pressure_second_reading,
				vli.diastolic_blood_pressure_second_reading,
				ROW_NUMBER() OVER (
					PARTITION BY c.initial_encounter_id
					ORDER BY
						vli.date_vitals_taken DESC,
						vli.encounter_id DESC
				) AS rn
			FROM
				cohort c
				JOIN vitals_and_laboratory_information vli ON c.patient_id = vli.patient_id
				AND c.initial_visit_date <= vli.date_vitals_taken
				AND COALESCE(c.outcome_date, CURRENT_DATE) >= vli.date_vitals_taken
			WHERE
				vli.date_vitals_taken IS NOT NULL
				AND (
					(
						vli.systolic_blood_pressure IS NOT NULL
						AND vli.diastolic_blood_pressure IS NOT NULL
					)
					OR (
						vli.systolic_blood_pressure_second_reading IS NOT NULL
						AND vli.diastolic_blood_pressure_second_reading IS NOT NULL
					)
				)
		) bp
	WHERE
		rn = 1
),
-- The last_bmi CTE extracts the last BMI measurement. Date is determined by the vitals date of the vitals and laboratory form. If no vitals date is present, results are not considered.
last_bmi AS (
	SELECT
		initial_encounter_id,
		last_bmi_date,
		last_bmi
	FROM
		(
			SELECT
				c.initial_encounter_id,
				vli.date_vitals_taken AS last_bmi_date,
				vli.bmi_kg_m2 AS last_bmi,
				ROW_NUMBER() OVER (
					PARTITION BY c.initial_encounter_id
					ORDER BY
						vli.date_vitals_taken DESC,
						vli.encounter_id DESC
				) AS rn
			FROM
				cohort c
				JOIN vitals_and_laboratory_information vli ON c.patient_id = vli.patient_id
				AND c.initial_visit_date <= vli.date_vitals_taken
				AND COALESCE(c.outcome_date, CURRENT_DATE) >= vli.date_vitals_taken
			WHERE
				vli.date_vitals_taken IS NOT NULL
				AND vli.bmi_kg_m2 IS NOT NULL
		) bp
	WHERE
		rn = 1
),
-- The last_fbg CTE extracts the last fasting blood glucose measurement reported per cohort enrollment. Date is determined by date of sample collection. If no date of sample collection is present, results are not considered. 
last_fbg AS (
	SELECT
		initial_encounter_id,
		last_fbg_date,
		last_fbg
	FROM
		(
			SELECT
				c.initial_encounter_id,
				vli.date_of_sample_collection AS last_fbg_date,
				vli.fasting_blood_glucose AS last_fbg,
				ROW_NUMBER() OVER (
					PARTITION BY c.initial_encounter_id
					ORDER BY
						vli.date_of_sample_collection DESC,
						vli.encounter_id DESC
				) AS rn
			FROM
				cohort c
				JOIN vitals_and_laboratory_information vli ON c.patient_id = vli.patient_id
				AND c.initial_visit_date <= vli.date_of_sample_collection
				AND COALESCE(c.outcome_date, CURRENT_DATE) >= vli.date_of_sample_collection
			WHERE
				vli.date_of_sample_collection IS NOT NULL
				AND vli.fasting_blood_glucose IS NOT NULL
		) fbg
	WHERE
		rn = 1
),
-- The last_hba1c CTE extracts the last HbA1c measurement. Date is determined by the date of sample collection. If no date of sample collection is present, results are not considered. 
last_hba1c AS (
	SELECT
		initial_encounter_id,
		last_hba1c_date,
		last_hba1c
	FROM
		(
			SELECT
				c.initial_encounter_id,
				vli.date_of_sample_collection AS last_hba1c_date,
				vli.hba1c AS last_hba1c,
				ROW_NUMBER() OVER (
					PARTITION BY c.initial_encounter_id
					ORDER BY
						vli.date_of_sample_collection DESC,
						vli.encounter_id DESC
				) AS rn
			FROM
				cohort c
				JOIN vitals_and_laboratory_information vli ON c.patient_id = vli.patient_id
				AND c.initial_visit_date <= vli.date_of_sample_collection
				AND COALESCE(c.outcome_date, CURRENT_DATE) >= vli.date_of_sample_collection
			WHERE
				vli.date_of_sample_collection IS NOT NULL
				AND vli.hba1c IS NOT NULL
		) hba1c
	WHERE
		rn = 1
),
-- The last GFR CTE extracts the last GFR measurement. Date is determined by the date of sample collection. If no date of sample collection is present, results are not considered. 
last_gfr AS (
	SELECT
		initial_encounter_id,
		last_gfr_date,
		last_gfr
	FROM
		(
			SELECT
				c.initial_encounter_id,
				vli.date_of_sample_collection AS last_gfr_date,
				vli.gfr AS last_gfr,
				ROW_NUMBER() OVER (
					PARTITION BY c.initial_encounter_id
					ORDER BY
						vli.date_of_sample_collection DESC,
						vli.encounter_id DESC
				) AS rn
			FROM
				cohort c
				JOIN vitals_and_laboratory_information vli ON c.patient_id = vli.patient_id
				AND c.initial_visit_date <= vli.date_of_sample_collection
				AND COALESCE(c.outcome_date, CURRENT_DATE) >= vli.date_of_sample_collection
			WHERE
				vli.date_of_sample_collection IS NOT NULL
				AND vli.gfr IS NOT NULL
		) gfr
	WHERE
		rn = 1
),
-- The last creatinine CTE extracts the last creatinine measurement. Date is determined by the date of sample collection. If no date of sample collection is present, results are not considered. 
last_creatinine AS (
	SELECT
		initial_encounter_id,
		last_creatinine_date,
		last_creatinine
	FROM
		(
			SELECT
				c.initial_encounter_id,
				vli.date_of_sample_collection AS last_creatinine_date,
				vli.creatinine AS last_creatinine,
				ROW_NUMBER() OVER (
					PARTITION BY c.initial_encounter_id
					ORDER BY
						vli.date_of_sample_collection DESC,
						vli.encounter_id DESC
				) AS rn
			FROM
				cohort c
				JOIN vitals_and_laboratory_information vli ON c.patient_id = vli.patient_id
				AND c.initial_visit_date <= vli.date_of_sample_collection
				AND COALESCE(c.outcome_date, CURRENT_DATE) >= vli.date_of_sample_collection
			WHERE
				vli.date_of_sample_collection IS NOT NULL
				AND vli.creatinine IS NOT NULL
		) creat
	WHERE
		rn = 1
),
-- The last ASAT CTE extracts the last ASAT measurement. Date is determined by the date of sample collection. If no date of sample collection is present, results are not considered.
last_asat AS (
	SELECT
		initial_encounter_id,
		last_asat_date,
		last_asat
	FROM
		(
			SELECT
				c.initial_encounter_id,
				vli.date_of_sample_collection AS last_asat_date,
				vli.asat AS last_asat,
				ROW_NUMBER() OVER (
					PARTITION BY c.initial_encounter_id
					ORDER BY
						vli.date_of_sample_collection DESC,
						vli.encounter_id DESC
				) AS rn
			FROM
				cohort c
				JOIN vitals_and_laboratory_information vli ON c.patient_id = vli.patient_id
				AND c.initial_visit_date <= vli.date_of_sample_collection
				AND COALESCE(c.outcome_date, CURRENT_DATE) >= vli.date_of_sample_collection
			WHERE
				vli.date_of_sample_collection IS NOT NULL
				AND vli.asat IS NOT NULL
		) asat
	WHERE
		rn = 1
),
-- The last ALAT CTE extracts the last ALAT measurement. Date is determined by the date of sample collection. If no date of sample collection is present, results are not considered.
last_alat AS (
	SELECT
		initial_encounter_id,
		last_alat_date,
		last_alat
	FROM
		(
			SELECT
				c.initial_encounter_id,
				vli.date_of_sample_collection AS last_alat_date,
				vli.alat AS last_alat,
				ROW_NUMBER() OVER (
					PARTITION BY c.initial_encounter_id
					ORDER BY
						vli.date_of_sample_collection DESC,
						vli.encounter_id DESC
				) AS rn
			FROM
				cohort c
				JOIN vitals_and_laboratory_information vli ON c.patient_id = vli.patient_id
				AND c.initial_visit_date <= vli.date_of_sample_collection
				AND COALESCE(c.outcome_date, CURRENT_DATE) >= vli.date_of_sample_collection
			WHERE
				vli.date_of_sample_collection IS NOT NULL
				AND vli.alat IS NOT NULL
		) alat
	WHERE
		rn = 1
),
-- The last HIV test CTE extracts the last HIV test result. Result is either from the NCD consultation form or the Vitals and Lab form, which ever test is reported most recently. In the NCD consultation form, the HIV test result and HIV test result date (or visit date is test result date is incomplete) need to both be completed. In the Vitals and Lab form, the HIV test result and HIV test date both need to be complete.
last_hiv AS (
	SELECT
		initial_encounter_id,
		last_hiv_date,
		last_hiv,
		hiv_result_source
	FROM
		(
			SELECT
				c.initial_encounter_id,
				vli.date_of_hiv_test AS last_hiv_date,
				vli.hiv_test_result AS last_hiv,
				vli.form_field_path AS hiv_result_source,
				ROW_NUMBER() OVER (
					PARTITION BY c.initial_encounter_id
					ORDER BY
						vli.date_of_hiv_test DESC,
						vli.encounter_id DESC
				) AS rn
			FROM
				cohort c
				LEFT OUTER JOIN (
					SELECT
						patient_id,
						encounter_id,
						date_of_hiv_test,
						hiv_test_result,
						form_field_path
					FROM
						vitals_and_laboratory_information
					WHERE
						date_of_hiv_test IS NOT NULL
						AND hiv_test_result IS NOT NULL
					UNION
					SELECT
						patient_id,
						encounter_id,
						COALESCE(date_last_tested_for_hiv, date_of_visit) AS date_of_hiv_test,
						hiv_status_at_initial_visit AS hiv_test_result,
						form_field_path
					FROM
						ncd_consultations
					WHERE
						COALESCE(date_last_tested_for_hiv, date_of_visit) IS NOT NULL
						AND hiv_status_at_initial_visit IS NOT NULL
				) vli ON c.patient_id = vli.patient_id
				AND c.initial_visit_date <= vli.date_of_hiv_test
				AND COALESCE(c.outcome_date, CURRENT_DATE) >= vli.date_of_hiv_test
		) hiv
	WHERE
		rn = 1
),
-- *MEDICATION: The medication CTEs extract and organize information from the medication tab during the patient's enrollment window (defined by the cohort CTE). 
-- The medication_edit CTE reformates the medication data to include both coded and non-coded drug names, start and end dates, and whether the medication is ongoing.
medication_edit AS (
	SELECT
		c.patient_id,
		c.initial_encounter_id,
		mdd.order_id,
		COALESCE(mdd.coded_drug_name, mdd.non_coded_drug_name) AS medication_name,
		mdd.start_date AS date_started,
		COALESCE(mdd.date_stopped, mdd.calculated_end_date) AS date_ended,
		CASE
			WHEN COALESCE(mdd.date_stopped, mdd.calculated_end_date) > CURRENT_DATE THEN 1
			ELSE NULL
		END AS ongoing
	FROM
		medication_data_default mdd
		LEFT JOIN cohort c ON mdd.patient_id = c.patient_id
		AND c.initial_visit_date <= mdd.start_date
		AND COALESCE(c.outcome_date, CURRENT_DATE) >= mdd.start_date
),
-- The medication_list CTE aggregates all medications reported for each patient, regardless of if the medication is still active or not.
medication_list AS (
	SELECT
		initial_encounter_id,
		STRING_AGG(medication_name, ', ') AS medication_list
	FROM
		medication_edit
	GROUP BY
		initial_encounter_id
),
-- The medication_list_ongoing CTE aggregates only active medications reported for each patient.
medication_list_ongoing AS (
	SELECT
		initial_encounter_id,
		STRING_AGG(medication_name, ', ') AS medication_list_ongoing
	FROM
		medication_edit
	WHERE
		ongoing = 1
	GROUP BY
		initial_encounter_id
) -- *Main query* --
SELECT
	pi. "Patient_Identifier",
	c.patient_id,
	c.initial_encounter_id,
	pa. "patientFileNumber",
	pa. "Patient_Unique_ID_CCC_No",
	pa. "facilityForArt",
	pdd.age AS age_current,
	CASE
		WHEN pdd.age :: INT <= 4 THEN '0-4'
		WHEN pdd.age :: INT >= 5
		AND pdd.age :: INT <= 14 THEN '05-14'
		WHEN pdd.age :: INT >= 15
		AND pdd.age :: INT <= 24 THEN '15-24'
		WHEN pdd.age :: INT >= 25
		AND pdd.age :: INT <= 34 THEN '25-34'
		WHEN pdd.age :: INT >= 35
		AND pdd.age :: INT <= 44 THEN '35-44'
		WHEN pdd.age :: INT >= 45
		AND pdd.age :: INT <= 54 THEN '45-54'
		WHEN pdd.age :: INT >= 55
		AND pdd.age :: INT <= 64 THEN '55-64'
		WHEN pdd.age :: INT >= 65 THEN '65+'
		ELSE NULL
	END AS age_group_current,
	EXTRACT(
		YEAR
		FROM
			age(
				ped.encounter_datetime,
				TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')
			)
	) AS age_admission,
	CASE
		WHEN EXTRACT(
			YEAR
			FROM
				age(
					ped.encounter_datetime,
					TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')
				)
		) :: INT <= 4 THEN '0-4'
		WHEN EXTRACT(
			YEAR
			FROM
				age(
					ped.encounter_datetime,
					TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')
				)
		) :: INT >= 5
		AND EXTRACT(
			YEAR
			FROM
				age(
					ped.encounter_datetime,
					TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')
				)
		) <= 14 THEN '05-14'
		WHEN EXTRACT(
			YEAR
			FROM
				age(
					ped.encounter_datetime,
					TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')
				)
		) :: INT >= 15
		AND EXTRACT(
			YEAR
			FROM
				age(
					ped.encounter_datetime,
					TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')
				)
		) <= 24 THEN '15-24'
		WHEN EXTRACT(
			YEAR
			FROM
				age(
					ped.encounter_datetime,
					TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')
				)
		) :: INT >= 25
		AND EXTRACT(
			YEAR
			FROM
				age(
					ped.encounter_datetime,
					TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')
				)
		) <= 34 THEN '25-34'
		WHEN EXTRACT(
			YEAR
			FROM
				age(
					ped.encounter_datetime,
					TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')
				)
		) :: INT >= 35
		AND EXTRACT(
			YEAR
			FROM
				age(
					ped.encounter_datetime,
					TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')
				)
		) <= 44 THEN '35-44'
		WHEN EXTRACT(
			YEAR
			FROM
				age(
					ped.encounter_datetime,
					TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')
				)
		) :: INT >= 45
		AND EXTRACT(
			YEAR
			FROM
				age(
					ped.encounter_datetime,
					TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')
				)
		) <= 54 THEN '45-54'
		WHEN EXTRACT(
			YEAR
			FROM
				age(
					ped.encounter_datetime,
					TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')
				)
		) :: INT >= 55
		AND EXTRACT(
			YEAR
			FROM
				age(
					ped.encounter_datetime,
					TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')
				)
		) <= 64 THEN '55-64'
		WHEN EXTRACT(
			YEAR
			FROM
				age(
					ped.encounter_datetime,
					TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')
				)
		) :: INT >= 65 THEN '65+'
		ELSE NULL
	END AS age_group_admission,
	pdd.gender,
	pad.city_village AS county,
	pad.state_province AS sub_county,
	pad.county_district AS ward,
	pad.address2 AS village,
	pa. "Occupation",
	c.initial_visit_date AS enrollment_date,
	CASE
		WHEN c.outcome_date IS NULL THEN 'Yes'
	END AS in_cohort,
	CASE
		WHEN (
			(
				DATE_PART('year', CURRENT_DATE) - DATE_PART('year', c.initial_visit_date)
			) * 12 + (
				DATE_PART('month', CURRENT_DATE) - DATE_PART('month', c.initial_visit_date)
			)
		) >= 6
		AND c.outcome_date IS NULL THEN 'Yes'
	END AS in_cohort_6m,
	CASE
		WHEN (
			(
				DATE_PART('year', CURRENT_DATE) - DATE_PART('year', c.initial_visit_date)
			) * 12 + (
				DATE_PART('month', CURRENT_DATE) - DATE_PART('month', c.initial_visit_date)
			)
		) >= 12
		AND c.outcome_date IS NULL THEN 'Yes'
	END AS in_cohort_12m,
	c.readmission,
	c.initial_visit_location,
	c.outcome_date,
	c.patient_outcome,
	vo.date_of_visit AS last_visit_date,
	(CURRENT_DATE - vo.date_of_visit) AS days_since_last_visit,
	vo.visit_type AS last_visit_type,
	vo.visit_location AS last_visit_location,
	vo.provider AS last_provider,
	vo.date_of_next_appointment AS next_appointment_date, 
	vo.visit_unscheduled AS last_visit_unscheduled,
	rul.reason_unscheduled AS last_visit_unscheduled_reason,
	vo.patient_s_stability AS last_stability,
	vo.pregnant AS pregnant_last_visit,
	vo.hospitalised AS hospitalised_last_visit,
	vo.seizures AS seizures_last_visit,
	cll.current_lifestyle_list AS current_lifestyle_list,
	COALESCE(vo.service_package, vo.prev_service_package) AS last_service_package,
	vo.dsdm_group_name_and_number AS current_dsdm_group,
	vo.dsdm_visit_status AS last_dsdm_visit_status,
	cds.current_dsd_start_date AS last_dsd_start_date,
	(CURRENT_DATE - cds.current_dsd_start_date) AS days_in_current_dsd,
	ds.ever_dsd AS ever_in_dsd,
	ds.interrupted_dsd AS dsd_interruption,
	ndx.asthma,
	ndx.sickle_cell_disease,
	ndx.copd,
	ndx.diabetes_type1,
	ndx.diabetes_type2,
	CASE
		WHEN ndx.diabetes_type1 IS NOT NULL
		OR ndx.diabetes_type2 IS NOT NULL THEN 1
	END AS diabetes_any,
	ndx.hypertension_stage1,
	ndx.hypertension_stage2,
	ndx.hypertension_stage3,
	CASE
		WHEN ndx.hypertension_stage1 IS NOT NULL
		OR ndx.hypertension_stage2 IS NOT NULL
		OR ndx.hypertension_stage3 IS NOT NULL THEN 1
	END AS hypertension_any,
	ndx.focal_epilepsy,
	ndx.generalised_epilepsy,
	ndx.unclassified_epilepsy,
	ndl.diagnosis_list,
	odl.other_diagnosis_list,
	cp.last_complication_date,
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
	cl.complication_list AS complication_list_all,
	clb.complication_list AS complication_list_baseline,
	fbp.first_bp_date,
	fbp.first_systolic_lowest,
	fbp.first_diastolic_lowest,
	CASE
		WHEN fbp.first_systolic_lowest IS NOT NULL
		AND fbp.first_diastolic_lowest IS NOT NULL THEN CONCAT(
			fbp.first_systolic_lowest,
			'/',
			fbp.first_diastolic_lowest
		)
	END AS first_bp_lowest,
	fbg.first_hba1c,
	fbg.first_hba1c_date,
	CASE
		WHEN h6m.nb_hospitalised_last_6m IS NOT NULL THEN 'Yes'
	END AS hospitalised_last_6m,
	h6m.nb_hospitalised_last_6m,
	lfa.last_foot_assessment_date,
	lfa.last_foot_assessment_outcome,
	ls.last_seizure_date,
	(CURRENT_DATE - ls.last_seizure_date) AS days_since_last_seizure,
	CASE
		WHEN AGE(
			COALESCE(c.outcome_date, CURRENT_DATE),
			ls.last_seizure_date
		) >= '2 years' THEN 'Yes'
	END AS seizure_free_2_years,
	tbs.last_tb_screening_date,
	tbs.last_tb_symptom,
	tbs.last_tb_sputum,
	tbs.last_tb_xray,
	tbs.last_tb_genexpert,
	tbs.last_tb_screening_positive,
	tbs.last_tb_referral,
	ae.last_asthma_exacerbation_date,
	ae.last_asthma_exacerbation_level,
	ce.last_copd_exacerbation_date,
	ce.last_copd_exacerbation_level,
	lbp.last_bp_date,
	lbp.last_systolic_first_reading,
	lbp.last_diastolic_first_reading,
	CASE
		WHEN lbp.last_systolic_first_reading IS NOT NULL
		AND lbp.last_diastolic_first_reading IS NOT NULL THEN CONCAT(
			lbp.last_systolic_first_reading,
			'/',
			lbp.last_diastolic_first_reading
		)
	END AS last_bp_first_reading,
	lbp.last_systolic_second_reading,
	lbp.last_diastolic_second_reading,
	CASE
		WHEN lbp.last_systolic_second_reading IS NOT NULL
		AND lbp.last_diastolic_second_reading IS NOT NULL THEN CONCAT(
			lbp.last_systolic_second_reading,
			'/',
			lbp.last_diastolic_second_reading
		)
	END AS last_bp_second_reading,
	lbp.last_systolic_lowest,
	lbp.last_diastolic_lowest,
	CASE
		WHEN lbp.last_systolic_lowest IS NOT NULL
		AND lbp.last_diastolic_lowest IS NOT NULL THEN CONCAT(
			lbp.last_systolic_lowest,
			'/',
			lbp.last_diastolic_lowest
		)
	END AS last_bp_lowest,
	CASE
		WHEN lbp.last_systolic_lowest <= 140
		AND lbp.last_diastolic_lowest <= 90 THEN 'Yes'
		WHEN lbp.last_systolic_lowest > 140
		OR lbp.last_diastolic_lowest > 90 THEN 'No'
	END AS last_bp_control_htn,
	CASE
		WHEN lbp.last_systolic_lowest <= 130
		AND lbp.last_diastolic_lowest <= 90 THEN 'Yes'
		WHEN lbp.last_systolic_lowest > 130
		OR lbp.last_diastolic_lowest > 90 THEN 'No'
	END AS last_bp_control_dm,
	lbmi.last_bmi_date,
	lbmi.last_bmi,
	lfbg.last_fbg_date,
	lfbg.last_fbg,
	lhba1c.last_hba1c_date,
	lhba1c.last_hba1c,
	CASE
		WHEN lhba1c.last_hba1c <= 6.5 THEN '0-6.5%'
		WHEN lhba1c.last_hba1c BETWEEN 6.6
		AND 8 THEN '6.6-8.0%'
		WHEN lhba1c.last_hba1c > 8 THEN '>8%'
	END AS last_hba1c_grouping,
	CASE
		WHEN lhba1c.last_hba1c < 8 THEN 'Yes'
		WHEN lhba1c.last_hba1c >= 8 THEN 'No'
		WHEN lhba1c.last_hba1c IS NULL
		AND lfbg.last_fbg < 8.3 THEN 'Yes'
		WHEN lhba1c.last_hba1c IS NULL
		AND lfbg.last_fbg >= 8.3 THEN 'No'
	END AS diabetes_control,
	lgfr.last_gfr_date,
	lgfr.last_gfr,
	CASE
		WHEN lgfr.last_gfr < 30 THEN 'Yes'
		WHEN lgfr.last_gfr >= 30 THEN 'No'
	END AS gfr_control,
	lc.last_creatinine_date,
	lc.last_creatinine,
	lal.last_alat_date,
	lal.last_alat,
	las.last_asat_date,
	las.last_asat,
	lh.last_hiv_date,
	lh.last_hiv,
	lh.hiv_result_source,
	ml.medication_list,
	mlo.medication_list_ongoing
FROM
	cohort c
	LEFT OUTER JOIN patient_identifier pi ON c.patient_id = pi.patient_id
	LEFT OUTER JOIN person_attributes pa ON c.patient_id = pa.person_id
	LEFT OUTER JOIN person_details_default pdd ON c.patient_id = pdd.person_id
	LEFT OUTER JOIN patient_encounter_details_default ped ON c.initial_encounter_id = ped.encounter_id
	LEFT OUTER JOIN person_address_default pad ON c.patient_id = pad.person_id
	LEFT OUTER JOIN (
		SELECT
			initial_encounter_id,
			date_of_visit,
			visit_type, 
			visit_location,
			provider,
			visit_unscheduled,
			patient_s_stability,
			pregnant,
			hospitalised,
			seizures,
			service_package,
			prev_service_package,
			dsdm_group_name_and_number,
			dsdm_visit_status,
			date_of_next_appointment
		FROM
			visits_ordered
		WHERE
			rn_desc = 1
	) vo ON c.initial_encounter_id = vo.initial_encounter_id
	LEFT OUTER JOIN reason_unscheduled_list rul ON c.initial_encounter_id = rul.initial_encounter_id
	LEFT OUTER JOIN current_lifestyle_list cll ON c.initial_encounter_id = cll.initial_encounter_id
	LEFT OUTER JOIN dsd_status ds ON c.initial_encounter_id = ds.initial_encounter_id
	LEFT OUTER JOIN current_dsd_streak cds ON c.initial_encounter_id = cds.initial_encounter_id
	LEFT OUTER JOIN ncd_diagnosis_pivot ndx ON c.initial_encounter_id = ndx.initial_encounter_id
	LEFT OUTER JOIN ncd_diagnosis_list ndl ON c.initial_encounter_id = ndl.initial_encounter_id
	LEFT OUTER JOIN other_diagnosis_list odl ON c.initial_encounter_id = odl.initial_encounter_id
	LEFT OUTER JOIN complication_pivot cp ON c.initial_encounter_id = cp.initial_encounter_id
	LEFT OUTER JOIN complication_list cl ON c.initial_encounter_id = cl.initial_encounter_id
	LEFT OUTER JOIN complication_list_baseline clb ON c.initial_encounter_id = clb.initial_encounter_id
	LEFT OUTER JOIN first_bp fbp ON c.initial_encounter_id = fbp.initial_encounter_id
	LEFT OUTER JOIN first_hba1c fbg ON c.initial_encounter_id = fbg.initial_encounter_id
	LEFT OUTER JOIN hospitalisation_last_6m h6m ON c.initial_encounter_id = h6m.initial_encounter_id
	LEFT OUTER JOIN last_foot_assessment lfa ON c.initial_encounter_id = lfa.initial_encounter_id
	LEFT OUTER JOIN last_seizure ls ON c.initial_encounter_id = ls.initial_encounter_id
	LEFT OUTER JOIN tb_screening tbs ON c.initial_encounter_id = tbs.initial_encounter_id
	LEFT OUTER JOIN asthma_exacerbation ae ON c.initial_encounter_id = ae.initial_encounter_id
	LEFT OUTER JOIN copd_exacerbation ce ON c.initial_encounter_id = ce.initial_encounter_id
	LEFT OUTER JOIN last_bp lbp ON c.initial_encounter_id = lbp.initial_encounter_id
	LEFT OUTER JOIN last_bmi lbmi ON c.initial_encounter_id = lbmi.initial_encounter_id
	LEFT OUTER JOIN last_fbg lfbg ON c.initial_encounter_id = lfbg.initial_encounter_id
	LEFT OUTER JOIN last_hba1c lhba1c ON c.initial_encounter_id = lhba1c.initial_encounter_id
	LEFT OUTER JOIN last_gfr lgfr ON c.initial_encounter_id = lgfr.initial_encounter_id
	LEFT OUTER JOIN last_creatinine lc ON c.initial_encounter_id = lc.initial_encounter_id
	LEFT OUTER JOIN last_alat lal ON c.initial_encounter_id = lal.initial_encounter_id
	LEFT OUTER JOIN last_asat las ON c.initial_encounter_id = las.initial_encounter_id
	LEFT OUTER JOIN last_hiv lh ON c.initial_encounter_id = lh.initial_encounter_id
	LEFT OUTER JOIN medication_list ml ON c.initial_encounter_id = ml.initial_encounter_id
	LEFT OUTER JOIN medication_list_ongoing mlo ON c.initial_encounter_id = mlo.initial_encounter_id;