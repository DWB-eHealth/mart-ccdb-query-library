-- *COHORT FRAME
-- The cohort CTE builds the enrollment window for patients entering and exiting the SCD cohort. The enrollment window is based on SCD forms with visit types of 'initial visit' and 'Discharge visit', matching all initial visit forms to the cooresponding patient discharge visit form, if present. If an individual patient has more than one initial visit form, then the patient discharge visit form will be matched to the initial visit form where the patient discharge visit form date falls between the first initial visit date and the following initial visit date.
WITH cohort AS (
	SELECT
		i.patient_id,
		i.initial_encounter_id,
		i.initial_visit_location,
		i.initial_visit_date,
		CASE
			WHEN i.initial_visit_order > 1 THEN 'Yes'
		END readmission,
		d.encounter_id AS discharge_encounter_id,
		d.discharge_date2 AS discharge_date,
		d.discharge_status,
		d.reason_for_lost_of_follow_up,
		d.transfer_location
	FROM
		(
			SELECT
				patient_id,
				encounter_id AS initial_encounter_id,
				scd_visit_location AS initial_visit_location,
				scd_date_of_visit AS initial_visit_date,
				DENSE_RANK () OVER (
					PARTITION BY patient_id
					ORDER BY
						scd_date_of_visit
				) AS initial_visit_order,
				LEAD (scd_date_of_visit) OVER (
					PARTITION BY patient_id
					ORDER BY
						scd_date_of_visit
				) AS next_initial_visit_date
			FROM
				scd
			WHERE
				visit_type = 'Initial visit'
		) i
		LEFT JOIN (
			SELECT
				patient_id,
				encounter_id,
				COALESCE(discharge_date, scd_date_of_visit) AS discharge_date2,
				discharge_status,
				reason_for_lost_of_follow_up,
				transfer_location
			FROM
				scd
			WHERE
				visit_type = 'Discharge visit'
		) d ON i.patient_id = d.patient_id
		AND (
			d.discharge_date2 IS NULL
			OR (
				d.discharge_date2 >= i.initial_visit_date
				AND (
					d.discharge_date2 < i.next_initial_visit_date
					OR i.next_initial_visit_date IS NULL
				)
			)
		)
),
-- *VISIT INFORMATION: The visit tables extract and organize visit information reported during the patient's enrollment window (defined by the cohort CTE). 
-- The visits_ordered CTE extracts the SCD visit information and ranks the visits in order, matching prior and following service packages. 
visits_ordered AS (
	SELECT
		s.patient_id,
		c.initial_encounter_id,
		s.encounter_id,
		s.scd_date_of_visit,
		s.visit_type,
		s.scd_visit_location,
		s.source_of_information,
		s.previous_scd_diagnosis,
		s.previous_scd_diagnosis_date,
		s.facility_confirmed_scd_diagnosis,
		s.tested_at_kac,
		s.date_tested_at_kac,
		s.method_of_diagnosis,
		s.blood_group,
		s.currently_pregnant,
		s.estimated_date_of_delivry,
		s.breastfeeding,
		s.date_of_last_menstrual_period,
		s.contraceptive_taken,
		s.contraceptive_method,
		s.previous_blood_transfusion,
		s.patient_under_hydroxyurea_treatment,
		s.hydroxyurea_compliance_since_last_visit,
		s.therapeutic_dose_reached,
		s.dose_escalation,
		s.toxicity,
		s.stop_treatment,
		s.phq4_result,
		s.integrated_to_mh_program,
		s.next_medical_appointment_date,
		ROW_NUMBER() OVER (
			PARTITION BY c.initial_encounter_id
			ORDER BY
				s.scd_date_of_visit DESC
		) AS rn_desc,
		ROW_NUMBER() OVER (
			PARTITION BY c.initial_encounter_id
			ORDER BY
				s.scd_date_of_visit
		) AS rn_asc
	FROM
		scd s
		LEFT OUTER JOIN cohort c ON s.patient_id = c.patient_id
		AND s.scd_date_of_visit >= c.initial_visit_date
		AND s.scd_date_of_visit <= COALESCE(c.discharge_date, CURRENT_DATE)
	WHERE
		s.visit_type != 'Discharge visit'
),
-- The co_morbidities_list CTE combines co-morbidities into a single string, separated by commas. The list will not include duplicates and will be orded in alphabetical order.
co_morbidities_list AS (
	SELECT
		vo.initial_encounter_id,
		STRING_AGG(
			DISTINCT cm.co_morbidities,
			', '
			ORDER BY
				cm.co_morbidities
		) AS co_morbidities
	FROM
		visits_ordered vo
		LEFT JOIN co_morbidities cm USING(encounter_id)
	GROUP BY
		vo.initial_encounter_id
),
-- The complications_list CTE combines complications into a single string, separated by commas. The list will not include duplicates and will be orded in alphabetical order.
complication_list AS (
	SELECT
		vo.initial_encounter_id,
		STRING_AGG(
			DISTINCT sc.scd_complications,
			', '
			ORDER BY
				sc.scd_complications
		) AS complications
	FROM
		visits_ordered vo
		LEFT JOIN scd_scd_complications sc USING(encounter_id)
	GROUP BY
		vo.initial_encounter_id
),
-- The last_new_complication CTE finds the date a complication was last reported for each patient. 
last_new_complication AS (
	SELECT
		initial_encounter_id,
		scd_date_of_visit AS date_last_complication
	FROM
		(
			SELECT
				vo.initial_encounter_id,
				vo.scd_date_of_visit,
				ROW_NUMBER() OVER (
					PARTITION BY vo.initial_encounter_id
					ORDER BY
						vo.scd_date_of_visit DESC
				) AS rn_desc
			FROM
				visits_ordered vo
				LEFT JOIN scd_scd_complications sc USING(encounter_id)
			WHERE
				sc.new_diagnosis = 'Yes'
		) comp
	WHERE
		rn_desc = 1
),
-- The vaccinations_list CTE combines vaccinations into a single string, separated by commas. The list will not include duplicates and will be orded in alphabetical order.
vaccinations_list AS (
	SELECT
		vo.initial_encounter_id,
		STRING_AGG(
			DISTINCT vh.vaccination_history,
			', '
			ORDER BY
				vh.vaccination_history
		) AS vaccinations
	FROM
		visits_ordered vo
		LEFT JOIN vaccination_history vh USING(encounter_id)
	GROUP BY
		vo.initial_encounter_id
),
-- The last_side_effects_list CTE combines the last side effects reported for the current hydroxyurea treatment into a single string, separated by commas. The list will not include duplicates and will be orded in alphabetical order.
last_side_effects_list AS (
	SELECT
		vo.initial_encounter_id,
		STRING_AGG(
			DISTINCT se.side_effects,
			', '
			ORDER BY
				se.side_effects
		) AS side_effects
	FROM
		visits_ordered vo
		LEFT JOIN side_effects se USING(encounter_id)
	WHERE
		rn_desc = 1
	GROUP BY
		vo.initial_encounter_id
),
-- *BASELINE: The baseline CTEs extract and organize information recorded at the initial visit or at the start of the patient care in the cohort reported during the patient's enrollment window (defined by the cohort CTE). 
-- The baseline_bmi CTE extracts the BMI measurement recorded at the first visit. Only results from forms with visit type as first visit are considered. The date is determined by the date vitals. If no date vitals is present, results are not considered. 
baseline_bmi AS (
	SELECT
		initial_encounter_id,
		first_bmi_date,
		first_bmi
	FROM
		(
			SELECT
				c.patient_id,
				c.initial_encounter_id,
				vli.date_vital AS first_bmi_date,
				vli.bmi_kg_m2 AS first_bmi,
				ROW_NUMBER() OVER (
					PARTITION BY c.initial_encounter_id
					ORDER BY
						vli.date_vital,
						vli.encounter_id
				) AS rn
			FROM
				cohort C
				JOIN vitals_and_laboratory_information vli ON c.patient_id = vli.patient_id
				AND c.initial_visit_date <= vli.date_vital
				AND COALESCE(c.discharge_date, CURRENT_DATE) >= vli.date_vital
			WHERE
				vli.date_vital IS NOT NULL
				AND vli.bmi_kg_m2 IS NOT NULL
				AND vli.visit_type = 'First visit'
		) bmi
	WHERE
		rn = 1
),
-- The baseline_muac CTE extracts the MUAC measurement recorded at the first visit. Only results from forms with visit type as first visit are considered. The date is determined by the date vitals. If no date vitals is present, results are not considered. 
baseline_muac AS (
	SELECT
		initial_encounter_id,
		first_muac_date,
		first_muac
	FROM
		(
			SELECT
				c.patient_id,
				c.initial_encounter_id,
				vli.date_vital AS first_muac_date,
				vli.muac AS first_muac,
				ROW_NUMBER() OVER (
					PARTITION BY c.initial_encounter_id
					ORDER BY
						vli.date_vital,
						vli.encounter_id
				) AS rn
			FROM
				cohort C
				JOIN vitals_and_laboratory_information vli ON c.patient_id = vli.patient_id
				AND c.initial_visit_date <= vli.date_vital
				AND COALESCE(c.discharge_date, CURRENT_DATE) >= vli.date_vital
			WHERE
				vli.date_vital IS NOT NULL
				AND vli.muac IS NOT NULL
				AND vli.visit_type = 'First visit'
		) muac
	WHERE
		rn = 1
),
-- The baseline_hemoglobin CTE extracts the hemoglobin measurement recorded at the first visit. Only results from forms with visit type as first visit are considered. The date is determined by the date vitals. If no date vitals is present, results are not considered. 
baseline_hemoglobin AS (
	SELECT
		initial_encounter_id,
		first_hemoglobin_date,
		first_hemoglobin
	FROM
		(
			SELECT
				c.patient_id,
				c.initial_encounter_id,
				vli.date_of_sample_collection AS first_hemoglobin_date,
				vli.hemoglobin_levels AS first_hemoglobin,
				ROW_NUMBER() OVER (
					PARTITION BY c.initial_encounter_id
					ORDER BY
						vli.date_vital,
						vli.encounter_id
				) AS rn
			FROM
				cohort C
				JOIN vitals_and_laboratory_information vli ON c.patient_id = vli.patient_id
				AND c.initial_visit_date <= vli.date_of_sample_collection
				AND COALESCE(c.discharge_date, CURRENT_DATE) >= vli.date_of_sample_collection
			WHERE
				vli.date_of_sample_collection IS NOT NULL
				AND vli.hemoglobin_levels IS NOT NULL
				AND vli.visit_type = 'First visit'
		) hemo
	WHERE
		rn = 1
),
-- *LATEST STATUS: The latest status CTEs extracts and organizes information the last time it was reported during the patient's enrollment window (defined by the cohort CTE).
-- The last_blood_group CTE extracts the date and outcome of the last blood group reported.
last_blood_group AS (
	SELECT
		initial_encounter_id,
		last_date_blood_group,
		last_blood_group
	FROM
		(
			SELECT
				initial_encounter_id,
				scd_date_of_visit AS last_date_blood_group,
				blood_group AS last_blood_group,
				ROW_NUMBER() OVER (
					PARTITION BY initial_encounter_id
					ORDER BY
						scd_date_of_visit DESC
				) AS rn
			FROM
				visits_ordered vo
			WHERE
				blood_group IS NOT NULL
		) foot
	WHERE
		rn = 1
),
-- The last_blood_transfusion CTE extracts the date and outcome of the last visit where blood transfusion were reported.
last_blood_transfusion AS (
	SELECT
		initial_encounter_id,
		last_date_blood_transfusion,
		last_blood_transfusion
	FROM
		(
			SELECT
				initial_encounter_id,
				scd_date_of_visit AS last_date_blood_transfusion,
				previous_blood_transfusion AS last_blood_transfusion,
				ROW_NUMBER() OVER (
					PARTITION BY initial_encounter_id
					ORDER BY
						scd_date_of_visit DESC
				) AS rn
			FROM
				visits_ordered vo
			WHERE
				previous_blood_transfusion = 'Yes'
		) foot
	WHERE
		rn = 1
),
-- The last_mh_screening CTE extracts the last mental health reported where PHQ4 was not null or none.
last_mh_screening AS (
	SELECT
		initial_encounter_id,
		last_date_mh_screening,
		last_phq4_result,
		integrated_to_mh_program
	FROM
		(
			SELECT
				initial_encounter_id,
				scd_date_of_visit AS last_date_mh_screening,
				phq4_result AS last_phq4_result,
				integrated_to_mh_program,
				ROW_NUMBER() OVER (
					PARTITION BY initial_encounter_id
					ORDER BY
						scd_date_of_visit DESC
				) AS rn
			FROM
				visits_ordered vo
			WHERE
				phq4_result IS NOT NULL
		) mh
	WHERE
		rn = 1
),
-- *RECENT VITALS & LAB RESULTS: The latest vitals and laboratory result CTEs extracts and organizes information from the last results during the patient's enrollment window (defined by the cohort CTE). 
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
				vli.date_vital AS last_bmi_date,
				vli.bmi_kg_m2 AS last_bmi,
				ROW_NUMBER() OVER (
					PARTITION BY c.initial_encounter_id
					ORDER BY
						vli.date_vital DESC,
						vli.encounter_id DESC
				) AS rn
			FROM
				cohort C
				JOIN vitals_and_laboratory_information vli ON c.patient_id = vli.patient_id
				AND c.initial_visit_date <= vli.date_vital
				AND COALESCE(c.discharge_date, CURRENT_DATE) >= vli.date_vital
			WHERE
				vli.date_vital IS NOT NULL
				AND vli.bmi_kg_m2 IS NOT NULL
		) bmi
	WHERE
		rn = 1
),
-- The last_muac CTE extracts the last MUAC measurement. Date is determined by the vitals date of the vitals and laboratory form. If no vitals date is present, results are not considered.
last_muac AS (
	SELECT
		initial_encounter_id,
		last_muac_date,
		last_muac
	FROM
		(
			SELECT
				c.initial_encounter_id,
				vli.date_vital AS last_muac_date,
				vli.muac AS last_muac,
				ROW_NUMBER() OVER (
					PARTITION BY c.initial_encounter_id
					ORDER BY
						vli.date_vital DESC,
						vli.encounter_id DESC
				) AS rn
			FROM
				cohort C
				JOIN vitals_and_laboratory_information vli ON c.patient_id = vli.patient_id
				AND c.initial_visit_date <= vli.date_vital
				AND COALESCE(c.discharge_date, CURRENT_DATE) >= vli.date_vital
			WHERE
				vli.date_vital IS NOT NULL
				AND vli.muac IS NOT NULL
		) muac
	WHERE
		rn = 1
),
-- The last_hemoglobin CTE extracts the last hemoglobin measurement. Date is determined by the date of sample collection. If no date of sample collection is present, results are not considered. 
last_hemoglobin AS (
	SELECT
		initial_encounter_id,
		last_hemoglobin_date,
		last_hemoglobin
	FROM
		(
			SELECT
				c.initial_encounter_id,
				vli.date_of_sample_collection AS last_hemoglobin_date,
				vli.hemoglobin_levels AS last_hemoglobin,
				ROW_NUMBER() OVER (
					PARTITION BY c.initial_encounter_id
					ORDER BY
						vli.date_of_sample_collection DESC,
						vli.encounter_id DESC
				) AS rn
			FROM
				cohort C
				JOIN vitals_and_laboratory_information vli ON c.patient_id = vli.patient_id
				AND c.initial_visit_date <= vli.date_of_sample_collection
				AND COALESCE(c.discharge_date, CURRENT_DATE) >= vli.date_of_sample_collection
			WHERE
				vli.date_of_sample_collection IS NOT NULL
				AND vli.hemoglobin_levels IS NOT NULL
		) hemoglobin
	WHERE
		rn = 1
),
-- The last_creatinine CTE extracts the last creatinine measurement. Date is determined by the date of sample collection. If no date of sample collection is present, results are not considered. 
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
				cohort C
				JOIN vitals_and_laboratory_information vli ON c.patient_id = vli.patient_id
				AND c.initial_visit_date <= vli.date_of_sample_collection
				AND COALESCE(c.discharge_date, CURRENT_DATE) >= vli.date_of_sample_collection
			WHERE
				vli.date_of_sample_collection IS NOT NULL
				AND vli.creatinine IS NOT NULL
		) creat
	WHERE
		rn = 1
),
-- The last_malaria_rdt CTE extracts the last creatinine measurement. Date is determined by the date of sample collection. If no date of sample collection is present, results are not considered. 
last_malaria_rdt AS (
	SELECT
		initial_encounter_id,
		last_malaria_rdt_date,
		last_malaria_rdt
	FROM
		(
			SELECT
				c.initial_encounter_id,
				vli.date_of_sample_collection AS last_malaria_rdt_date,
				vli.malaria_rdt AS last_malaria_rdt,
				ROW_NUMBER() OVER (
					PARTITION BY c.initial_encounter_id
					ORDER BY
						vli.date_of_sample_collection DESC,
						vli.encounter_id DESC
				) AS rn
			FROM
				cohort C
				JOIN vitals_and_laboratory_information vli ON c.patient_id = vli.patient_id
				AND c.initial_visit_date <= vli.date_of_sample_collection
				AND COALESCE(c.discharge_date, CURRENT_DATE) >= vli.date_of_sample_collection
			WHERE
				vli.date_of_sample_collection IS NOT NULL
				AND vli.malaria_rdt IS NOT NULL
		) malaria
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
		LEFT JOIN cohort C ON mdd.patient_id = c.patient_id
		AND c.initial_visit_date <= mdd.start_date
		AND COALESCE(c.discharge_date, CURRENT_DATE) >= mdd.start_date
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
)
 -- *Main query* --
SELECT
	pi."Patient_Identifier",
	c.patient_id,
	c.initial_encounter_id,
	pa."patientFileNumber",
	EXTRACT('YEAR' FROM AGE(CURRENT_DATE, pdd.birthdate))::INT AS age_current,
	CASE
		WHEN (EXTRACT('YEAR' FROM AGE(CURRENT_DATE, pdd.birthdate)))::INT <= 4 THEN '0-4'
		WHEN (EXTRACT('YEAR' FROM AGE(CURRENT_DATE, pdd.birthdate)))::INT >= 5
		AND (EXTRACT('YEAR' FROM AGE(CURRENT_DATE, pdd.birthdate)))::INT <= 9 THEN '05-9'
		WHEN (EXTRACT('YEAR' FROM AGE(CURRENT_DATE, pdd.birthdate)))::INT >= 10
		AND (EXTRACT('YEAR' FROM AGE(CURRENT_DATE, pdd.birthdate)))::INT <= 14 THEN '10-14'
		WHEN (EXTRACT('YEAR' FROM AGE(CURRENT_DATE, pdd.birthdate)))::INT >= 15
		AND (EXTRACT('YEAR' FROM AGE(CURRENT_DATE, pdd.birthdate)))::INT <= 19 THEN '15-19'
		WHEN (EXTRACT('YEAR' FROM AGE(CURRENT_DATE, pdd.birthdate)))::INT >= 20 THEN '20+'
		ELSE NULL
	END AS age_group_current,
	EXTRACT('YEAR' FROM AGE(c.initial_visit_date, pdd.birthdate))::INT AS age_admission,
	CASE
		WHEN (EXTRACT('YEAR' FROM AGE(c.initial_visit_date, pdd.birthdate)))::INT <= 4 THEN '0-4'
		WHEN (EXTRACT('YEAR' FROM AGE(c.initial_visit_date, pdd.birthdate)))::INT >= 5
		AND (EXTRACT('YEAR' FROM AGE(c.initial_visit_date, pdd.birthdate)))::INT <= 9 THEN '05-9'
		WHEN (EXTRACT('YEAR' FROM AGE(c.initial_visit_date, pdd.birthdate)))::INT >= 10
		AND (EXTRACT('YEAR' FROM AGE(c.initial_visit_date, pdd.birthdate)))::INT <= 14 THEN '10-14'
		WHEN (EXTRACT('YEAR' FROM AGE(c.initial_visit_date, pdd.birthdate)))::INT >= 15
		AND (EXTRACT('YEAR' FROM AGE(c.initial_visit_date, pdd.birthdate)))::INT <= 19 THEN '15-19'
		WHEN (EXTRACT('YEAR' FROM AGE(c.initial_visit_date, pdd.birthdate)))::INT >= 20 THEN '20+'
		ELSE NULL
	END AS age_group_admission,
	pdd.gender,
	pad.city_village AS parish,
	pad.state_province AS sub_county,
	pad.county_district AS district,
	pa."Legal_status",
	pa."Personal_situation",
	pa."Education_level",
	pa."Civil_status",
	pa."Living_conditions",
	pa."Occupation",
	c.initial_visit_date AS enrollment_date,
	CASE
		WHEN c.discharge_date IS NULL THEN 'Yes'
	END AS in_cohort,
	CASE
		WHEN (
			(
				DATE_PART('year', CURRENT_DATE) - DATE_PART('year', c.initial_visit_date)
			) * 12 + (
				DATE_PART('month', CURRENT_DATE) - DATE_PART('month', c.initial_visit_date)
			)
		) >= 6
		AND c.discharge_date IS NULL THEN 'Yes'
	END AS in_cohort_6m,
	CASE
		WHEN (
			(
				DATE_PART('year', CURRENT_DATE) - DATE_PART('year', c.initial_visit_date)
			) * 12 + (
				DATE_PART('month', CURRENT_DATE) - DATE_PART('month', c.initial_visit_date)
			)
		) >= 12
		AND c.discharge_date IS NULL THEN 'Yes'
	END AS in_cohort_12m,
	c.readmission,
	c.initial_visit_location,
	iv.source_of_information,
	iv.previous_scd_diagnosis,
	iv.previous_scd_diagnosis_date,
	iv.facility_confirmed_scd_diagnosis,
	iv.tested_at_kac,
	iv.date_tested_at_kac,
	iv.method_of_diagnosis,
	c.discharge_date,
	c.discharge_status,
	c.reason_for_lost_of_follow_up,
	c.transfer_location,
	vo.scd_date_of_visit AS last_visit_date,
	(CURRENT_DATE - vo.scd_date_of_visit) AS days_since_last_visit,
	vo.visit_type AS last_visit_type,
	vo.scd_visit_location AS last_visit_location,
	vo.currently_pregnant,
	vo.estimated_date_of_delivry AS estimated_date_of_delivery,
	vo.breastfeeding,
	vo.date_of_last_menstrual_period,
	vo.contraceptive_taken,
	vo.contraceptive_method,
	vo.patient_under_hydroxyurea_treatment,
	vo.hydroxyurea_compliance_since_last_visit,
	vo.therapeutic_dose_reached,
	vo.dose_escalation,
	vo.toxicity,
	vo.stop_treatment,
	vo.next_medical_appointment_date,
	cml.co_morbidities,
	cl.complications,
	lnc.date_last_complication,
	vl.vaccinations,
	lsel.side_effects AS hu_side_effects,
	bb.first_bmi_date,
	bb.first_bmi,
	bm.first_muac_date,
	bm.first_muac,
	bh.first_hemoglobin_date,
	bh.first_hemoglobin,
	lbg.last_date_blood_group,
	lbg.last_blood_group,
	lbt.last_date_blood_transfusion,
	lbt.last_blood_transfusion,
	lms.last_date_mh_screening,
	lms.last_phq4_result,
	lms.integrated_to_mh_program,
	lb.last_bmi_date,
	lb.last_bmi,
	lm.last_muac_date,
	lm.last_muac,
	lh.last_hemoglobin_date,
	lh.last_hemoglobin,
	lc.last_creatinine_date,
	lc.last_creatinine,
	lmr.last_malaria_rdt_date,
	lmr.last_malaria_rdt,
	ml.medication_list,
	mlo.medication_list_ongoing
FROM
	cohort c
	LEFT OUTER JOIN patient_identifier pi ON c.patient_id = pi.patient_id
	LEFT OUTER JOIN person_attributes pa ON c.patient_id = pa.person_id
	LEFT OUTER JOIN person_details_default pdd ON c.patient_id = pdd.person_id
	LEFT OUTER JOIN person_address_default pad ON c.patient_id = pad.person_id
	LEFT OUTER JOIN (
		SELECT
			initial_encounter_id,
			scd_date_of_visit,
			visit_type,
			scd_visit_location,
			source_of_information,
			previous_scd_diagnosis,
			previous_scd_diagnosis_date,
			facility_confirmed_scd_diagnosis,
			tested_at_kac,
			date_tested_at_kac,
			method_of_diagnosis
		FROM
			visits_ordered
		WHERE
			rn_asc = 1
	) iv ON c.initial_encounter_id = iv.initial_encounter_id
	LEFT OUTER JOIN (
		SELECT
			initial_encounter_id,
			scd_date_of_visit,
			visit_type,
			scd_visit_location,
			currently_pregnant,
			estimated_date_of_delivry,
			breastfeeding,
			date_of_last_menstrual_period,
			contraceptive_taken,
			contraceptive_method,
			patient_under_hydroxyurea_treatment,
			hydroxyurea_compliance_since_last_visit,
			therapeutic_dose_reached,
			dose_escalation,
			toxicity,
			stop_treatment,
			next_medical_appointment_date
		FROM
			visits_ordered
		WHERE
			rn_desc = 1
	) vo ON c.initial_encounter_id = vo.initial_encounter_id
	LEFT OUTER JOIN co_morbidities_list cml ON c.initial_encounter_id = cml.initial_encounter_id
	LEFT OUTER JOIN complication_list cl ON c.initial_encounter_id = cl.initial_encounter_id
	LEFT OUTER JOIN last_new_complication lnc ON c.initial_encounter_id = lnc.initial_encounter_id
	LEFT OUTER JOIN vaccinations_list vl ON c.initial_encounter_id = vl.initial_encounter_id
	LEFT OUTER JOIN last_side_effects_list lsel ON c.initial_encounter_id = lsel.initial_encounter_id
	LEFT OUTER JOIN baseline_bmi bb ON c.initial_encounter_id = bb.initial_encounter_id
	LEFT OUTER JOIN baseline_muac bm ON c.initial_encounter_id = bm.initial_encounter_id
	LEFT OUTER JOIN baseline_hemoglobin bh ON c.initial_encounter_id = bh.initial_encounter_id
	LEFT OUTER JOIN last_blood_group lbg ON c.initial_encounter_id = lbg.initial_encounter_id
	LEFT OUTER JOIN last_blood_transfusion lbt ON c.initial_encounter_id = lbt.initial_encounter_id
	LEFT OUTER JOIN last_mh_screening lms ON c.initial_encounter_id = lms.initial_encounter_id
	LEFT OUTER JOIN last_bmi lb ON c.initial_encounter_id = lb.initial_encounter_id
	LEFT OUTER JOIN last_muac lm ON c.initial_encounter_id = lm.initial_encounter_id
	LEFT OUTER JOIN last_hemoglobin lh ON c.initial_encounter_id = lh.initial_encounter_id
	LEFT OUTER JOIN last_creatinine lc ON c.initial_encounter_id = lc.initial_encounter_id
	LEFT OUTER JOIN last_malaria_rdt lmr ON c.initial_encounter_id = lmr.initial_encounter_id
	LEFT OUTER JOIN medication_list ml ON c.initial_encounter_id = ml.initial_encounter_id
	LEFT OUTER JOIN medication_list_ongoing mlo ON c.initial_encounter_id = mlo.initial_encounter_id;