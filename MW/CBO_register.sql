-- CTE with registration information (ID, address and gender) (!! one patient is missing Patient ID 97 she has no MSF ID)
WITH patient_attributes AS (
    SELECT person_id AS patient_id, "MSF_ID", "PrEP_ID", "Nationality", "Literacy"
    FROM person_attributes),
address AS (
    SELECT person_id AS patient_id, city_village, state_province AS ta, county_district
    FROM person_address_default),
gender AS (
    SELECT gender, person_id AS patient_id
    FROM person_details_default),
pi AS (
    SELECT patient_id, "Patient_Identifier"
    FROM patient_identifier),
registration AS (
    SELECT 
        pa.patient_id,
        pa."MSF_ID",
        pa."PrEP_ID",
        pa."Nationality",
        pa."Literacy",
        a.city_village,
        a.ta AS TA,
        a.county_district,
        g.gender,
        p."Patient_Identifier"
    FROM patient_attributes pa
    LEFT OUTER JOIN address a USING (patient_id)
    LEFT OUTER JOIN gender g USING (patient_id)
    LEFT OUTER JOIN pi p USING (patient_id)),

-- CTE for the information about the initial visit (date, location, HIV status and PrEP)    
initial AS (
    SELECT 
        DISTINCT ON (cf.patient_id) 
        cf.patient_id, 
        cf.date_of_visit AS date_of_initial_visit, 
        cf.visit_location AS initial_visit_location, 
        cf.hiv_status_at_visit AS initial_hiv_status_at_visit, 
        cf.hiv_testing_at_visit AS initial_hiv_testing_at_visit, 
        cf.prep_treatment AS initial_prep_treatment
    FROM "1_client_form" cf
    ORDER BY cf.patient_id, cf.date_of_visit),
    
-- CTE for age group for current age and inclusion age  
age_calculation AS (
    SELECT 
        cf.patient_id,
        EXTRACT(YEAR FROM age(cf.date_of_visit, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy'))) AS age_inclusion
    FROM "1_client_form" cf
    JOIN person_details_default pdd ON cf.patient_id = pdd.person_id),
groupe_age_inclusion AS (
    SELECT 
        cf.patient_id,
        CASE 
            WHEN EXTRACT(YEAR FROM age(cf.date_of_visit, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')))::int <= 4 THEN '0-4'
            WHEN EXTRACT(YEAR FROM age(cf.date_of_visit, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')))::int BETWEEN 5 AND 14 THEN '05-14'
            WHEN EXTRACT(YEAR FROM age(cf.date_of_visit, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')))::int BETWEEN 15 AND 24 THEN '15-24'
            WHEN EXTRACT(YEAR FROM age(cf.date_of_visit, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')))::int BETWEEN 25 AND 34 THEN '25-34'
            WHEN EXTRACT(YEAR FROM age(cf.date_of_visit, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')))::int BETWEEN 35 AND 44 THEN '35-44'
            WHEN EXTRACT(YEAR FROM age(cf.date_of_visit, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')))::int BETWEEN 45 AND 54 THEN '45-54'
            WHEN EXTRACT(YEAR FROM age(cf.date_of_visit, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')))::int BETWEEN 55 AND 64 THEN '55-64'
            WHEN EXTRACT(YEAR FROM age(cf.date_of_visit, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')))::int >= 65 THEN '65+'
            ELSE NULL
        END AS groupe_age_inclusion
    FROM "1_client_form" cf
    JOIN person_details_default pdd ON cf.patient_id = pdd.person_id),
group_age_current AS (
    SELECT 
        pdd.person_id AS patient_id,
        pdd.age AS age_current,
        CASE 
            WHEN pdd.age::int <= 4 THEN '0-4'
            WHEN pdd.age::int BETWEEN 5 AND 14 THEN '05-14'
            WHEN pdd.age::int BETWEEN 15 AND 24 THEN '15-24'
            WHEN pdd.age::int BETWEEN 25 AND 34 THEN '25-34'
            WHEN pdd.age::int BETWEEN 35 AND 44 THEN '35-44'
            WHEN pdd.age::int BETWEEN 45 AND 54 THEN '45-54'
            WHEN pdd.age::int BETWEEN 55 AND 64 THEN '55-64'
            WHEN pdd.age::int >= 65 THEN '65+'
            ELSE NULL
        END AS groupe_age_current
    FROM person_details_default pdd),
age AS (
    SELECT 
        DISTINCT on (gac.patient_id)
        gac.patient_id, 
        ac.age_inclusion,
        gai.groupe_age_inclusion,
        gac.age_current,
        gac.groupe_age_current
    FROM age_calculation ac
    LEFT OUTER JOIN group_age_current gac USING (patient_id)
    LEFT OUTER JOIN groupe_age_inclusion gai USING (patient_id)),

-- CTE with the last follow up visit (date and location)      
follow_up AS (
    SELECT 
        DISTINCT ON (cf.patient_id) 
        cf.patient_id, 
        cf.date_of_visit AS date_of_last_visit, 
        cf.visit_location AS last_visit_location 
    FROM "1_client_form" cf
    WHERE visit_type = 'Follow-up visit'
    ORDER BY cf.patient_id, cf.date_of_visit DESC),

-- CTE with last visit information (missing diagnosis list and contraceptive list)
last_pregnant AS (
    SELECT 
        DISTINCT ON (cf.patient_id) 
        cf.patient_id,
        cf.date_of_visit AS date_last_pregnant_information, 
        cf.pregnant 
    FROM "1_client_form" cf
    ORDER BY cf.patient_id, cf.date_of_visit DESC),
last_sti AS (
    SELECT 
        DISTINCT ON (cf.patient_id) 
        cf.patient_id,
        cf.ever_treated_for_sti, 
        cf.sti_screening
    FROM "1_client_form" cf
    ORDER BY cf.patient_id, cf.date_of_visit DESC),
last_hiv_at_visit AS (
    SELECT 
        DISTINCT ON (cf.patient_id) 
        cf.patient_id, 
        cf.hiv_status_at_visit AS last_hiv_status_at_visit, 
        cf.hiv_testing_at_visit AS last_hiv_testing_at_visit 
    FROM "1_client_form" cf
    ORDER BY cf.patient_id, cf.date_of_visit DESC),
last_arv_date AS (
    SELECT 
        DISTINCT ON (cf.patient_id) 
        cf.patient_id, 
        cf.arv_start_date AS last_arv_start_date
    FROM "1_client_form" cf
    ORDER BY cf.patient_id, cf.date_of_visit DESC),
last_hpv_screening AS (
    SELECT 
        DISTINCT ON (cf.patient_id) 
        cf.patient_id,
        cf.date_of_visit AS date_last_HPV_screening,
        cf.hpv_screening
    FROM "1_client_form" cf
    ORDER BY cf.patient_id, cf.date_of_visit DESC),
last_hpv_treated AS (
    SELECT 
        DISTINCT ON (cf.patient_id) 
        cf.patient_id,
        cf.treated_by_thermal_coagulation
    FROM "1_client_form" cf
    ORDER BY cf.patient_id, cf.date_of_visit DESC),
last_contra AS (
    SELECT 
        DISTINCT ON (cf.patient_id) 
        cf.patient_id, 
        cf.status_of_contraceptive_service
    FROM "1_client_form" cf
    ORDER BY cf.patient_id, cf.date_of_visit DESC),
last_appointment AS (
    SELECT 
        DISTINCT ON (cf.patient_id) 
        cf.patient_id,
        cf.next_appointment_to_be_scheduled
    FROM "1_client_form" cf
    ORDER BY cf.patient_id, cf.date_of_visit DESC),
last_use_of_routine_data AS (
    SELECT 
        DISTINCT ON (cf.patient_id) 
        cf.patient_id,
        cf.use_of_pseudonymized_routine_data_for_the_prep_implementati
    FROM "1_client_form" cf
    ORDER BY cf.patient_id, cf.date_of_visit DESC),

-- CTE for contraceptive on going (multiselect)
co AS (
    SELECT DISTINCT ON (contraceptive_ongoing.patient_id, contraceptive_ongoing.contraceptive_ongoing) 
        contraceptive_ongoing.patient_id, 
        contraceptive_ongoing.encounter_id, 
        cf.date_of_visit AS date_contraceptive_ongoing, 
        contraceptive_ongoing.contraceptive_ongoing
    FROM contraceptive_ongoing 
    LEFT JOIN "1_client_form" cf USING (patient_id)
    ORDER BY contraceptive_ongoing.patient_id, contraceptive_ongoing.contraceptive_ongoing, cf.date_of_visit),

co_pivot AS (
    SELECT 
        DISTINCT ON (co.encounter_id, co.patient_id) co.encounter_id, 
        co.patient_id, 
        MAX(CASE WHEN co.contraceptive_ongoing = 'Injectable' THEN co.date_contraceptive_ongoing END) AS Injectable_ongoing,
        MAX(CASE WHEN co.contraceptive_ongoing = 'Implant' THEN  co.date_contraceptive_ongoing END) AS Implant_ongoing,
        MAX(CASE WHEN co.contraceptive_ongoing = 'Oral contraceptive pill' THEN  co.date_contraceptive_ongoing END) AS Oral_contraceptive_pill_ongoing,
        MAX(CASE WHEN co.contraceptive_ongoing = 'Condom' THEN co.date_contraceptive_ongoing END) AS Condom_ongoing,
        MAX(CASE WHEN co.contraceptive_ongoing = 'Emergency contraceptive pill' THEN  co.date_contraceptive_ongoing END) AS Emergency_contraceptive_pill_ongoing,
        MAX(CASE WHEN co.contraceptive_ongoing = 'IUCD' THEN  co.date_contraceptive_ongoing END) AS IUCD_ongoing
    FROM co
    GROUP BY co.encounter_id, co.patient_id),
co_list AS (
	SELECT encounter_id, STRING_AGG(contraceptive_ongoing, ', ') AS list_contraceptive_ongoing
	FROM co
	GROUP BY encounter_id),

co_table AS (
SELECT co.patient_id, co.encounter_id, co.date_contraceptive_ongoing, co.contraceptive_ongoing, cop.Injectable_ongoing, cop.Implant_ongoing, cop.Oral_contraceptive_pill_ongoing, cop.Condom_ongoing, cop.Emergency_contraceptive_pill_ongoing, cop.IUCD_ongoing, col.list_contraceptive_ongoing
FROM co
LEFT JOIN co_pivot cop ON co.patient_id = cop.patient_id AND co.encounter_id = cop.encounter_id
LEFT JOIN co_list col ON co.encounter_id = col.encounter_id),
    
-- CTE with info on contraceptive provided today (multiselect)
contraceptive_provided AS (
    SELECT DISTINCT ON (cpt.patient_id, cpt.contraceptive_provided_today) 
        cpt.patient_id, 
        cpt.encounter_id, 
        cf.date_of_visit AS date_contraceptive_provided, 
        cpt.contraceptive_provided_today
    FROM contraceptive_provided_today cpt
    LEFT JOIN "1_client_form" cf USING (patient_id)
    ORDER BY cpt.patient_id, cpt.contraceptive_provided_today, cf.date_of_visit),

contraceptive_provided_pivot AS (
    SELECT 
        DISTINCT ON (cp.encounter_id, cp.patient_id) cp.encounter_id, 
        cp.patient_id, 
        MAX(CASE WHEN cp.contraceptive_provided_today = 'Injectable' THEN cp.date_contraceptive_provided END) AS Injectable_provided,
        MAX(CASE WHEN cp.contraceptive_provided_today = 'Implant' THEN  cp.date_contraceptive_provided END) AS Implant_provided,
        MAX(CASE WHEN cp.contraceptive_provided_today = 'Oral contraceptive pill' THEN  cp.date_contraceptive_provided END) AS Oral_contraceptive_pill_provided,
        MAX(CASE WHEN cp.contraceptive_provided_today = 'Condom' THEN cp.date_contraceptive_provided END) AS Condom_provided,
        MAX(CASE WHEN cp.contraceptive_provided_today = 'Emergency contraceptive pill' THEN  cp.date_contraceptive_provided END) AS Emergency_contraceptive_pill_provided,
        MAX(CASE WHEN cp.contraceptive_provided_today = 'IUCD' THEN  cp.date_contraceptive_provided END) AS IUCD_provided
    FROM contraceptive_provided cp
    GROUP BY cp.encounter_id, cp.patient_id),
contraceptive_provided_list AS (
	SELECT encounter_id, STRING_AGG(contraceptive_provided_today, ', ') AS list_contraceptive_provided
	FROM contraceptive_provided
	GROUP BY encounter_id),

contraceptive_provided_table AS (
SELECT cp.patient_id, cp.encounter_id, cp.date_contraceptive_provided, cp.contraceptive_provided_today, cpp.Injectable_provided, cpp.Implant_provided, cpp.Oral_contraceptive_pill_provided, cpp.Condom_provided, cpp.Emergency_contraceptive_pill_provided, cpp.IUCD_provided, cpl.list_contraceptive_provided
FROM contraceptive_provided cp
LEFT JOIN contraceptive_provided_pivot cpp ON cp.patient_id = cpp.patient_id AND cp.encounter_id = cpp.encounter_id
LEFT JOIN contraceptive_provided_list cpl ON cp.encounter_id = cpl.encounter_id),

-- CTE with last lab information (!!!HPV result missing)
last_lab_hiv_result AS (
    SELECT DISTINCT ON (l.patient_id) 
        l.patient_id,
        l.date_of_rapid_hiv_test,
        l.rapid_hiv_test_result
    FROM "2_lab_and_vital_signs_form" l
    WHERE l.date_of_rapid_hiv_test IS NOT NULL OR l.rapid_hiv_test_result IS NOT NULL
    ORDER BY l.patient_id, l.date_of_rapid_hiv_test DESC),
last_lab_syphilis AS (
    SELECT DISTINCT ON (l.patient_id) 
        l.patient_id,
        l.date_of_syphilis_test,
        l.syphilis_test_result
    FROM "2_lab_and_vital_signs_form" l
    WHERE l.date_of_syphilis_test IS NOT NULL OR l.syphilis_test_result IS NOT NULL
    ORDER BY l.patient_id, l.date_of_syphilis_test DESC),
last_lab_hep_b AS (
    SELECT DISTINCT ON (l.patient_id) 
        l.patient_id,
        l.date_of_hepatitis_b_test,
        l.hepatitis_b_test_result
    FROM "2_lab_and_vital_signs_form" l
    WHERE l.date_of_hepatitis_b_test IS NOT NULL OR l.hepatitis_b_test_result IS NOT NULL
    ORDER BY l.patient_id, l.date_of_hepatitis_b_test DESC),
last_lab_pregnancy AS (
    SELECT DISTINCT ON (l.patient_id) 
        l.patient_id,
        l.date_of_pregnancy_test,
        l.pregnancy_test_result
    FROM "2_lab_and_vital_signs_form" l
    WHERE l.date_of_pregnancy_test IS NOT NULL OR l.pregnancy_test_result IS NOT NULL
    ORDER BY l.patient_id, l.date_of_pregnancy_test DESC),
last_lab_hpv AS (
    SELECT DISTINCT ON (l.patient_id) 
        l.patient_id,
        l.date_of_sample_collection_for_hpv_test
    FROM "2_lab_and_vital_signs_form" l
    WHERE l.date_of_sample_collection_for_hpv_test IS NOT NULL
    ORDER BY l.patient_id, l.date_of_sample_collection_for_hpv_test DESC),
last_lab_creat AS (
    SELECT DISTINCT ON (l.patient_id) 
        l.patient_id,
        l.date_of_creatinine_concentration,
        l.creatinine_concentration_result
    FROM "2_lab_and_vital_signs_form" l
    WHERE l.date_of_creatinine_concentration IS NOT NULL OR l.creatinine_concentration_result IS NOT NULL
    ORDER BY l.patient_id, l.date_of_creatinine_concentration DESC),
last_lab_gfr AS (
    SELECT DISTINCT ON (l.patient_id) 
        l.patient_id,
        l.date_of_estimated_glomerular_filtration_rate,
        l.estimated_glomerular_filtration_rate_result
    FROM "2_lab_and_vital_signs_form" l
    WHERE l.date_of_estimated_glomerular_filtration_rate IS NOT NULL OR l.estimated_glomerular_filtration_rate_result IS NOT NULL
    ORDER BY l.patient_id, l.date_of_estimated_glomerular_filtration_rate DESC)

SELECT *
FROM registration
LEFT OUTER JOIN age USING (patient_id)
LEFT OUTER JOIN initial USING (patient_id)
LEFT OUTER JOIN follow_up USING (patient_id)
LEFT OUTER JOIN last_pregnant USING (patient_id)
LEFT OUTER JOIN last_sti USING (patient_id)
LEFT OUTER JOIN last_hiv_at_visit USING (patient_id)
LEFT OUTER JOIN last_arv_date USING (patient_id)
LEFT OUTER JOIN last_hpv_screening USING (patient_id)
LEFT OUTER JOIN last_hpv_treated USING (patient_id)
LEFT OUTER JOIN last_contra USING (patient_id)
LEFT OUTER JOIN last_appointment USING (patient_id)
LEFT OUTER JOIN last_use_of_routine_data USING (patient_id)
LEFT OUTER JOIN co_table USING (patient_id)
LEFT OUTER JOIN contraceptive_provided_table USING (patient_id)
LEFT OUTER JOIN last_lab_hiv_result USING (patient_id)
LEFT OUTER JOIN last_lab_syphilis USING (patient_id)
LEFT OUTER JOIN last_lab_hep_b USING (patient_id)
LEFT OUTER JOIN last_lab_pregnancy USING (patient_id)
LEFT OUTER JOIN last_lab_hpv USING (patient_id)
LEFT OUTER JOIN last_lab_creat USING (patient_id)
LEFT OUTER JOIN last_lab_gfr USING (patient_id);






-- CTE with info on contraceptive on going (done :) )
WITH co AS (
    SELECT DISTINCT ON (contraceptive_ongoing.patient_id, contraceptive_ongoing.contraceptive_ongoing) 
        contraceptive_ongoing.patient_id, 
        contraceptive_ongoing.encounter_id, 
        cf.date_of_visit AS date_contraceptive_ongoing, 
        contraceptive_ongoing.contraceptive_ongoing
    FROM contraceptive_ongoing 
    LEFT JOIN "1_client_form" cf USING (patient_id)
    ORDER BY contraceptive_ongoing.patient_id, contraceptive_ongoing.contraceptive_ongoing, cf.date_of_visit),

co_pivot AS (
    SELECT 
        DISTINCT ON (co.encounter_id, co.patient_id) co.encounter_id, 
        co.patient_id, 
        MAX(CASE WHEN co.contraceptive_ongoing = 'Injectable' THEN co.date_contraceptive_ongoing END) AS Injectable_ongoing,
        MAX(CASE WHEN co.contraceptive_ongoing = 'Implant' THEN  co.date_contraceptive_ongoing END) AS Implant_ongoing,
        MAX(CASE WHEN co.contraceptive_ongoing = 'Oral contraceptive pill' THEN  co.date_contraceptive_ongoing END) AS Oral_contraceptive_pill_ongoing,
        MAX(CASE WHEN co.contraceptive_ongoing = 'Condom' THEN co.date_contraceptive_ongoing END) AS Condom_ongoing,
        MAX(CASE WHEN co.contraceptive_ongoing = 'Emergency contraceptive pill' THEN  co.date_contraceptive_ongoing END) AS Emergency_contraceptive_pill_ongoing,
        MAX(CASE WHEN co.contraceptive_ongoing = 'IUCD' THEN  co.date_contraceptive_ongoing END) AS IUCD_ongoing
    FROM co
    GROUP BY co.encounter_id, co.patient_id),
co_list AS (
	SELECT encounter_id, STRING_AGG(contraceptive_ongoing, ', ') AS list_contraceptive_ongoing
	FROM co
	GROUP BY encounter_id),

co_table AS (
SELECT co.patient_id, co.encounter_id, co.date_contraceptive_ongoing, co.contraceptive_ongoing, cop.Injectable_ongoing, cop.Implant_ongoing, cop.Oral_contraceptive_pill_ongoing, cop.Condom_ongoing, cop.Emergency_contraceptive_pill_ongoing, cop.IUCD_ongoing, col.list_contraceptive_ongoing
FROM co
LEFT JOIN co_pivot cop ON co.patient_id = cop.patient_id AND co.encounter_id = cop.encounter_id
LEFT JOIN co_list col ON co.patient_id = cop.patient_id AND co.encounter_id = col.encounter_id)

SELECT *
FROM co_table;