-- This is a test file for the CBO register. 

-- CTE with registration information (!!! need to add gender and age group)
WITH r AS (
    SELECT person_id AS patient_id, "MSF_ID", "PrEP_ID", "Nationality", "Literacy"
    FROM person_attributes pa),
address AS (
    SELECT person_id AS patient_id, city_village, state_province AS ta, county_district
    FROM person_address_default pad),

-- CTE with initial visit information
initial AS (
    SELECT 
        DISTINCT ON (cf.patient_id) 
        cf.patient_id, 
        cf.date_of_visit, 
        cf.visit_location AS initial_visit_location, 
        cf.hiv_status_at_visit AS initial_hiv_status_at_visit, 
        cf.hiv_testing_at_visit AS initial_hiv_testing_at_visit, 
        cf.prep_treatment AS initial_prep_treatment
    FROM "1_client_form" cf
    ORDER BY cf.patient_id, cf.date_of_visit),

-- CTE if a visit happened after the first visit, see date and location
follow_up AS (
    SELECT 
        DISTINCT ON (cf.patient_id) 
        cf.patient_id, 
        cf.date_of_visit AS date_of_last_visit, 
        cf.visit_location AS last_visit_location 
    FROM "1_client_form" cf
    WHERE visit_type = 'Follow-up visit'
    ORDER BY cf.patient_id, cf.date_of_visit DESC),

-- CTE with last visit information
last_pregnant AS (
    SELECT 
        DISTINCT ON (cf.patient_id) 
        cf.patient_id, 
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

last_HPV_screening AS (
    SELECT 
        DISTINCT ON (cf.patient_id) 
        cf.patient_id,
        cf.hpv_screening
    FROM "1_client_form" cf
    ORDER BY cf.patient_id, cf.date_of_visit DESC),

last_HPV_treated AS (
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
FROM patient_identifier
LEFT OUTER JOIN r USING (patient_id)
LEFT OUTER JOIN address USING (patient_id)
LEFT OUTER JOIN initial USING (patient_id)
LEFT OUTER JOIN follow_up USING (patient_id)
LEFT OUTER JOIN last_pregnant USING (patient_id)
LEFT OUTER JOIN last_sti USING (patient_id)
LEFT OUTER JOIN last_hiv_at_visit USING (patient_id)
LEFT OUTER JOIN last_arv_date USING (patient_id)
LEFT OUTER JOIN last_HPV_screening USING (patient_id)
LEFT OUTER JOIN last_HPV_treated USING (patient_id)
LEFT OUTER JOIN last_contra USING (patient_id)
LEFT OUTER JOIN last_appointment USING (patient_id)
LEFT OUTER JOIN last_use_of_routine_data USING (patient_id)
LEFT OUTER JOIN last_lab_hiv_result USING (patient_id)
LEFT OUTER JOIN last_lab_syphilis USING (patient_id)
LEFT OUTER JOIN last_lab_hep_b USING (patient_id)
LEFT OUTER JOIN last_lab_pregnancy USING (patient_id)
LEFT OUTER JOIN last_lab_hpv USING (patient_id)
LEFT OUTER JOIN last_lab_creat USING (patient_id)
LEFT OUTER JOIN last_lab_gfr USING (patient_id);

-- CTE to have current age and inclusion age
WITH initial AS (
    SELECT 
        DISTINCT ON (cf.patient_id) 
        cf.patient_id, 
        cf.date_of_visit, 
        cf.visit_location AS initial_visit_location, 
        cf.hiv_status_at_visit AS initial_hiv_status_at_visit, 
        cf.hiv_testing_at_visit AS initial_hiv_testing_at_visit, 
        cf.prep_treatment AS initial_prep_treatment
    FROM "1_client_form" cf
    ORDER BY cf.patient_id, cf.date_of_visit)
SELECT 
    pi."Patient_Identifier",
    i.patient_id,
    pa."MSF_ID",
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
    END AS groupe_age_current,
    
    -- Calculate age only once
    EXTRACT(YEAR FROM age(cf.date_of_visit, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy'))) AS age_inclusion,

    -- Use the calculated age for inclusion grouping
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
    END AS groupe_age_inclusion,

    cf.date_of_visit AS date_inclusion
FROM initial AS i
LEFT OUTER JOIN "1_client_form" cf ON i.patient_id = cf.patient_id
LEFT OUTER JOIN patient_identifier pi ON i.patient_id = pi.patient_id
LEFT OUTER JOIN person_attributes pa ON i.patient_id = pa.person_id
LEFT OUTER JOIN person_details_default pdd ON i.patient_id = pdd.person_id;
