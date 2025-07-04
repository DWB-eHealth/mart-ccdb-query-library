-- CTE for the information about the initial visit (date, location, HIV status and PrEP)
WITH initial AS (
    SELECT 
        DISTINCT ON (cf.patient_id) 
        cf.patient_id, 
        cf.date_of_visit AS date_of_initial_visit, 
        cf.visit_location AS initial_visit_location, 
        cf.hiv_status_at_visit AS initial_hiv_status_at_visit, 
        cf.hiv_testing_at_visit AS initial_hiv_testing_at_visit, 
        cf.prep_treatment AS initial_prep_treatment
    FROM "1_client_form" cf
    WHERE cf.visit_type = 'Initial visit'
    ORDER BY cf.patient_id, cf.date_of_visit),
    
-- CTE for age group for current age and inclusion age  
age_calculation AS (
    SELECT 
        cf.patient_id,
        EXTRACT(YEAR FROM age(cf.date_of_visit, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy'))) AS age_inclusion
    FROM "1_client_form" cf
    JOIN person_details_default pdd ON cf.patient_id = pdd.person_id
    WHERE cf.visit_type = 'Initial visit'
    ORDER BY cf.patient_id, cf.date_of_visit),
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
    JOIN person_details_default pdd ON cf.patient_id = pdd.person_id
    WHERE cf.visit_type = 'Initial visit'
    ORDER BY cf.patient_id, cf.date_of_visit),
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
    FROM person_details_default pdd
    WHERE pdd.age IS NOT NULL),
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

-- CTE with last visit information - mandatory question (missing diagnosis list and contraceptive list)
last_mandatory_visit_information AS (
    SELECT 
        DISTINCT ON (cf.patient_id) 
        cf.patient_id,
        cf.date_of_visit AS date_of_last_visit,
        cf.visit_location AS last_visit_location,
        cf.visit_type AS last_visit_type, 
        cf.pregnant AS last_pregnant_status,
        cf.ever_treated_for_sti AS last_ever_treated_for_sti,
        cf.sti_screening AS last_sti_screening,
        cf.hiv_status_at_visit AS last_hiv_status_at_visit,
        cf.hiv_testing_at_visit AS last_hiv_testing_at_visit,
        cf.hpv_screening,
        cf.status_of_contraceptive_service
    FROM "1_client_form" cf
    ORDER BY cf.patient_id, cf.date_of_visit DESC),

-- CTE with last visit information - NOT mandatory question (missing diagnosis list and contraceptive list)
last_hpv_treated AS (
    SELECT 
        DISTINCT ON (cf.patient_id) 
        cf.patient_id,
        cf.date_of_visit AS last_date_treated_by_thermal_coagulation,
        cf.treated_by_thermal_coagulation
    FROM "1_client_form" cf
    WHERE cf.treated_by_thermal_coagulation IS NOT NULL
    ORDER BY cf.patient_id, cf.date_of_visit DESC),
    
last_appointment AS (
    SELECT 
        DISTINCT ON (cf.patient_id) 
        cf.patient_id,
        cf.next_appointment_to_be_scheduled
    FROM "1_client_form" cf
    WHERE next_appointment_to_be_scheduled IS NOT NULL
    ORDER BY cf.patient_id, cf.date_of_visit DESC),

-- Last answer completed for use of routine data (not looking the not recorded answer)     
last_use_of_routine_data AS (
    SELECT 
    DISTINCT ON (cf.patient_id) 
    cf.patient_id,
    cf.use_of_pseudonymized_routine_data_for_the_prep_implementati
FROM "1_client_form" cf
WHERE cf.use_of_pseudonymized_routine_data_for_the_prep_implementati IN ('Agreed', 'Objected', 'Withdrew agreement')
ORDER BY cf.patient_id, cf.date_of_visit DESC),

-- CTE for contraceptive on going (multiselect)
co AS (SELECT * FROM (SELECT cf.patient_id, cf.date_of_visit AS date_contraceptive_ongoing, co.contraceptive_ongoing, row_number() over (partition by cf.patient_id ORDER BY cf.date_of_visit DESC) AS rn
FROM "1_client_form" cf
LEFT JOIN contraceptive_ongoing co USING (encounter_id)
WHERE co.encounter_id IS NOT NULL) foo
WHERE rn = 1),

co_pivot AS (
    SELECT 
        DISTINCT ON (co.patient_id) 
        co.patient_id, co.date_contraceptive_ongoing,
        MAX(CASE WHEN co.contraceptive_ongoing = 'Injectable' THEN co.date_contraceptive_ongoing END) AS Injectable_ongoing,
        MAX(CASE WHEN co.contraceptive_ongoing = 'Implant' THEN  co.date_contraceptive_ongoing END) AS Implant_ongoing,
        MAX(CASE WHEN co.contraceptive_ongoing = 'Oral contraceptive pill' THEN  co.date_contraceptive_ongoing END) AS Oral_contraceptive_pill_ongoing,
        MAX(CASE WHEN co.contraceptive_ongoing = 'Condom' THEN co.date_contraceptive_ongoing END) AS Condom_ongoing,
        MAX(CASE WHEN co.contraceptive_ongoing = 'Emergency contraceptive pill' THEN  co.date_contraceptive_ongoing END) AS Emergency_contraceptive_pill_ongoing,
        MAX(CASE WHEN co.contraceptive_ongoing = 'IUCD' THEN  co.date_contraceptive_ongoing END) AS IUCD_ongoing
    FROM co
    GROUP BY co.patient_id, co.date_contraceptive_ongoing),
co_list AS (
	SELECT patient_id, STRING_AGG(contraceptive_ongoing, ', ') AS list_contraceptive_ongoing
	FROM co
	GROUP BY patient_id),

co_table AS (
SELECT pi.patient_id, cop.date_contraceptive_ongoing, cop.Injectable_ongoing, cop.Implant_ongoing, cop.Oral_contraceptive_pill_ongoing, cop.Condom_ongoing, cop.Emergency_contraceptive_pill_ongoing, cop.IUCD_ongoing, col.list_contraceptive_ongoing
FROM patient_identifier pi
LEFT JOIN co_pivot cop ON pi.patient_id = cop.patient_id
LEFT JOIN co_list col ON cop.patient_id = col.patient_id),
    
-- CTE with info on contraceptive provided today (multiselect)
cp AS (SELECT * FROM (SELECT cf.patient_id, cf.date_of_visit AS date_contraceptive_provided, cp.contraceptive_provided_today, row_number() over (partition by cf.patient_id ORDER BY cf.date_of_visit DESC) AS rn
FROM "1_client_form" cf
LEFT JOIN contraceptive_provided_today cp USING (encounter_id)
WHERE cp.encounter_id IS NOT NULL) foo
WHERE rn = 1),

cp_pivot AS (
    SELECT 
        DISTINCT ON (cp.patient_id)  
        cp.patient_id, cp.date_contraceptive_provided,
        MAX(CASE WHEN cp.contraceptive_provided_today = 'Injectable' THEN cp.date_contraceptive_provided END) AS Injectable_provided,
        MAX(CASE WHEN cp.contraceptive_provided_today = 'Implant' THEN  cp.date_contraceptive_provided END) AS Implant_provided,
        MAX(CASE WHEN cp.contraceptive_provided_today = 'Oral contraceptive pill' THEN  cp.date_contraceptive_provided END) AS Oral_contraceptive_pill_provided,
        MAX(CASE WHEN cp.contraceptive_provided_today = 'Condom' THEN cp.date_contraceptive_provided END) AS Condom_provided,
        MAX(CASE WHEN cp.contraceptive_provided_today = 'Emergency contraceptive pill' THEN  cp.date_contraceptive_provided END) AS Emergency_contraceptive_pill_provided,
        MAX(CASE WHEN cp.contraceptive_provided_today = 'IUCD' THEN  cp.date_contraceptive_provided END) AS IUCD_provided
    FROM cp
    GROUP BY cp.patient_id, cp.date_contraceptive_provided),
cp_list AS (
	SELECT patient_id, STRING_AGG(contraceptive_provided_today, ', ') AS list_contraceptive_provided
	FROM cp
	GROUP BY patient_id),

cp_table AS (
SELECT pi.patient_id, cpp.date_contraceptive_provided, cpp.Injectable_provided, cpp.Implant_provided, cpp.Oral_contraceptive_pill_provided, cpp.Condom_provided, cpp.Emergency_contraceptive_pill_provided, cpp.IUCD_provided, cpl.list_contraceptive_provided
FROM patient_identifier pi
LEFT JOIN cp_pivot cpp ON pi.patient_id = cpp.patient_id
LEFT JOIN cp_list cpl ON pi.patient_id = cpl.patient_id),

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
    ORDER BY l.patient_id, l.date_of_estimated_glomerular_filtration_rate DESC),

-- CTE with the last date of PrEP visit card
last_prep_visit_date AS (
    SELECT DISTINCT ON (pvc.patient_id)
pvc.patient_id,
pvc.prep_visit_date as last_prep_visit_date
FROM "4_prep_visit_card" pvc
WHERE pvc.prep_visit_date IS NOT NULL
    ORDER BY pvc.patient_id, pvc.prep_visit_date DESC),
    
-- CTE with the last next appointement date from the PrEP visit card
last_prep_next_appointement_date AS (
SELECT DISTINCT ON (pvc.patient_id)
pvc.patient_id,
pvc.next_appointement_date AS last_prep_next_appointement_date,
DATE_PART('day', pvc.next_appointement_date - NOW()) AS days_until_next_prep_appointment
FROM "4_prep_visit_card" pvc
WHERE pvc.next_appointement_date IS NOT NULL
ORDER BY pvc.patient_id, pvc.next_appointement_date DESC)


SELECT 
    pi.patient_id, 
    "Patient_Identifier",
    pdd.gender,
       "MSF_ID", 
    "PrEP_ID", 
    pad.city_village, 
    pad.state_province AS TA, 
    pad.county_district,
    "Nationality", 
    "Literacy",
    a.age_inclusion, 
    a.groupe_age_inclusion, 
    a.age_current, 
    a.groupe_age_current,
    i.date_of_initial_visit, 
    i.initial_visit_location, 
    i.initial_hiv_status_at_visit, 
    i.initial_hiv_testing_at_visit, 
    i.initial_prep_treatment,
    lmvi.date_of_last_visit,
    lmvi.last_visit_location,
    lmvi.last_visit_type,
    lmvi.last_pregnant_status,
    lmvi.last_ever_treated_for_sti, 
    lmvi.last_sti_screening,
    lmvi.last_hiv_status_at_visit, 
    lmvi.last_hiv_testing_at_visit,
    lmvi.hpv_screening, 
    lmvi.status_of_contraceptive_service,
    lht.last_date_treated_by_thermal_coagulation,
    lht.treated_by_thermal_coagulation,
    la.next_appointment_to_be_scheduled,
    lurd.use_of_pseudonymized_routine_data_for_the_prep_implementati,
    cot.date_contraceptive_ongoing, 
    cot.Injectable_ongoing, 
    cot.Implant_ongoing, 
    cot.Oral_contraceptive_pill_ongoing, 
    cot.Condom_ongoing, 
    cot.Emergency_contraceptive_pill_ongoing, 
    cot.IUCD_ongoing, 
    cot.list_contraceptive_ongoing,
    cpt.date_contraceptive_provided,  
    cpt.Injectable_provided, 
    cpt.Implant_provided, 
    cpt.Oral_contraceptive_pill_provided, 
    cpt.Condom_provided, 
    cpt.Emergency_contraceptive_pill_provided, 
    cpt.IUCD_provided, 
    cpt.list_contraceptive_provided,
    llhr.date_of_rapid_hiv_test, 
    llhr.rapid_hiv_test_result,
    llsy.date_of_syphilis_test, 
    llsy.syphilis_test_result,
    llhb.date_of_hepatitis_b_test, 
    llhb.hepatitis_b_test_result,
    llpt.date_of_pregnancy_test, 
    llpt.pregnancy_test_result,
    llhpv.date_of_sample_collection_for_hpv_test,
    llc.date_of_creatinine_concentration, 
    llc.creatinine_concentration_result,      
    llg.date_of_estimated_glomerular_filtration_rate, 
    llg.estimated_glomerular_filtration_rate_result,
    lpvd.last_prep_visit_date,
    lpnad.last_prep_next_appointement_date,
   lpnad.days_until_next_prep_appointment

FROM patient_identifier pi
LEFT OUTER JOIN person_details_default pdd ON pi.patient_id = pdd.person_id
LEFT OUTER JOIN person_address_default pad ON pi.patient_id = pad.person_id
LEFT OUTER JOIN person_attributes pa ON pi.patient_id = pa.person_id
LEFT OUTER JOIN initial i ON pi.patient_id = i.patient_id
LEFT OUTER JOIN age a ON pi.patient_id = a.patient_id
LEFT OUTER JOIN last_mandatory_visit_information lmvi ON pi.patient_id = lmvi.patient_id
LEFT OUTER JOIN last_hpv_treated lht ON pi.patient_id = lht.patient_id
LEFT OUTER JOIN last_appointment la ON pi.patient_id = la.patient_id
LEFT OUTER JOIN last_use_of_routine_data lurd ON pi.patient_id = lurd.patient_id
LEFT OUTER JOIN co_table cot ON pi.patient_id = cot.patient_id
LEFT OUTER JOIN cp_table cpt ON pi.patient_id = cpt.patient_id
LEFT OUTER JOIN last_lab_hiv_result llhr ON pi.patient_id = llhr.patient_id
LEFT OUTER JOIN last_lab_syphilis llsy ON pi.patient_id = llsy.patient_id
LEFT OUTER JOIN last_lab_hep_b llhb ON pi.patient_id = llhb.patient_id
LEFT OUTER JOIN last_lab_pregnancy llpt ON pi.patient_id = llpt.patient_id
LEFT OUTER JOIN last_lab_hpv llhpv ON pi.patient_id = llhpv.patient_id
LEFT OUTER JOIN last_lab_creat llc ON pi.patient_id = llc.patient_id
LEFT OUTER JOIN last_lab_gfr llg ON pi.patient_id = llg.patient_id
LEFT OUTER JOIN last_prep_visit_date lpvd ON pi.patient_id = lpvd.patient_id
LEFT OUTER JOIN last_prep_next_appointement_date lpnad ON pi.patient_id = lpnad.patient_id
WHERE pi."Patient_Identifier" IS NOT NULL
ORDER BY patient_id;