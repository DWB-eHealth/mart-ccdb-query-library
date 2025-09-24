-- The first CTE build the frame for patients entering and exiting the cohort. This frame is based on MNT VIH TB form with visit types of 'Inclusion' and 'Sortie'. For each patient, the query takes all initial visit dates and matches the discharge visit date occuring after the initial visit. If a patient has multiple initial visits and discharge visits, the match happens sequentially based on the date of visit (e.g. the first initial visit is matched to the first discharge, and so on). If the patient does not have a discharge visit, then the discharge information is empty until completed. 
WITH inclusion AS (
	SELECT 
		patient_id, encounter_id AS encounter_id_inclusion, lieu_de_visite AS lieu_de_visite_inclusion, date AS date_inclusion, DENSE_RANK () OVER (PARTITION BY patient_id ORDER BY date) AS inclusion_visit_order, LEAD (date) OVER (PARTITION BY patient_id ORDER BY date) AS date_inclusion_suivi
	FROM mnt_vih_tb WHERE type_de_visite = 'Inclusion'),
cohorte AS (
	SELECT
		i.patient_id, i.encounter_id_inclusion, i.lieu_de_visite_inclusion, i.date_inclusion, CASE WHEN i.inclusion_visit_order > 1 THEN 'Oui' END readmission, d.encounter_id AS encounter_id_sortie, date_de_sortie, d.statut_de_sortie AS statut_de_sortie
	FROM inclusion i
	LEFT JOIN (SELECT patient_id, encounter_id, COALESCE(date_de_sortie, date) AS date_de_sortie, statut_de_sortie FROM mnt_vih_tb WHERE type_de_visite = 'Sortie') d 
		ON i.patient_id = d.patient_id AND d.date_de_sortie >= i.date_inclusion AND (d.date_de_sortie < i.date_inclusion_suivi OR i.date_inclusion_suivi IS NULL)),
-- The PTPE CTE looks at if the patient has a PTPE form completed. 
dernière_ptpe AS (
	SELECT patient_id, encounter_id_inclusion, date_derniere_ptpe
	FROM (
		SELECT
			c.patient_id, 
			c.encounter_id_inclusion, 
			ptpe.date AS date_derniere_ptpe,
			ROW_NUMBER() OVER (PARTITION BY c.encounter_id_inclusion ORDER BY ptpe.date DESC) AS rn
		FROM cohorte c
		LEFT OUTER JOIN ptpe
			ON c.patient_id = ptpe.patient_id AND c.date_inclusion <= ptpe.date::date AND COALESCE(c.date_de_sortie, CURRENT_DATE) >= ptpe.date::date) foo
	WHERE rn = 1),
-- The last completed form CTE looks at the last date and type of visit for each patient based on the clinical forms (including MNT VIH TB, PTPE).
dernière_fiche AS (
	SELECT 
		DISTINCT ON (c.patient_id, c.encounter_id_inclusion, c.date_inclusion, c.date_de_sortie) c.encounter_id_inclusion,
		forms.date AS date_derniere_visite,
		forms.dernière_fiche_type,
		CASE WHEN forms.form_field_path = 'NCD2' THEN 'MNT/VIH/TB' WHEN forms.form_field_path = 'PMTCT' THEN 'PTPE' ELSE NULL END AS type_derniere_fiche
	FROM cohorte c
	LEFT OUTER JOIN (SELECT patient_id, COALESCE(date_de_sortie, date) AS date, type_de_visite AS dernière_fiche_type, form_field_path FROM mnt_vih_tb UNION SELECT patient_id, date, type_de_visite AS dernière_fiche_type, form_field_path FROM ptpe) forms
		ON c.patient_id = forms.patient_id AND c.date_inclusion <= forms.date::date AND COALESCE(c.date_de_sortie, CURRENT_DATE) >= forms.date::date
	GROUP BY c.patient_id, c.encounter_id_inclusion, c.date_inclusion, c.date_de_sortie, forms.date, forms.dernière_fiche_type, forms.form_field_path
	ORDER BY c.patient_id, c.encounter_id_inclusion, c.date_inclusion, c.date_de_sortie, forms.form_field_path, forms.date DESC),
-- The last visit location CTE finds the last visit location reported in clinical forms (including MNT VIH TB, PTPE).
dernière_fiche_location AS (	
	SELECT 
		DISTINCT ON (c.patient_id, c.encounter_id_inclusion, c.date_inclusion, c.date_de_sortie) c.encounter_id_inclusion,
		forms.lieu_de_visite AS dernière_fiche_location
	FROM cohorte c
	LEFT OUTER JOIN (SELECT patient_id, COALESCE(date_de_sortie, date) AS date, lieu_de_visite FROM mnt_vih_tb UNION 
	SELECT patient_id, date, lieu_de_visite FROM ptpe) forms
		ON c.patient_id = forms.patient_id AND c.date_inclusion <= forms.date::date AND COALESCE(c.date_de_sortie, CURRENT_DATE) >= forms.date::date
	WHERE forms.lieu_de_visite IS NOT NULL
	GROUP BY c.patient_id, c.encounter_id_inclusion, c.date_inclusion, c.date_de_sortie, forms.date, forms.lieu_de_visite
	ORDER BY c.patient_id, c.encounter_id_inclusion, c.date_inclusion, c.date_de_sortie, forms.date DESC),
-- The diagnosis CTEs select the last reported diagnosis per cohort enrollment, both listing and pivoting the data horizonally. The pivoted diagnosis data is presented with the date the diagnosis was first reported.
diagnostic_cohorte_dates AS (
    SELECT 
        d.patient_id, c.encounter_id_inclusion, d.diagnostic, MIN(n.date) AS first_date, MAX(n.date) AS last_date
    FROM diagnostic d
    LEFT JOIN mnt_vih_tb n USING(encounter_id)
    LEFT JOIN cohorte c 
        ON d.patient_id = c.patient_id AND c.date_inclusion <= n.date AND COALESCE(c.date_de_sortie, CURRENT_DATE) >= n.date
    GROUP BY d.patient_id, c.encounter_id_inclusion, d.diagnostic),
dernière_diagnostic_cohorte AS (
    SELECT 
        patient_id,
		encounter_id_inclusion,
        MAX(last_date) AS most_recent_date
    FROM diagnostic_cohorte_dates
    GROUP BY patient_id, encounter_id_inclusion),
diagnostic_cohorte AS (
	SELECT 
    	dcd.patient_id, dcd.encounter_id_inclusion, dcd.diagnostic, dcd.first_date, dcd.last_date
	FROM diagnostic_cohorte_dates dcd
	INNER JOIN dernière_diagnostic_cohorte ddc
    	ON dcd.encounter_id_inclusion = ddc.encounter_id_inclusion AND dcd.last_date = ddc.most_recent_date 
	ORDER BY dcd.encounter_id_inclusion, dcd.diagnostic),
diagnostic_cohorte_pivot AS (
	SELECT 
		DISTINCT ON (encounter_id_inclusion, patient_id) encounter_id_inclusion, 
		patient_id,
		MAX (CASE WHEN diagnostic = 'Asthme' THEN first_date::date ELSE NULL END) AS asthme,
		MAX (CASE WHEN diagnostic = 'Drépanocytose' THEN first_date::date ELSE NULL END) AS drépanocytose,
		MAX (CASE WHEN diagnostic = 'Insuffisance renale chronique' THEN first_date::date ELSE NULL END) AS insuffisance_renal_chronique,
		MAX (CASE WHEN diagnostic = 'Syndrome néphrotique' THEN first_date::date ELSE NULL END) AS syndrome_néphrotique,
		MAX (CASE WHEN diagnostic = 'Maladie cardiovasculaire' THEN first_date::date ELSE NULL END) AS maladie_cardiovasculaire,
		MAX (CASE WHEN diagnostic = 'Bronchopneumopathie chronique obstructive' THEN first_date::date ELSE NULL END) AS bronchopneumopathie_chronique_obstructive,
		MAX (CASE WHEN diagnostic = 'Diabète sucré de type 1' THEN first_date::date ELSE NULL END) AS diabète_type1,
		MAX (CASE WHEN diagnostic = 'Diabète sucré de type 2' THEN first_date::date ELSE NULL END) AS diabète_type2,
		MAX (CASE WHEN diagnostic = 'Hypertension' THEN first_date::date ELSE NULL END) AS hypertension,
		MAX (CASE WHEN diagnostic = 'Hypothyroïdie' THEN first_date::date ELSE NULL END) AS hypothyroïdie,
		MAX (CASE WHEN diagnostic = 'Hyperthyroïdie' THEN first_date::date ELSE NULL END) AS hyperthyroïdie,
		MAX (CASE WHEN diagnostic = 'Épilepsie focale' THEN first_date::date ELSE NULL END) AS épilepsie_focale,
		MAX (CASE WHEN diagnostic = 'Épilepsie généralisée' THEN first_date::date ELSE NULL END) AS épilepsie_généralisée,
		MAX (CASE WHEN diagnostic = 'Épilepsie non classifiée' THEN first_date::date ELSE NULL END) AS épilepsie_non_classifiée,
		MAX (CASE WHEN diagnostic = 'Tuberculose pulmonaire' THEN first_date::date ELSE NULL END) AS tb_pulmonaire,
		MAX (CASE WHEN diagnostic = 'Tuberculose extrapulmonaire' THEN first_date::date ELSE NULL END) AS tb_extrapulmonaire,
		MAX (CASE WHEN diagnostic = 'Infection par le VIH' THEN first_date::date ELSE NULL END) AS vih,
		MAX (CASE WHEN diagnostic LIKE '%hépatite B' THEN first_date::date ELSE NULL END) AS infection_hep_b,
		MAX (CASE WHEN diagnostic LIKE '%hépatite C' THEN first_date::date ELSE NULL END) AS infection_hep_c,
		MAX (CASE WHEN diagnostic = 'Troubles de santé mentale' THEN first_date::date ELSE NULL END) AS troubles_de_santé_mentale,
		MAX (CASE WHEN diagnostic = 'Autre' THEN first_date::date ELSE NULL END) AS autre_diagnostic,
		MAX (CASE WHEN diagnostic IN ('Asthme','Drépanocytose','Insuffisance renale chronique','Syndrome néphrotique','Maladie cardiovasculaire','Bronchopneumopathie chronique obstructive','Diabète sucré de type 1','Diabète sucré de type 2','Hypertension','Hypothyroïdie','Hyperthyroïdie','Épilepsie focale','Épilepsie généralisée','Épilepsie non classifiée','Autre') THEN 'Oui' ELSE NULL END) AS mnt,
		MAX (CASE WHEN diagnostic IN ('Tuberculose pulmonaire','Tuberculose extrapulmonaire') THEN 'Oui' ELSE NULL END) AS tb		
	FROM diagnostic_cohorte
	GROUP BY encounter_id_inclusion, patient_id),
diagnostic_cohorte_liste AS (
	SELECT encounter_id_inclusion, STRING_AGG(diagnostic, ', ') AS liste_diagnostic
	FROM diagnostic_cohorte
	GROUP BY encounter_id_inclusion),
-- The comorbidités CTE selects all reported comorbidities per cohort enrollment, listing the data horizonally.
comorbidités_cohorte AS (
	SELECT
		DISTINCT ON (cm.patient_id, cm.comorbidités) cm.patient_id, c.encounter_id_inclusion, n.date, cm.comorbidités
	FROM comorbidités cm 
	LEFT JOIN mnt_vih_tb n USING(encounter_id)
	LEFT JOIN cohorte c ON cm.patient_id = c.patient_id AND c.date_inclusion <= n.date AND COALESCE(c.date_de_sortie, CURRENT_DATE) >= n.date
	ORDER BY cm.patient_id, cm.comorbidités, n.date),
comorbidités_cohorte_liste AS (
	SELECT encounter_id_inclusion, STRING_AGG(comorbidités, ', ') AS liste_comorbidities
	FROM comorbidités_cohorte
	GROUP BY encounter_id_inclusion),
-- The risk factor CTE pivots the risk factor data horizontally from the MNT VIH TB form. Only the last risk factors are reported per cohort enrollment are present. 
facteurs_risque_cohorte AS (
	SELECT
		DISTINCT ON (fr.patient_id, fr.facteurs_de_risque) fr.patient_id, c.encounter_id_inclusion, n.date, fr.facteurs_de_risque
	FROM facteurs_de_risque fr
	LEFT JOIN mnt_vih_tb n USING(encounter_id) 
	LEFT JOIN cohorte c ON fr.patient_id = c.patient_id AND c.date_inclusion <= n.date AND COALESCE(c.date_de_sortie, CURRENT_DATE) >= n.date
	ORDER BY fr.patient_id, fr.facteurs_de_risque, n.date),
facteurs_risque_pivot AS (
	SELECT 
		DISTINCT ON (encounter_id_inclusion, patient_id) encounter_id_inclusion, 
		patient_id, 
		MAX (CASE WHEN facteurs_de_risque = 'Traditional medicine' THEN 'Oui' ELSE NULL END) AS médecine_traditionnelle,
		MAX (CASE WHEN facteurs_de_risque = 'Second-hand smoking' THEN 'Oui' ELSE NULL END) AS tabagisme_passif,
		MAX (CASE WHEN facteurs_de_risque = 'Smoker' THEN 'Oui' ELSE NULL END) AS fumeur,
		MAX (CASE WHEN facteurs_de_risque = 'Alcohol use' THEN 'Oui' ELSE NULL END) AS consommation_alcool,
		MAX (CASE WHEN facteurs_de_risque = 'Other' THEN 'Oui' ELSE NULL END) AS autre_facteurs_risque
	FROM facteurs_risque_cohorte
	GROUP BY encounter_id_inclusion, patient_id),
-- The ARV initiation CTE provides the ARV initiation date reported in the MNT VIH TB form. The first date of ARV initiation is reported. 
instauration_arv AS (
	SELECT 
		DISTINCT ON (c.patient_id, c.encounter_id_inclusion, c.date_inclusion, c.date_de_sortie) c.encounter_id_inclusion,
		mvt.date_d_instauration_des_arv AS date_instauration_arv
	FROM cohorte c
	LEFT OUTER JOIN mnt_vih_tb mvt
		ON c.patient_id = mvt.patient_id AND c.date_inclusion <= mvt.date::date AND COALESCE(c.date_de_sortie, CURRENT_DATE) >= mvt.date::date
	WHERE mvt.date_d_instauration_des_arv IS NOT NULL
	GROUP BY c.patient_id, c.encounter_id_inclusion, c.date_inclusion, c.date_de_sortie, mvt.date, mvt.date_d_instauration_des_arv
	ORDER BY c.patient_id, c.encounter_id_inclusion, c.date_inclusion, c.date_de_sortie, mvt.date ASC),
-- The ARV treatment CTE provides the ARV initiation date reported in the MNT VIH TB form. The most recent ARV treatment is reported. 
traitement_arv AS (
	SELECT 
		DISTINCT ON (c.patient_id, c.encounter_id_inclusion, c.date_inclusion, c.date_de_sortie) c.encounter_id_inclusion,
		mvt.traitement_arv
	FROM cohorte c
	LEFT OUTER JOIN mnt_vih_tb mvt
		ON c.patient_id = mvt.patient_id AND c.date_inclusion <= mvt.date::date AND COALESCE(c.date_de_sortie, CURRENT_DATE) >= mvt.date::date
	WHERE mvt.traitement_arv IS NOT NULL
	GROUP BY c.patient_id, c.encounter_id_inclusion, c.date_inclusion, c.date_de_sortie, mvt.date, mvt.traitement_arv
	ORDER BY c.patient_id, c.encounter_id_inclusion, c.date_inclusion, c.date_de_sortie, mvt.date DESC),
-- The ARV medication CTE reports if a patient has an active perscription for an ARV medication. Medications are only considered to be active if the calculated end date is after the current date and the stopped date is null.
médicament_arv AS (
	SELECT
		c.patient_id, c.encounter_id_inclusion, mdd.coded_drug_name
	FROM cohorte c 
	LEFT OUTER JOIN instauration_arv ia
	    ON c.encounter_id_inclusion = ia.encounter_id_inclusion
	LEFT OUTER JOIN medication_data_default mdd
		ON c.patient_id = mdd.patient_id AND LEAST(ia.date_instauration_arv, c.date_inclusion) <= mdd.start_date::date AND COALESCE(c.date_de_sortie, CURRENT_DATE) >= mdd.start_date::date
	WHERE mdd.coded_drug_name IN ('ABC 120 mg / 3TC 60 mg, disp. tab.','ABC 600 mg / 3TC 300 mg, tab.','ATV 300 mg / r 100 mg, tab.','AZT 60 mg / 3TC 30 mg , disp. tab.','DARUNAVIR ethanolate (DRV), eq. 600 mg base, tab.','DOLUTEGRAVIR sodium (DTG), eq. 10mg base, disp. tab.','DOLUTEGRAVIR sodium (DTG), eq. 50 mg base, tab.','DORALPVR1P- LPV 40 mg / r 10 mg, granules dans gélule','LPV 200mg / r 50mg, tab.','TDF 300 mg / FTC 200 mg / DTG 50 mg, tab.','TDF 300 mg / FTC 200 mg, tab.','TDF 300mg / 3TC 300mg / DTG 50mg, tab.') AND mdd.calculated_end_date > CURRENT_DATE AND mdd.date_stopped IS NULL),
médicament_arv_list AS (
	SELECT
		encounter_id_inclusion,
		STRING_AGG(coded_drug_name, ', ') AS liste_arv
	FROM médicament_arv
	GROUP BY encounter_id_inclusion),
-- The last HIV CTE provides the last HIV test result and date per patient, both routine and confirmation test are considered. Only tests with both a date and result are included. If a confirmation test result is present then it is reported, if a confirmation test result is not present then the routine test result is reported. 
dernière_test_vih AS (
	SELECT patient_id, encounter_id_inclusion, date_test_vih, test_vih
	FROM (
		SELECT
			c.patient_id, 
			c.encounter_id_inclusion, 
			svil.date_test_vih,
			svil.test_vih, 
			ROW_NUMBER() OVER (PARTITION BY c.encounter_id_inclusion ORDER BY svil.date_test_vih DESC) AS rn
		FROM cohorte c 
		LEFT OUTER JOIN (
			SELECT 
				patient_id, 
				CASE WHEN date_de_test_vih_de_confirmation IS NOT NULL AND test_vih_de_confirmation IS NOT NULL THEN date_de_test_vih_de_confirmation WHEN (date_de_test_vih_de_confirmation IS NULL OR test_vih_de_confirmation IS NULL) AND date_de_test_vih_de_routine IS NOT NULL AND test_vih_de_routine IS NOT NULL THEN date_de_test_vih_de_routine ELSE NULL END AS date_test_vih, 
				CASE WHEN date_de_test_vih_de_confirmation IS NOT NULL AND test_vih_de_confirmation IS NOT NULL THEN test_vih_de_confirmation WHEN (date_de_test_vih_de_confirmation IS NULL OR test_vih_de_confirmation IS NULL) AND date_de_test_vih_de_routine IS NOT NULL AND test_vih_de_routine IS NOT NULL THEN test_vih_de_routine ELSE NULL END AS test_vih 
			FROM signes_vitaux_et_laboratoire svil
			WHERE (date_de_test_vih_de_confirmation IS NOT NULL AND test_vih_de_confirmation IS NOT NULL) OR (date_de_test_vih_de_routine IS NOT NULL AND test_vih_de_routine IS NOT NULL)) svil 
			ON c.patient_id = svil.patient_id AND c.date_inclusion <= svil.date_test_vih::date AND COALESCE(c.date_de_sortie, CURRENT_DATE) >= svil.date_test_vih::date) foo
	WHERE rn = 1),	
-- The last CD4 CTE provides the last CD4 result and date per patient. Only tests with both a date and result are included. If the prélèvement date is completed, then this data is reported. If no pélèvement date is completed, then the récéption date is reported. 
dernière_cd4 AS (
	SELECT patient_id, encounter_id_inclusion, date_cd4, résultat_brut_cd4
	FROM (
		SELECT
			c.patient_id, 
			c.encounter_id_inclusion,  
			svil.date_cd4, 
			svil.résultat_brut_cd4,
			ROW_NUMBER() OVER (PARTITION BY c.encounter_id_inclusion ORDER BY svil.date_cd4 DESC) AS rn
		FROM cohorte c 
		LEFT OUTER JOIN (
			SELECT 
				patient_id, 
				COALESCE(date_de_prélèvement_cd4, date_de_récéption_des_résultats_cd4) AS date_cd4, 
				résultat_brut_cd4
			FROM signes_vitaux_et_laboratoire_cd4
			WHERE encounter_id > 14375
			UNION
			SELECT
				patient_id, 
				COALESCE(MAX(date_de_prélèvement_cd4), MAX(date_de_récéption_des_résultats_cd4)) AS date_cd4, 
				MAX(résultat_brut_cd4::int)
			FROM signes_vitaux_et_laboratoire_cd4
			WHERE encounter_id <= 14375
			GROUP BY encounter_id, patient_id) svil 
			ON c.patient_id = svil.patient_id AND c.date_inclusion <= svil.date_cd4::date AND COALESCE(c.date_de_sortie, CURRENT_DATE) >= svil.date_cd4::date
		WHERE svil.date_cd4 IS NOT NULL AND svil.résultat_brut_cd4 IS NOT NULL) foo
	WHERE rn = 1),
-- The last viral load CTE provides the last viral load result and date per patient. Only tests with both a date and result are included. If the prélèvement date is completed, then this data is reported. If no pélèvement date is completed, then the récéption date is reported. 
dernière_charge_virale_vih AS (
	SELECT patient_id, encounter_id_inclusion, date_charge_virale_vih, résultat_brut_charge_virale_vih
	FROM (
		SELECT
			c.patient_id, 
			c.encounter_id_inclusion, 
			svil.date_charge_virale_vih, 
			svil.résultat_brut_charge_virale_vih,
			ROW_NUMBER() OVER (PARTITION BY c.encounter_id_inclusion ORDER BY svil.date_charge_virale_vih DESC) AS rn
		FROM cohorte c 
		LEFT OUTER JOIN (
			SELECT 
				patient_id, 
				COALESCE(date_de_prélèvement_charge_virale_vih, date_de_réception_des_résultats_charge_virale_vih) AS date_charge_virale_vih, 
				résultat_brut_charge_virale_vih
			FROM signes_vitaux_et_laboratoire_charge_virale_vih
			WHERE encounter_id > 14371
			UNION
			SELECT 
    			patient_id, 
				COALESCE(MAX(date_de_prélèvement_charge_virale_vih), MAX(date_de_réception_des_résultats_charge_virale_vih)) AS date_charge_virale_vih,
				MAX(résultat_brut_charge_virale_vih::int) AS résultat_brut_charge_virale_vih
			FROM signes_vitaux_et_laboratoire_charge_virale_vih
			WHERE encounter_id <= 14371
			GROUP BY encounter_id, patient_id) svil 
			ON c.patient_id = svil.patient_id AND c.date_inclusion <= svil.date_charge_virale_vih::date AND COALESCE(c.date_de_sortie, CURRENT_DATE) >= svil.date_charge_virale_vih::date
		WHERE svil.date_charge_virale_vih IS NOT NULL AND svil.résultat_brut_charge_virale_vih IS NOT NULL) foo
	WHERE rn = 1),
-- The last blood pressure CTE extracts the last complete blood pressure measurements reported per cohort enrollment.
dernière_pression_artérielle AS (
    SELECT patient_id, encounter_id_inclusion, date_dernière_pression_artérielle, dernière_pression_artérielle_systolique, dernière_pression_artérielle_diastolique
    FROM (
        SELECT 
            c.patient_id, 
            c.encounter_id_inclusion, 
            svil.date_heure_enregistrée AS date_dernière_pression_artérielle,
            svil.tension_arterielle_systolique AS dernière_pression_artérielle_systolique,
            svil.tension_arterielle_diastolique AS dernière_pression_artérielle_diastolique, 
            ROW_NUMBER() OVER (PARTITION BY c.encounter_id_inclusion ORDER BY svil.date_heure_enregistrée DESC) AS rn
        FROM cohorte c
        LEFT OUTER JOIN signes_vitaux_et_laboratoire svil
            ON c.patient_id = svil.patient_id 
           AND c.date_inclusion <= svil.date_heure_enregistrée::date 
           AND COALESCE(c.date_de_sortie, CURRENT_DATE) >= svil.date_heure_enregistrée::date
        WHERE svil.date_heure_enregistrée IS NOT NULL 
          AND svil.tension_arterielle_systolique IS NOT NULL 
          AND svil.tension_arterielle_diastolique IS NOT NULL
    ) foo
    WHERE rn = 1
),

-- The last BMI CTE extracts the last BMI measurement reported per cohort enrollment.
dernière_imc AS (
    SELECT patient_id, encounter_id_inclusion, date_dernière_imc, dernière_imc
    FROM (
        SELECT
            c.patient_id, 
            c.encounter_id_inclusion, 
            svil.date_heure_enregistrée AS date_dernière_imc,
            svil.indice_de_masse_corporelle AS dernière_imc, 
            ROW_NUMBER() OVER (PARTITION BY c.encounter_id_inclusion ORDER BY svil.date_heure_enregistrée DESC) AS rn
        FROM cohorte c
        LEFT OUTER JOIN signes_vitaux_et_laboratoire svil
            ON c.patient_id = svil.patient_id 
           AND c.date_inclusion <= svil.date_heure_enregistrée::date 
           AND COALESCE(c.date_de_sortie, CURRENT_DATE) >= svil.date_heure_enregistrée::date
        WHERE svil.date_heure_enregistrée IS NOT NULL 
          AND svil.indice_de_masse_corporelle IS NOT NULL
    ) foo
    WHERE rn = 1
),

-- The last HbA1c CTE extracts the last HbA1c measurement reported per cohort enrollment.
dernière_hba1c AS (
    SELECT patient_id, encounter_id_inclusion, date_dernière_hba1c, dernière_hba1c
    FROM (
        SELECT 
            c.patient_id, 
            c.encounter_id_inclusion, 
            COALESCE(svil.date_de_prélèvement, svil.date_heure_enregistrée) AS date_dernière_hba1c,
            svil.hba1c AS dernière_hba1c, 
            ROW_NUMBER() OVER (PARTITION BY c.encounter_id_inclusion ORDER BY COALESCE(svil.date_de_prélèvement, svil.date_heure_enregistrée) DESC) AS rn
        FROM cohorte c
        LEFT OUTER JOIN signes_vitaux_et_laboratoire svil
            ON c.patient_id = svil.patient_id 
           AND c.date_inclusion <= COALESCE(svil.date_de_prélèvement, svil.date_heure_enregistrée)::date 
           AND COALESCE(c.date_de_sortie, CURRENT_DATE) >= COALESCE(svil.date_de_prélèvement, svil.date_heure_enregistrée)::date
        WHERE COALESCE(svil.date_de_prélèvement, svil.date_heure_enregistrée) IS NOT NULL 
          AND svil.hba1c IS NOT NULL
    ) foo
    WHERE rn = 1
),

-- The last glycémie CTE extracts the last glycémie measurement reported per cohort enrollment.
dernière_glycémie AS (
    SELECT patient_id, encounter_id_inclusion, date_dernière_glycémie, dernière_glycémie
    FROM (
        SELECT 
            c.patient_id, 
            c.encounter_id_inclusion, 
            COALESCE(svil.date_de_prélèvement, svil.date_heure_enregistrée) AS date_dernière_glycémie,
            svil.glycémie_aléatoire AS dernière_glycémie, 
            ROW_NUMBER() OVER (PARTITION BY c.encounter_id_inclusion ORDER BY COALESCE(svil.date_de_prélèvement, svil.date_heure_enregistrée) DESC) AS rn
        FROM cohorte c
        LEFT OUTER JOIN signes_vitaux_et_laboratoire svil
            ON c.patient_id = svil.patient_id 
           AND c.date_inclusion <= COALESCE(svil.date_de_prélèvement, svil.date_heure_enregistrée)::date 
           AND COALESCE(c.date_de_sortie, CURRENT_DATE) >= COALESCE(svil.date_de_prélèvement, svil.date_heure_enregistrée)::date
        WHERE COALESCE(svil.date_de_prélèvement, svil.date_heure_enregistrée) IS NOT NULL 
          AND svil.glycémie_aléatoire IS NOT NULL
    ) foo
    WHERE rn = 1
),

-- The MNT VIH TB form CTE extracts the last MNT VIH TB visit data per cohort enrollment to look at if there are values reported for pregnancy, family planning, hospitalization, missed medication, seizures, or asthma/COPD exacerbations repoted at the last visit. 
dernière_visite AS (
	SELECT patient_id, encounter_id_inclusion, enceinte_dernière_visite, allaitante_dernière_visite, hospitalisé_signalée_dernière_visite, prise_de_médicaments_oubliée_signalée_dernière_visite, convulsions_signalée_dernière_visite, exacerbation_signalée_dernière_visite, nbr_exacerbation_signalée_dernière_visite
	FROM (
		SELECT 
			c.patient_id,
			c.encounter_id_inclusion,
			CASE WHEN n.enceinte_actuellement = 'Oui' THEN 'Oui' END AS enceinte_dernière_visite,
			CASE WHEN n.allaitante = 'Oui' THEN 'Oui' END AS allaitante_dernière_visite,
			CASE WHEN n.hospitalisé_depuis_la_dernière_visite = 'Oui' THEN 'Oui' END AS hospitalisé_signalée_dernière_visite,
			CASE WHEN n.prise_de_médicaments_oubliée_au_cours_des_7_derniers_jours = 'Oui' THEN 'Oui' END AS prise_de_médicaments_oubliée_signalée_dernière_visite,
			CASE WHEN n.convulsions_depuis_la_derniere_consultation = 'Oui' THEN 'Oui' END AS convulsions_signalée_dernière_visite,
			CASE WHEN n.exacerbation_par_semaine IS NOT NULL AND n.exacerbation_par_semaine > 0 THEN 'Oui' END AS exacerbation_signalée_dernière_visite,
			n.exacerbation_par_semaine AS nbr_exacerbation_signalée_dernière_visite,
			ROW_NUMBER() OVER (PARTITION BY c.encounter_id_inclusion ORDER BY n.date DESC) AS rn
		FROM cohorte c
		LEFT OUTER JOIN mnt_vih_tb n
			ON c.patient_id = n.patient_id AND c.date_inclusion <= n.date::date AND COALESCE(c.date_de_sortie, CURRENT_DATE) >= n.date::date) foo
	WHERE rn = 1),
-- The last gravité de l'asthme CTE extracts the last asthma severity reported per cohort enrollment.
dernière_gravité_asthme AS (
	SELECT patient_id, encounter_id_inclusion, date, dernière_gravité_asthme
	FROM (
		SELECT 
			c.patient_id, 
			c.encounter_id_inclusion,
			n.date::date,
			n.gravité_de_l_asthme AS dernière_gravité_asthme, 
			ROW_NUMBER() OVER (PARTITION BY c.encounter_id_inclusion ORDER BY n.date DESC) AS rn
		FROM cohorte c
		LEFT OUTER JOIN mnt_vih_tb n
			ON c.patient_id = n.patient_id AND c.date_inclusion <= n.date::date AND COALESCE(c.date_de_sortie, CURRENT_DATE) >= n.date::date
		WHERE n.gravité_de_l_asthme IS NOT NULL) foo
	WHERE rn = 1),
-- The hospitalised CTE checks there is a hospitlisation reported in visits taking place in the last 6 months. 
hospitalisé_dernière_6m AS (
	SELECT 
		DISTINCT ON (c.patient_id, c.encounter_id_inclusion) c.patient_id, 
		c.encounter_id_inclusion, 
		COUNT(n.hospitalisé_depuis_la_dernière_visite) AS nb_hospitalisé_dernière_6m, 
		CASE WHEN n.hospitalisé_depuis_la_dernière_visite IS NOT NULL THEN 'Oui' ELSE 'Non' END AS hospitalisé_dernière_6m
	FROM cohorte c
	LEFT OUTER JOIN mnt_vih_tb n
		ON c.patient_id = n.patient_id AND c.date_inclusion <= n.date::date AND COALESCE(c.date_de_sortie, CURRENT_DATE) >= n.date::date
	WHERE n.hospitalisé_depuis_la_dernière_visite = 'Oui' and n.date <= CURRENT_DATE and n.date >= CURRENT_DATE - INTERVAL '6 months'
	GROUP BY c.patient_id, c.encounter_id_inclusion, n.hospitalisé_depuis_la_dernière_visite)
-- Main query --
SELECT
	pi."Patient_Identifier",
	c.patient_id,
	c.encounter_id_inclusion,
	pa."Identifiant_cohorte",
	pdd.age AS age_actuel,
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
	END AS groupe_age_actuel,
	EXTRACT(YEAR FROM (SELECT age(c.date_inclusion, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')))) AS age_inclusion,
	CASE 
		WHEN EXTRACT(YEAR FROM (SELECT age(c.date_inclusion, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy'))))::int <= 4 THEN '0-4'
		WHEN EXTRACT(YEAR FROM (SELECT age(c.date_inclusion, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy'))))::int >= 5 AND EXTRACT(YEAR FROM (SELECT age(c.date_inclusion, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')))) <= 14 THEN '05-14'
		WHEN EXTRACT(YEAR FROM (SELECT age(c.date_inclusion, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy'))))::int >= 15 AND EXTRACT(YEAR FROM (SELECT age(c.date_inclusion, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')))) <= 24 THEN '15-24'
		WHEN EXTRACT(YEAR FROM (SELECT age(c.date_inclusion, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy'))))::int >= 25 AND EXTRACT(YEAR FROM (SELECT age(c.date_inclusion, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')))) <= 34 THEN '25-34'
		WHEN EXTRACT(YEAR FROM (SELECT age(c.date_inclusion, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy'))))::int >= 35 AND EXTRACT(YEAR FROM (SELECT age(c.date_inclusion, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')))) <= 44 THEN '35-44'
		WHEN EXTRACT(YEAR FROM (SELECT age(c.date_inclusion, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy'))))::int >= 45 AND EXTRACT(YEAR FROM (SELECT age(c.date_inclusion, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')))) <= 54 THEN '45-54'
		WHEN EXTRACT(YEAR FROM (SELECT age(c.date_inclusion, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy'))))::int >= 55 AND EXTRACT(YEAR FROM (SELECT age(c.date_inclusion, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')))) <= 64 THEN '55-64'
		WHEN EXTRACT(YEAR FROM (SELECT age(c.date_inclusion, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy'))))::int >= 65 THEN '65+'
		ELSE NULL
	END AS groupe_age_inclusion,
	pdd.gender AS sexe,
	CASE 
		WHEN pa."Civil_status" = 'Never married' THEN 'Célibataire' 
		WHEN pa."Civil_status" = 'Living together' THEN 'Concubinage' 
		WHEN pa."Civil_status" = 'Married' THEN 'Marié' 
		WHEN pa."Civil_status" = 'Widowed' THEN 'Veuf(ve)' 
		WHEN pa."Civil_status" = 'Separated' THEN 'Séparé' 
		WHEN pa."Civil_status" = 'Other' THEN 'Autre' 
	ELSE NULL END AS statut_civil,
	CASE 
		WHEN pa."Education_level" = 'No formal education' THEN 'Pas éducation formelle'
		WHEN pa."Education_level" = 'Intermittent schooling' THEN 'Scolarisation intermittente' 
		WHEN pa."Education_level" = 'Primary school education' THEN 'École primaire' 
		WHEN pa."Education_level" = 'High school' THEN 'École secondaire' 
		WHEN pa."Education_level" = 'College/University' THEN 'Collège/Université' 
	ELSE NULL END AS niveau_education,
	CASE 
		WHEN pa."Occupation" = 'Employed' THEN 'oui - rémunéré'
		WHEN pa."Occupation" = 'Retired' THEN 'oui - non rémunéré'
		WHEN pa."Occupation" = 'No' THEN 'non, Autre'
	ELSE NULL END AS activite,
	CASE 
		WHEN pa."Living_conditions" = 'Unstable accommodation' THEN 'Logement instable'
		WHEN pa."Living_conditions" = 'Stable accommodation' THEN 'Logement stable'
		WHEN pa."Living_conditions" LIKE 'Lives at relatives/friends' THEN 'Vit chez des parents/amis'
		WHEN pa."Living_conditions" = 'In transit' THEN 'En transit/déménagement'
		WHEN pa."Living_conditions" = 'Homeless' THEN 'Sans domicile fixe'
		WHEN pa."Living_conditions" = 'Other' THEN 'Autre'
	ELSE NULL END AS condition_habitation,
	c.date_inclusion AS date_inclusion,
	CASE WHEN c.date_de_sortie IS NULL THEN 'Oui' END AS en_cohorte,
	CASE WHEN ((DATE_PART('year', CURRENT_DATE) - DATE_PART('year', c.date_inclusion)) * 12 + (DATE_PART('month', CURRENT_DATE) - DATE_PART('month', c.date_inclusion))) >= 6 AND c.date_de_sortie IS NULL THEN 'Oui' END AS en_cohorte_6m,
	CASE WHEN ((DATE_PART('year', CURRENT_DATE) - DATE_PART('year', c.date_inclusion)) * 12 + (DATE_PART('month', CURRENT_DATE) - DATE_PART('month', c.date_inclusion))) >= 12 AND c.date_de_sortie IS NULL THEN 'Oui' END AS en_cohorte_12m,
	c.readmission,
	c.lieu_de_visite_inclusion,
	CASE WHEN dp.date_derniere_ptpe IS NOT NULL THEN 'Oui' ELSE NULL END AS ptpe,
	lfl.dernière_fiche_location,
	lf.date_derniere_visite,
	lf.dernière_fiche_type,
	dp.date_derniere_ptpe,
	CASE WHEN GREATEST(
         COALESCE(lf.date_derniere_visite, DATE '1900-01-01'),
         COALESCE(dp.date_derniere_ptpe, DATE '1900-01-01')
       ) < (CURRENT_DATE - INTERVAL '90 DAYS') THEN 'Oui'
  WHEN lf.date_derniere_visite IS NULL AND dp.date_derniere_ptpe IS NULL
    THEN 'Oui' -- aucun enregistrement => considéré sans visite
  ELSE NULL
END AS sans_visite_90j,
	c.date_de_sortie,
	c.statut_de_sortie,
	CASE 
		WHEN lndx.mnt IS NOT NULL AND lndx.tb IS NULL AND lndx.vih IS NULL AND lndx.troubles_de_santé_mentale IS NULL THEN 'MNT' 
		WHEN lndx.mnt IS NULL AND lndx.tb IS NOT NULL AND lndx.vih IS NULL AND lndx.troubles_de_santé_mentale IS NULL THEN 'TB' 
		WHEN lndx.mnt IS NULL AND lndx.tb IS NULL AND lndx.vih IS NOT NULL AND lndx.troubles_de_santé_mentale IS NULL THEN 'VIH' 
		WHEN lndx.mnt IS NULL AND lndx.tb IS NULL AND lndx.vih IS NULL AND lndx.troubles_de_santé_mentale IS NOT NULL THEN 'Santé mentale' 
		WHEN lndx.mnt IS NOT NULL AND lndx.tb IS NULL AND lndx.vih IS NOT NULL AND lndx.troubles_de_santé_mentale IS NULL THEN 'MNT + VIH' 
		WHEN lndx.mnt IS NULL AND lndx.tb IS NOT NULL AND lndx.vih IS NOT NULL AND lndx.troubles_de_santé_mentale IS NULL THEN 'VIH + TB'
		WHEN lndx.mnt IS NOT NULL AND lndx.tb IS NOT NULL AND lndx.vih IS NULL AND lndx.troubles_de_santé_mentale IS NULL THEN 'MNT + TB'
		WHEN lndx.mnt IS NOT NULL AND lndx.tb IS NULL AND lndx.vih IS NULL AND lndx.troubles_de_santé_mentale IS NOT NULL THEN 'MNT + Santé mentale'
		WHEN lndx.mnt IS NULL AND lndx.tb IS NOT NULL AND lndx.vih IS NULL AND lndx.troubles_de_santé_mentale IS NOT NULL THEN 'TB + Santé mentale'
		WHEN lndx.mnt IS NULL AND lndx.tb IS NULL AND lndx.vih IS NOT NULL AND lndx.troubles_de_santé_mentale IS NOT NULL THEN 'VIH + Santé mentale' 
	ELSE NULL END AS cohorte,
	lndx.asthme,
	lndx.drépanocytose,
	lndx.insuffisance_renal_chronique,
	lndx.syndrome_néphrotique,
	lndx.maladie_cardiovasculaire,
	lndx.bronchopneumopathie_chronique_obstructive,
	lndx.diabète_type1,
	CASE WHEN lndx.diabète_type1 IS NOT NULL THEN 'Oui' ELSE NULL END AS diabète_type1_filtre,
	lndx.diabète_type2,
	lndx.hypertension,
	lndx.hypothyroïdie,
	lndx.hyperthyroïdie,
	lndx.épilepsie_focale,
	lndx.épilepsie_généralisée,
	lndx.épilepsie_non_classifiée,
	lndx.tb_pulmonaire,
	lndx.tb_extrapulmonaire,
	lndx.vih,
	CASE WHEN lndx.vih IS NOT NULL THEN 'Oui' ELSE NULL END AS vih_filtre,
	lndx.infection_hep_b,
	CASE WHEN lndx.infection_hep_b IS NOT NULL THEN 'Oui' ELSE NULL END AS infection_hep_b_filtre,
	lndx.infection_hep_c,
	CASE WHEN lndx.infection_hep_c IS NOT NULL THEN 'Oui' ELSE NULL END AS infection_hep_c_filtre,
	lndx.troubles_de_santé_mentale,
	lndx.autre_diagnostic,
	lndl.liste_diagnostic,
	cmcl.liste_comorbidities,
	frp.médecine_traditionnelle,
	frp.tabagisme_passif,
	frp.fumeur,
	frp.consommation_alcool,
	frp.autre_facteurs_risque,
	iarv.date_instauration_arv,
	tarv.traitement_arv AS traitement_arv_fiche,
	CASE WHEN marv.liste_arv IS NOT NULL THEN 'Oui' ELSE NULL END AS traitement_arv_actuellement,
	marv.liste_arv,
	CASE WHEN (lndx.tb_pulmonaire IS NULL AND lndx.tb_extrapulmonaire IS NULL) AND ((marv.liste_arv LIKE '%TDF 300mg / 3TC 300mg / DTG 50mg, tab.%') OR ((marv.liste_arv LIKE '%ABC %' OR marv.liste_arv LIKE '%AZT 60 mg%') AND marv.liste_arv LIKE '%DOLUTEGRAVIR sodium (DTG)%')) THEN 'TAR de première ligne' WHEN (lndx.tb_pulmonaire IS NOT NULL OR lndx.tb_extrapulmonaire IS NOT NULL) AND marv.liste_arv LIKE '%TDF 300mg / 3TC 300mg / DTG 50mg, tab.%' AND marv.liste_arv LIKE '%DOLUTEGRAVIR sodium (DTG)%' THEN 'TAR de première ligne'
	ELSE NULL END AS traitement_arv_médicaments,
	dtv.date_test_vih,
	dtv.test_vih,
	dc.date_cd4, 
	dc.résultat_brut_cd4,
	CASE WHEN dc.résultat_brut_cd4 >= 200 THEN 'Plus de 200 cellules/mL' WHEN dc.résultat_brut_cd4 < 200 THEN 'Moins de 200 cellules/mL' ELSE NULL END AS résultat_seuil_cd4_cellules_ml,
	dcv.date_charge_virale_vih, 
	dcv.résultat_brut_charge_virale_vih, 
	CASE WHEN dcv.résultat_brut_charge_virale_vih >= 1000 THEN 'Plus de 1000 copies/mL' WHEN dcv.résultat_brut_charge_virale_vih < 1000 THEN 'Moins de 1000 copies/mL' ELSE NULL END AS résultat_seuil_charge_virale_vih, 
	dpa.date_dernière_pression_artérielle,
	dpa.dernière_pression_artérielle_systolique,
	dpa.dernière_pression_artérielle_diastolique,
	CASE WHEN dpa.dernière_pression_artérielle_systolique IS NOT NULL AND dpa.dernière_pression_artérielle_diastolique IS NOT NULL THEN CONCAT(dpa.dernière_pression_artérielle_systolique,'/',dpa.dernière_pression_artérielle_diastolique) END AS dernière_pression_artérielle,
	CASE WHEN dpa.dernière_pression_artérielle_systolique <= 140 AND dpa.dernière_pression_artérielle_diastolique <= 90 THEN 'Oui' WHEN dpa.dernière_pression_artérielle_systolique > 140 OR dpa.dernière_pression_artérielle_diastolique > 90 THEN 'Non' END AS dernière_tension_artérielle_controlée,
	dimc.date_dernière_imc,
	dimc.dernière_imc,
	dhba1c.date_dernière_hba1c,
	dhba1c.dernière_hba1c,
	CASE WHEN dhba1c.dernière_hba1c <= 6.5 THEN '0-6.5%' WHEN dhba1c.dernière_hba1c BETWEEN 6.6 AND 8 THEN '6.6-8.0%' WHEN dhba1c.dernière_hba1c > 8 THEN '>8%' END AS dernière_hba1c_seuil,
	dgyl.date_dernière_glycémie,
	dgyl.dernière_glycémie,
	CASE WHEN dhba1c.dernière_hba1c < 8 THEN 'Oui' WHEN dhba1c.dernière_hba1c >= 8 THEN 'Non' WHEN dhba1c.dernière_hba1c IS NULL AND dgyl.dernière_glycémie < 150 THEN 'Oui' WHEN dhba1c.dernière_hba1c IS NULL AND dgyl.dernière_glycémie >= 150 THEN 'No' END AS diabète_contrôlé,
	dv.enceinte_dernière_visite, 
	dv.allaitante_dernière_visite, 
	dv.hospitalisé_signalée_dernière_visite, 
	dv.prise_de_médicaments_oubliée_signalée_dernière_visite, 
	dv.convulsions_signalée_dernière_visite, 
	dv.exacerbation_signalée_dernière_visite, 
	dv.nbr_exacerbation_signalée_dernière_visite,
	dga.dernière_gravité_asthme, 
	hd6m.nb_hospitalisé_dernière_6m, 
	hd6m.hospitalisé_dernière_6m
FROM cohorte c
LEFT OUTER JOIN patient_identifier pi
	ON c.patient_id = pi.patient_id
LEFT OUTER JOIN person_attributes pa
	ON c.patient_id = pa.person_id
LEFT OUTER JOIN person_details_default pdd 
	ON c.patient_id = pdd.person_id
LEFT OUTER JOIN dernière_ptpe dp
	ON c.encounter_id_inclusion = dp.encounter_id_inclusion
LEFT OUTER JOIN dernière_fiche lf
	ON c.encounter_id_inclusion = lf.encounter_id_inclusion
LEFT OUTER JOIN dernière_fiche_location lfl
	ON c.encounter_id_inclusion = lfl.encounter_id_inclusion
LEFT OUTER JOIN diagnostic_cohorte_pivot lndx
	ON c.encounter_id_inclusion = lndx.encounter_id_inclusion
LEFT OUTER JOIN diagnostic_cohorte_liste lndl
	ON c.encounter_id_inclusion = lndl.encounter_id_inclusion
LEFT OUTER JOIN comorbidités_cohorte_liste cmcl 
	ON c.encounter_id_inclusion = cmcl.encounter_id_inclusion
LEFT OUTER JOIN facteurs_risque_pivot frp 
	ON c.encounter_id_inclusion = frp.encounter_id_inclusion
LEFT OUTER JOIN instauration_arv iarv 
	ON c.encounter_id_inclusion = iarv.encounter_id_inclusion
LEFT OUTER JOIN traitement_arv tarv 
	ON c.encounter_id_inclusion = tarv.encounter_id_inclusion
LEFT OUTER JOIN médicament_arv_list marv 
	ON c.encounter_id_inclusion = marv.encounter_id_inclusion
LEFT OUTER JOIN dernière_test_vih dtv
	ON c.encounter_id_inclusion = dtv.encounter_id_inclusion
LEFT OUTER JOIN dernière_cd4 dc
	ON c.encounter_id_inclusion = dc.encounter_id_inclusion
LEFT OUTER JOIN dernière_charge_virale_vih dcv
	ON c.encounter_id_inclusion = dcv.encounter_id_inclusion
LEFT OUTER JOIN dernière_pression_artérielle dpa
	ON c.encounter_id_inclusion = dpa.encounter_id_inclusion
LEFT OUTER JOIN dernière_imc dimc
	ON c.encounter_id_inclusion = dimc.encounter_id_inclusion
LEFT OUTER JOIN dernière_hba1c dhba1c
	ON c.encounter_id_inclusion = dhba1c.encounter_id_inclusion
LEFT OUTER JOIN dernière_glycémie dgyl 
	ON c.encounter_id_inclusion = dgyl.encounter_id_inclusion
LEFT OUTER JOIN dernière_visite dv 
	ON c.encounter_id_inclusion = dv.encounter_id_inclusion
LEFT OUTER JOIN dernière_gravité_asthme dga 
	ON c.encounter_id_inclusion = dga.encounter_id_inclusion
LEFT OUTER JOIN hospitalisé_dernière_6m hd6m 
	ON c.encounter_id_inclusion = hd6m.encounter_id_inclusion;