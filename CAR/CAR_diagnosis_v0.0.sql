-- The first CTE build the frame for patients entering and exiting the cohort. This frame is based on MNT VIH TB form with visit types of 'Inclusion' and 'Sortie'. For each patient, the query takes all initial visit dates and matches the discharge visit date occuring after the initial visit. If a patient has multiple initial visits and discharge visits, the match happens sequentially based on the date of visit (e.g. the first initial visit is matched to the first discharge, and so on). If the patient does not have a discharge visit, then the discharge information is empty until completed. 
WITH inclusion AS (
	SELECT 
		patient_id, encounter_id AS encounter_id_inclusion, lieu_de_visite AS lieu_de_visite_inclusion, date AS date_inclusion, DENSE_RANK () OVER (PARTITION BY patient_id ORDER BY date) AS inclusion_visit_order, LEAD (date) OVER (PARTITION BY patient_id ORDER BY date) AS date_inclusion_suivi
	FROM mnt_vih_tb WHERE type_de_visite = 'Inclusion'),
cohorte AS (
	SELECT
		i.patient_id, i.encounter_id_inclusion, i.lieu_de_visite_inclusion, i.date_inclusion, CASE WHEN i.inclusion_visit_order > 1 THEN 'Oui' END readmission, d.encounter_id AS encounter_id_sortie, CASE WHEN d.date_de_sortie IS NOT NULL THEN d.date_de_sortie WHEN d.date_de_sortie IS NULL THEN d.date ELSE NULL END AS date_de_sortie, d.statut_de_sortie AS statut_de_sortie
	FROM inclusion i
	LEFT JOIN (SELECT patient_id, date, encounter_id, date_de_sortie, statut_de_sortie FROM mnt_vih_tb WHERE type_de_visite = 'Sortie') d 
		ON i.patient_id = d.patient_id AND d.date >= i.date_inclusion AND (d.date < i.date_inclusion_suivi OR i.date_inclusion_suivi IS NULL)),
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
-- The last visit location CTE finds the last visit location reported in clinical forms (including MNT VIH TB, PTPE).
dernière_fiche_location AS (	
	SELECT 
		DISTINCT ON (c.patient_id, c.encounter_id_inclusion, c.date_inclusion, c.date_de_sortie) c.encounter_id_inclusion,
		forms.lieu_de_visite AS dernière_fiche_location
	FROM cohorte c
	LEFT OUTER JOIN (SELECT patient_id, CASE WHEN type_de_visite = 'Sortie' AND date_de_sortie IS NOT NULL THEN date_de_sortie ELSE date END AS date, lieu_de_visite FROM mnt_vih_tb UNION 
	SELECT patient_id, date, lieu_de_visite FROM ptpe) forms
		ON c.patient_id = forms.patient_id AND c.date_inclusion <= forms.date::date AND CASE WHEN c.date_de_sortie IS NOT NULL THEN c.date_de_sortie ELSE current_date END >= forms.date::date
	WHERE forms.lieu_de_visite IS NOT NULL
	GROUP BY c.patient_id, c.encounter_id_inclusion, c.date_inclusion, c.date_de_sortie, forms.date, forms.lieu_de_visite
	ORDER BY c.patient_id, c.encounter_id_inclusion, c.date_inclusion, c.date_de_sortie, forms.date DESC)
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
	c.date_inclusion AS date_inclusion,
	CASE WHEN c.date_de_sortie IS NULL THEN 'Oui' END AS en_cohorte,
	c.readmission,
	c.lieu_de_visite_inclusion,
	lfl.dernière_fiche_location,	
	c.date_de_sortie,
	c.statut_de_sortie,
	dc.diagnostic
FROM diagnostic_cohorte dc
LEFT OUTER JOIN cohorte c
	ON dc.encounter_id_inclusion = c.encounter_id_inclusion
LEFT OUTER JOIN patient_identifier pi
	ON c.patient_id = pi.patient_id
LEFT OUTER JOIN person_attributes pa
	ON c.patient_id = pa.person_id
LEFT OUTER JOIN person_details_default pdd 
	ON c.patient_id = pdd.person_id
LEFT OUTER JOIN dernière_fiche_location lfl
	ON c.encounter_id_inclusion = lfl.encounter_id_inclusion;
