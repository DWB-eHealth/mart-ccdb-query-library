-- The initial and cohort CTEs build the frame for patients entering and exiting the cohort. This frame is based on the HepC form with visit type of 'initial visit'. The query takes all initial visit dates and matches the next initial visit date (if present) or the current date. 
WITH initial AS (
	SELECT 
		patient_id, encounter_id AS initial_encounter_id, visit_location AS initial_visit_location, date AS initial_visit_date, DENSE_RANK () OVER (PARTITION BY patient_id ORDER BY date) AS initial_visit_order, LEAD (date) OVER (PARTITION BY patient_id ORDER BY date) AS next_initial_visit_date
	FROM hepatitis_c WHERE visit_type = 'Initial visit'),
cohort AS (
	SELECT
		i1.patient_id, i1.initial_encounter_id, i1.initial_visit_location, i1.initial_visit_date, CASE WHEN i1.initial_visit_order > 1 THEN 'Yes' END readmission, COALESCE(i2.initial_visit_date, CURRENT_DATE) AS end_date
	FROM initial i1
	LEFT OUTER JOIN initial i2
		ON i1.patient_id = i2.patient_id AND  i1.initial_visit_order = (i2.initial_visit_order - 1)),
-- The last appointment / form / visit CTEs extract the last appointment or form reported for each patient and identify if a patient currently enrolled in the cohort has not attended their last appointments.  
last_completed_appointment AS (
	SELECT patient_id, appointment_start_time, appointment_service, appointment_location
	FROM (
		SELECT
			patient_id,
			appointment_start_time,
			appointment_service,
			appointment_location,
			ROW_NUMBER() OVER (PARTITION BY patient_id ORDER BY appointment_start_time DESC) AS rn
		FROM patient_appointment_default
		WHERE appointment_start_time < now() AND (appointment_status = 'Completed' OR appointment_status = 'CheckedIn')) foo
	WHERE rn = 1),
last_form AS (
	SELECT initial_encounter_id, last_form_date, last_form_type, last_login_location, last_form_location, last_form
	FROM (
		SELECT 
			c.patient_id,
			c.initial_encounter_id,
			hcvl.date AS last_form_date,
			hcvl.visit_type AS last_form_type,
			hcvl.login_location AS last_login_location,
			hcvl.form_location AS last_form_location,
			hcvl.form_field_path AS last_form,
			ROW_NUMBER() OVER (PARTITION BY hcvl.patient_id ORDER BY hcvl.date DESC, CASE WHEN hcvl.form_field_path = 'Hepatitis C' THEN 1 WHEN hcvl.form_field_path = 'Vitals and Laboratory Information' THEN 2 ELSE 3 END) AS rn
		FROM cohort c
		LEFT OUTER JOIN (
			SELECT patient_id, COALESCE(discharge_date, date) AS date, location_name AS login_location, visit_location AS form_location, visit_type, form_field_path FROM hepatitis_c
			UNION
			SELECT patient_id, COALESCE(date, date_of_sample_collection) AS date, location_name AS login_location, NULL AS form_location, visit_type, form_field_path FROM vitals_and_laboratory_information) hcvl
			ON c.patient_id = hcvl.patient_id AND c.initial_visit_date <= hcvl.date::date AND c.end_date >= hcvl.date::date) foo
	WHERE rn = 1),
last_visit AS (
	SELECT
		lca.patient_id,
		c.initial_encounter_id,
		lca.appointment_start_time::date AS last_appointment_date,
		lca.appointment_service AS last_appointment_service,
		lca.appointment_location AS last_appointment_location,
		lf.last_form_date,
		lf.last_form_type,
		CASE WHEN lca.appointment_start_time >= lf.last_form_date THEN lca.appointment_start_time::date WHEN lca.appointment_start_time < lf.last_form_date THEN lf.last_form_date::date WHEN lca.appointment_start_time IS NOT NULL AND lf.last_form_date IS NULL THEN lca.appointment_start_time::date WHEN lca.appointment_start_time IS NULL AND lf.last_form_date IS NOT NULL THEN lf.last_form_date::date ELSE NULL END AS last_visit_date,
		CASE WHEN lca.appointment_start_time >= lf.last_form_date THEN lca.appointment_service WHEN lca.appointment_start_time < lf.last_form_date THEN lf.last_form_type WHEN lca.appointment_start_time IS NOT NULL AND lf.last_form_date IS NULL THEN lca.appointment_service WHEN lca.appointment_start_time IS NULL AND lf.last_form_date IS NOT NULL THEN lf.last_form_type ELSE NULL END AS last_visit_type
	FROM cohort c
	LEFT OUTER JOIN last_completed_appointment lca 
		ON c.patient_id = lca.patient_id AND c.initial_visit_date <= lca.appointment_start_time AND c.end_date >= lca.appointment_start_time
	LEFT OUTER JOIN last_form lf
		ON c.initial_encounter_id = lf.initial_encounter_id),			
-- The initial treatment CTE extracts treatment start data from the initial visit per cohort enrollment. If multiple initial visits have treatment initiation data, the most recent one is reported. 
treatment_order AS (
	SELECT 
		DISTINCT ON (hc.patient_id, c.initial_encounter_id, hc.treatment_start_date, hc.medication_duration, hepatitis_c_treatment_choice) hc.patient_id,
		c.initial_encounter_id,
		hc.date::date,
		hc.treatment_start_date,
		hc.medication_duration,
		hc.hepatitis_c_treatment_choice,
		CASE WHEN hc.treatment_end_date IS NOT NULL THEN hc.treatment_end_date WHEN hc.treatment_end_date IS NULL AND hc.medication_duration = '12 weeks' THEN (hc.treatment_start_date + INTERVAL '84 days')::date WHEN hc.treatment_end_date IS NULL AND hc.medication_duration = '24 weeks' THEN (hc.treatment_start_date + INTERVAL '168 days')::date END AS treatment_end_date,
		DENSE_RANK () OVER (PARTITION BY c.initial_encounter_id ORDER BY date) AS treatment_order
	FROM hepatitis_c hc
	LEFT OUTER JOIN cohort c
		ON c.patient_id = hc.patient_id AND c.initial_visit_date <= hc.date::date AND c.end_date >= hc.date::date
	WHERE hc.treatment_start_date IS NOT NULL AND hc.medication_duration IS NOT NULL AND hc.hepatitis_c_treatment_choice IS NOT NULL
	ORDER BY hc.patient_id, c.initial_encounter_id, hc.treatment_start_date, hc.medication_duration, hepatitis_c_treatment_choice, hc.date::date ASC),
-- The first viral load CTE extracts the first viral load result from the lab form for each patient in the cohort where test results are present. 
first_vl AS (
	SELECT 
		DISTINCT ON (tto.patient_id, tto.initial_encounter_id) tto.patient_id,
		tto.initial_encounter_id,
		COALESCE(vli.date_of_sample_collection::date, vli.date::date) AS initial_vl_date, 
		vli.hcv_rna_pcr_qualitative_result AS initial_vl_result,
		vli.visit_type,
		vli.initial_test_and_treat_pcr
	FROM treatment_order tto
	LEFT OUTER JOIN vitals_and_laboratory_information vli
		ON tto.patient_id = vli.patient_id AND tto.treatment_start_date >= COALESCE(vli.date_of_sample_collection::date, vli.date::date)
	WHERE COALESCE(vli.date_of_sample_collection::date, vli.date::date) IS NOT NULL AND vli.hcv_rna_pcr_qualitative_result IS NOT NULL AND tto.treatment_order = 1
	ORDER BY tto.patient_id, tto.initial_encounter_id, vli.patient_id, COALESCE(vli.date_of_sample_collection::date, vli.date::date) DESC),
cohort_tt AS (
SELECT
	c.initial_visit_date,
	CASE WHEN fvl.initial_test_and_treat_pcr = 'Yes' THEN fvl.initial_test_and_treat_pcr ELSE NULL END AS test_and_treat,
	CASE WHEN lv.last_visit_type = 'Discharge visit' THEN lv.last_visit_date ELSE NULL END AS discharge_date
FROM cohort c
LEFT OUTER JOIN last_visit lv
	ON c.initial_encounter_id = lv.initial_encounter_id
LEFT OUTER JOIN first_vl fvl 
	ON c.initial_encounter_id = fvl.initial_encounter_id),
-- The following CTEs create the active patient calculation by counting each day the cumulative admissions and exits from the cohort.
range_values AS (
	SELECT 
		date_trunc('day',min(ct.initial_visit_date)) AS minval,
		date_trunc('day',max(CURRENT_DATE)) AS maxval
	FROM cohort_tt ct),
day_range AS (
	SELECT 
		generate_series(minval,maxval,'1 day'::interval) as day
	FROM range_values),   
daily_admissions_total AS (
	SELECT 
		date_trunc('day', ct.initial_visit_date) AS day,
		count(*) AS patients
	FROM cohort_tt ct
	GROUP BY 1),
daily_exits_total AS (
	SELECT
		date_trunc('day',ct.discharge_date) AS day,
		count(*) AS patients
	FROM cohort_tt ct
	GROUP BY 1),
daily_admissions_hepc AS (
	SELECT 
		date_trunc('day', ct.initial_visit_date) AS day,
		count(*) AS patients
	FROM cohort_tt ct
	WHERE ct.test_and_treat IS NULL
	GROUP BY 1),
daily_exits_hepc AS (
	SELECT
		date_trunc('day',ct.discharge_date) AS day,
		count(*) AS patients
	FROM cohort_tt ct
	WHERE ct.test_and_treat IS NULL
	GROUP BY 1),
daily_admissions_tt AS (
	SELECT 
		date_trunc('day', ct.initial_visit_date) AS day,
		count(*) AS patients
	FROM cohort_tt ct
	WHERE ct.test_and_treat = 'Yes'
	GROUP BY 1),
daily_exits_tt AS (
	SELECT
		date_trunc('day',ct.discharge_date) AS day,
		count(*) AS patients
	FROM cohort_tt ct
	WHERE ct.test_and_treat = 'Yes'
	GROUP BY 1),
daily_active_patients AS (
	SELECT 
		dr.day as reporting_day,
		sum(dat.patients) over (order by dr.day asc rows between unbounded preceding and current row) AS cumulative_admissions_total,
		sum(dath.patients) over (order by dr.day asc rows between unbounded preceding and current row) AS cumulative_admissions_hepc,
		sum(datt.patients) over (order by dr.day asc rows between unbounded preceding and current row) AS cumulative_admissions_tt,
		CASE
		    WHEN sum(det.patients) over (order by dr.day asc rows between unbounded preceding and current row) IS NULL
		    THEN 0
		    ELSE sum(det.patients) over (order by dr.day asc rows between unbounded preceding and current row) 
		END AS cumulative_exits_total,
		CASE
		    WHEN sum(deth.patients) over (order by dr.day asc rows between unbounded preceding and current row) IS NULL
		    THEN 0
		    ELSE sum(deth.patients) over (order by dr.day asc rows between unbounded preceding and current row) 
		END AS cumulative_exits_hepc,
		CASE
		    WHEN sum(dett.patients) over (order by dr.day asc rows between unbounded preceding and current row) IS NULL
		    THEN 0
		    ELSE sum(dett.patients) over (order by dr.day asc rows between unbounded preceding and current row) 
		END AS cumulative_exits_tt,
		CASE
		    WHEN sum(det.patients) over (order by dr.day asc rows between unbounded preceding and current row) IS NULL 
			THEN sum(dat.patients) over (order by dr.day asc rows between unbounded preceding and current row)
		    ELSE (sum(dat.patients) over (order by dr.day asc rows between unbounded preceding and current row)-
				sum(det.patients) over (order by dr.day asc rows between unbounded preceding and current row)) 
		END AS active_patients_total,
		CASE
		    WHEN sum(deth.patients) over (order by dr.day asc rows between unbounded preceding and current row) IS NULL 
			THEN sum(dath.patients) over (order by dr.day asc rows between unbounded preceding and current row)
		    ELSE (sum(dath.patients) over (order by dr.day asc rows between unbounded preceding and current row)-
				sum(deth.patients) over (order by dr.day asc rows between unbounded preceding and current row)) 
		END AS active_patients_hepc,
		CASE
		    WHEN sum(dett.patients) over (order by dr.day asc rows between unbounded preceding and current row) IS NULL 
			THEN sum(datt.patients) over (order by dr.day asc rows between unbounded preceding and current row)
		    ELSE (sum(datt.patients) over (order by dr.day asc rows between unbounded preceding and current row)-
				sum(dett.patients) over (order by dr.day asc rows between unbounded preceding and current row)) 
		END AS active_patients_tt,
		CASE 
			WHEN date(dr.day)::date = (date_trunc('MONTH', dr.day) + INTERVAL '1 MONTH - 1 day')::date THEN 1
		END AS last_day_of_month
	FROM day_range dr
	LEFT OUTER JOIN daily_admissions_total dat ON dr.day = dat.day
	LEFT OUTER JOIN daily_exits_total det ON dr.day = det.day
	LEFT OUTER JOIN daily_admissions_hepc dath ON dr.day = dath.day
	LEFT OUTER JOIN daily_exits_hepc deth ON dr.day = deth.day
	LEFT OUTER JOIN daily_admissions_tt datt ON dr.day = datt.day
	LEFT OUTER JOIN daily_exits_tt dett ON dr.day = dett.day)
-- Main query --
SELECT 
	dap.reporting_day,
	dap.active_patients_total,
	dap.active_patients_hepc,
	dap.active_patients_tt
FROM daily_active_patients dap
WHERE dap.last_day_of_month = 1 and dap.reporting_day > date_trunc('MONTH', CURRENT_DATE) - INTERVAL '1 year'
GROUP BY dap.reporting_day, dap.active_patients_total, dap.active_patients_hepc, dap.active_patients_tt;