-- The first CTEs build the frame for patients entering and exiting the cohort. This frame is based on NCD forms with visit types of 'initial visit' and 'discharge visit'. The query takes all initial visit dates and matches discharge visit dates if the discharge visit date falls between the initial visit date and the next initial visit date (if present).
WITH initial AS (
	SELECT 
		patient_id, encounter_id AS initial_encounter_id, visit_location AS initial_visit_location, date AS initial_visit_date, DENSE_RANK () OVER (PARTITION BY patient_id ORDER BY date) AS initial_visit_order, LEAD (date) OVER (PARTITION BY patient_id ORDER BY date) AS next_initial_visit_date
	FROM ncd WHERE visit_type = 'Initial visit'),
cohort AS (
	SELECT
		i.patient_id, i.initial_encounter_id, i.initial_visit_location, i.initial_visit_date, CASE WHEN i.initial_visit_order > 1 THEN 'Yes' END readmission, d.encounter_id AS discharge_encounter_id, d.discharge_date2 AS discharge_date, d.patient_outcome
	FROM initial i
	LEFT JOIN (SELECT patient_id, encounter_id, COALESCE(discharge_date::date, date::date) AS discharge_date2, patient_outcome FROM ncd WHERE visit_type = 'Discharge visit') d 
		ON i.patient_id = d.patient_id AND (d.discharge_date2 IS NULL OR (d.discharge_date2 >= i.initial_visit_date AND (d.discharge_date2 < i.next_initial_visit_date OR i.next_initial_visit_date IS NULL)))),
-- The last visit location CTE finds the last visit location reported in NCD forms.
last_form_location AS (	
	SELECT 
		DISTINCT ON (c.patient_id, c.initial_encounter_id, c.initial_visit_date, c.discharge_date) c.initial_encounter_id,
		nvsl.date AS last_form_date,
		nvsl.visit_location AS last_form_location
	FROM cohort c
	LEFT OUTER JOIN (SELECT patient_id, date, visit_location FROM NCD UNION SELECT patient_id, date, location_name AS visit_location FROM vitals_and_laboratory_information) nvsl
		ON c.patient_id = nvsl.patient_id AND c.initial_visit_date <= nvsl.date::date AND CASE WHEN c.discharge_date IS NOT NULL THEN c.discharge_date ELSE current_date END >= nvsl.date::date
	WHERE nvsl.visit_location IS NOT NULL
	GROUP BY c.patient_id, c.initial_encounter_id, c.initial_visit_date, c.discharge_date, nvsl.date, nvsl.visit_location
	ORDER BY c.patient_id, c.initial_encounter_id, c.initial_visit_date, c.discharge_date, nvsl.date DESC),
last_completed_appointment AS (
	SELECT patient_id, initial_encounter_id, appointment_start_time::date, appointment_service, appointment_location
	FROM (
		SELECT
			pad.patient_id,
			c.initial_encounter_id,
			pad.appointment_start_time,
			pad.appointment_service,
			pad.appointment_location,
			ROW_NUMBER() OVER (PARTITION BY pad.patient_id ORDER BY pad.appointment_start_time DESC) AS rn
		FROM patient_appointment_default pad
		LEFT OUTER JOIN cohort c
			ON pad.patient_id = c.patient_id AND c.initial_visit_date <= pad.appointment_start_time::date AND COALESCE(c.discharge_date, CURRENT_DATE) >= pad.appointment_start_time::date
		WHERE pad.appointment_start_time < now() AND (pad.appointment_status = 'Completed' OR pad.appointment_status = 'CheckedIn')) foo
	WHERE rn = 1 AND initial_encounter_id IS NOT NULL),
last_visit_location AS (	
	SELECT 
		c.initial_encounter_id,
		CASE WHEN lfl.last_form_date > lca.appointment_start_time THEN lfl.last_form_location WHEN lfl.last_form_date <= lca.appointment_start_time THEN lca.appointment_location WHEN lfl.last_form_date IS NOT NULL AND lca.appointment_start_time IS NULL THEN lfl.last_form_location WHEN lfl.last_form_date IS NULL AND lca.appointment_start_time IS NOT NULL THEN lca.appointment_location ELSE NULL END AS last_visit_location
	FROM cohort c
	LEFT OUTER JOIN last_form_location lfl
		ON c.initial_encounter_id = lfl.initial_encounter_id 
	LEFT OUTER JOIN last_completed_appointment lca
		ON c.patient_id = lca.patient_id AND c.initial_visit_date <= lca.appointment_start_time::date AND CASE WHEN c.discharge_date IS NOT NULL THEN c.discharge_date ELSE current_date END >= lca.appointment_start_time::date),
-- The following CTEs create the active patient calculation by counting each day the cumulative admissions and exits from the cohort.
active_patients AS (
	SELECT
		c.initial_visit_date, 
		c.discharge_date,
		lvl.last_visit_location
	FROM cohort c
	LEFT OUTER JOIN last_visit_location lvl
		ON c.initial_encounter_id = lvl.initial_encounter_id),
range_values AS (
	SELECT 
		date_trunc('day',min(ap.initial_visit_date)) AS minval,
		date_trunc('day',max(CURRENT_DATE)) AS maxval
	FROM active_patients ap),
day_range AS (
	SELECT 
		generate_series(minval,maxval,'1 day'::interval) as day
	FROM range_values),   
daily_admissions_total AS (
	SELECT 
		date_trunc('day', ap.initial_visit_date) AS day,
		count(*) AS patients
	FROM active_patients ap
	GROUP BY 1),
daily_admissions_opd2 AS (
	SELECT 
		date_trunc('day', ap.initial_visit_date) AS day,
		count(*) AS patients
	FROM active_patients ap
	WHERE ap.last_visit_location = 'OPD2'
	GROUP BY 1),
daily_admissions_opd3 AS (
	SELECT 
		date_trunc('day', ap.initial_visit_date) AS day,
		count(*) AS patients
	FROM active_patients ap
	WHERE ap.last_visit_location = 'OPD3'
	GROUP BY 1),
daily_admissions_hoh AS (
	SELECT 
		date_trunc('day', ap.initial_visit_date) AS day,
		count(*) AS patients
	FROM active_patients ap
	WHERE ap.last_visit_location = 'HoH'
	GROUP BY 1),
daily_exits_total AS (
	SELECT
		date_trunc('day',ap.discharge_date) AS day,
		count(*) AS patients
	FROM active_patients ap
	GROUP BY 1),
daily_exits_opd2 AS (
	SELECT
		date_trunc('day',ap.discharge_date) AS day,
		count(*) AS patients
	FROM active_patients ap
	WHERE ap.last_visit_location = 'OPD2'
	GROUP BY 1),
daily_exits_opd3 AS (
	SELECT
		date_trunc('day',ap.discharge_date) AS day,
		count(*) AS patients
	FROM active_patients ap
	WHERE ap.last_visit_location = 'OPD3'
	GROUP BY 1),
daily_exits_hoh AS (
	SELECT
		date_trunc('day',ap.discharge_date) AS day,
		count(*) AS patients
	FROM active_patients ap
	WHERE ap.last_visit_location = 'HoH'
	GROUP BY 1),
daily_active_patients AS (
	SELECT 
		dr.day as reporting_day,
		SUM(dat.patients) over (order by dr.day asc rows between unbounded preceding and current row) AS cumulative_admissions_total,
		CASE
		    WHEN SUM(det.patients) over (order by dr.day asc rows between unbounded preceding and current row) IS NULL
		    THEN 0
		    ELSE SUM(det.patients) over (order by dr.day asc rows between unbounded preceding and current row) 
		END AS cumulative_exits_total, 
		CASE
		    WHEN SUM(de2.patients) over (order by dr.day asc rows between unbounded preceding and current row) IS NULL
		    THEN 0
		    ELSE SUM(de2.patients) over (order by dr.day asc rows between unbounded preceding and current row) 
		END AS cumulative_exits_opd2, 
		CASE
		    WHEN SUM(de3.patients) over (order by dr.day asc rows between unbounded preceding and current row) IS NULL
		    THEN 0
		    ELSE SUM(de3.patients) over (order by dr.day asc rows between unbounded preceding and current row) 
		END AS cumulative_exits_opd3, 
		CASE
		    WHEN SUM(deh.patients) over (order by dr.day asc rows between unbounded preceding and current row) IS NULL
		    THEN 0
		    ELSE SUM(deh.patients) over (order by dr.day asc rows between unbounded preceding and current row) 
		END AS cumulative_exits_hoh, 
		CASE
		    WHEN SUM(det.patients) over (order by dr.day asc rows between unbounded preceding and current row) IS NULL 
			THEN SUM(dat.patients) over (order by dr.day asc rows between unbounded preceding and current row)
		    ELSE (SUM(dat.patients) over (order by dr.day asc rows between unbounded preceding and current row)-
				SUM(det.patients) over (order by dr.day asc rows between unbounded preceding and current row)) 
		END AS active_patients_total,
		CASE
		    WHEN SUM(de2.patients) over (order by dr.day asc rows between unbounded preceding and current row) IS NULL 
			THEN SUM(da2.patients) over (order by dr.day asc rows between unbounded preceding and current row)
		    ELSE (SUM(da2.patients) over (order by dr.day asc rows between unbounded preceding and current row)-
				SUM(de2.patients) over (order by dr.day asc rows between unbounded preceding and current row)) 
		END AS active_patients_opd2,
		CASE
		    WHEN SUM(de3.patients) over (order by dr.day asc rows between unbounded preceding and current row) IS NULL 
			THEN SUM(da3.patients) over (order by dr.day asc rows between unbounded preceding and current row)
		    ELSE (SUM(da3.patients) over (order by dr.day asc rows between unbounded preceding and current row)-
				SUM(de3.patients) over (order by dr.day asc rows between unbounded preceding and current row)) 
		END AS active_patients_opd3,
		CASE
		    WHEN SUM(deh.patients) over (order by dr.day asc rows between unbounded preceding and current row) IS NULL 
			THEN SUM(dah.patients) over (order by dr.day asc rows between unbounded preceding and current row)
		    ELSE (SUM(dah.patients) over (order by dr.day asc rows between unbounded preceding and current row)-
				SUM(deh.patients) over (order by dr.day asc rows between unbounded preceding and current row)) 
		END AS active_patients_hoh,
		CASE 
			WHEN date(dr.day)::date = (date_trunc('MONTH', dr.day) + INTERVAL '1 MONTH - 1 day')::date THEN 1
		END AS last_day_of_month
	FROM day_range dr
	LEFT OUTER JOIN daily_admissions_total dat ON dr.day = dat.day
	LEFT OUTER JOIN daily_admissions_opd2 da2 ON dr.day = da2.day
	LEFT OUTER JOIN daily_admissions_opd3 da3 ON dr.day = da3.day
	LEFT OUTER JOIN daily_admissions_hoh dah ON dr.day = dah.day
	LEFT OUTER JOIN daily_exits_total det ON dr.day = det.day
	LEFT OUTER JOIN daily_exits_opd2 de2 ON dr.day = de2.day
	LEFT OUTER JOIN daily_exits_opd3 de3 ON dr.day = de3.day
	LEFT OUTER JOIN daily_exits_hoh deh ON dr.day = deh.day)
-- Main query --
SELECT 
	dap.reporting_day,
	dap.active_patients_total,
	dap.active_patients_opd2,
	dap.active_patients_opd3,
	dap.active_patients_hoh
FROM daily_active_patients dap
WHERE dap.last_day_of_month = 1 and dap.reporting_day > date_trunc('MONTH', CURRENT_DATE) - INTERVAL '1 year'
GROUP BY dap.reporting_day, dap.active_patients_total, dap.active_patients_opd2, dap.active_patients_opd3, dap.active_patients_hoh;