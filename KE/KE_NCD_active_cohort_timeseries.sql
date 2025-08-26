-- The cohort CTE builds the enrollment window for patients entering and exiting the NCD cohort. The enrollment window is based on NCD forms with visit types of 'initial visit' and 'patient outcome', matching all initial visit forms to the cooresponding patient outcome visit form, if present. If an individual patient has more than one initial visit form, then the patient outcome visit form will be matched to the initial visit form where the patient outcome visit form date falls between the first initial visit date and the following initial visit date.
WITH cohort AS (
	SELECT
		i.patient_id,
		i.initial_visit_date,
		CASE WHEN d.outcome_date2 IS NOT NULL THEN d.outcome_date2 WHEN d.outcome_date2 IS NULL THEN CURRENT_DATE END AS outcome_date
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
				visit_type = 'Initial visit' AND date_of_visit > '2020-01-01'
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
-- The following CTEs create the active patient calculation by counting each day the cumulative admissions and exits from the cohort.
range_values AS (
	SELECT 
		date_trunc('day',min(c.initial_visit_date)) AS minval,
		date_trunc('day',max(CURRENT_DATE)) AS maxval 
	FROM cohort c),
day_range AS (
	SELECT 
		generate_series(minval,maxval,'1 day'::interval) as day
	FROM range_values),   
daily_admissions_total AS (
	SELECT 
		date_trunc('day', c.initial_visit_date) AS day,
		count(*) AS patients
	FROM cohort c
	GROUP BY 1),
daily_exits_total AS (
	SELECT
		date_trunc('day',c.outcome_date) AS day,
		count(*) AS patients
	FROM cohort c
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
		    WHEN SUM(det.patients) over (order by dr.day asc rows between unbounded preceding and current row) IS NULL 
			THEN SUM(dat.patients) over (order by dr.day asc rows between unbounded preceding and current row)
		    ELSE (SUM(dat.patients) over (order by dr.day asc rows between unbounded preceding and current row)-
				SUM(det.patients) over (order by dr.day asc rows between unbounded preceding and current row)) 
		END AS active_patients_total,
		CASE 
			WHEN date(dr.day)::date = (date_trunc('MONTH', dr.day) + INTERVAL '1 MONTH - 1 day')::date THEN 1
		END AS last_day_of_month
	FROM day_range dr
	LEFT OUTER JOIN daily_admissions_total dat ON dr.day = dat.day
	LEFT OUTER JOIN daily_exits_total det ON dr.day = det.day)
-- Main query --
SELECT 
	dap.reporting_day,
	dap.active_patients_total
FROM daily_active_patients dap
WHERE dap.last_day_of_month = 1 and dap.reporting_day > date_trunc('MONTH', CURRENT_DATE) - INTERVAL '1 year'
GROUP BY dap.reporting_day, dap.active_patients_total;