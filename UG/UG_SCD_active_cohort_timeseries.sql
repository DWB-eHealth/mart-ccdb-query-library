-- The cohort CTE builds the enrollment window for patients entering and exiting the SCD cohort. The enrollment window is based on SCD forms with visit types of 'initial visit' and 'Discharge visit', matching all initial visit forms to the cooresponding patient discharge visit form, if present. If an individual patient has more than one initial visit form, then the patient discharge visit form will be matched to the initial visit form where the patient discharge visit form date falls between the first initial visit date and the following initial visit date.
WITH cohort AS (
	SELECT
		i.patient_id,
		i.initial_visit_date,
		CASE
			WHEN d.discharge_date2 IS NOT NULL THEN d.discharge_date2
			WHEN d.discharge_date2 IS NULL THEN CURRENT_DATE
		END AS discharge_date
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
				scd
			WHERE
				visit_type = 'Initial visit'
		) i
		LEFT JOIN (
			SELECT
				patient_id,
				encounter_id,
				COALESCE(discharge_date, date_of_visit) AS discharge_date2,
				discharge_status
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
-- The following CTEs create the active patient calculation by counting each day the cumulative admissions and exits from the cohort.
range_values AS (
	SELECT
		date_trunc('day', MIN(c.initial_visit_date)) AS minval,
		date_trunc('day', MAX(CURRENT_DATE)) AS maxval
	FROM
		cohort c
),
day_range AS (
	SELECT
		generate_series(minval, maxval, '1 day' :: INTERVAL) AS DAY
	FROM
		range_values
),
daily_admissions_total AS (
	SELECT
		date_trunc('day', c.initial_visit_date) AS DAY,
		COUNT(*) AS patients
	FROM
		cohort c
	GROUP BY
		1
),
daily_exits_total AS (
	SELECT
		date_trunc('day', c.discharge_date) AS DAY,
		COUNT(*) AS patients
	FROM
		cohort c
	GROUP BY
		1
),
daily_active_patients AS (
	SELECT
		dr.day AS reporting_day,
		SUM(dat.patients) over (
			ORDER BY
				dr.day ASC rows BETWEEN unbounded preceding
				AND CURRENT ROW
		) AS cumulative_admissions_total,
		CASE
			WHEN SUM(det.patients) over (
				ORDER BY
					dr.day ASC rows BETWEEN unbounded preceding
					AND CURRENT ROW
			) IS NULL THEN 0
			ELSE SUM(det.patients) over (
				ORDER BY
					dr.day ASC rows BETWEEN unbounded preceding
					AND CURRENT ROW
			)
		END AS cumulative_exits_total,
		CASE
			WHEN SUM(det.patients) over (
				ORDER BY
					dr.day ASC rows BETWEEN unbounded preceding
					AND CURRENT ROW
			) IS NULL THEN SUM(dat.patients) over (
				ORDER BY
					dr.day ASC rows BETWEEN unbounded preceding
					AND CURRENT ROW
			)
			ELSE (
				SUM(dat.patients) over (
					ORDER BY
						dr.day ASC rows BETWEEN unbounded preceding
						AND CURRENT ROW
				) - SUM(det.patients) over (
					ORDER BY
						dr.day ASC rows BETWEEN unbounded preceding
						AND CURRENT ROW
				)
			)
		END AS active_patients_total,
		CASE
			WHEN DATE(dr.day) :: DATE = (
				date_trunc('MONTH', dr.day) + INTERVAL '1 MONTH - 1 day'
			) :: DATE THEN 1
		END AS last_day_of_month
	FROM
		day_range dr
		LEFT OUTER JOIN daily_admissions_total dat ON dr.day = dat.day
		LEFT OUTER JOIN daily_exits_total det ON dr.day = det.day
) -- Main query --
SELECT
	dap.reporting_day,
	dap.active_patients_total
FROM
	daily_active_patients dap
WHERE
	dap.last_day_of_month = 1
GROUP BY
	dap.reporting_day,
	dap.active_patients_total;