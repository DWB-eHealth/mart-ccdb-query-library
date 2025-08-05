-- The cohort CTE builds the enrollment window for patients entering and exiting the NCD cohort. The enrollment window is based on NCD forms with visit types of 'initial visit' and 'patient outcome', matching all initial visit forms to the cooresponding patient outcome visit form, if present. If an individual patient has more than one initial visit form, then the patient outcome visit form will be matched to the initial visit form where the patient outcome visit form date falls between the first initial visit date and the following initial visit date.
WITH cohort AS (
    SELECT
        i.patient_id,
        i.initial_encounter_id,
        i.initial_visit_date,
        d.outcome_date2 AS outcome_date
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
                visit_type = 'Initial visit'
                AND date_of_visit > '2020-01-01'
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
-- The visits_dsdm CTE categorizes all visits as DSDM (1) or other (0). 
visits_dsdm AS (
    SELECT
        c.initial_encounter_id,
        n.date_of_visit,
        CASE
            WHEN n.type_of_service_package_provided IN (
                'Facility based DSDM',
                'Community based DSDM'
            ) THEN 1
            ELSE 0
        END AS dsdm
    FROM
        ncd_consultations n
        LEFT OUTER JOIN cohort c ON n.patient_id = c.patient_id
        AND n.date_of_visit >= c.initial_visit_date
        AND n.date_of_visit <= COALESCE(c.outcome_date, CURRENT_DATE)
    WHERE
        c.initial_encounter_id IS NOT NULL
),
-- The dsdm_streak_groups CTE creates a group number (id) to identify consecutive DSDM visits as a single group and creates the MAX date columns needed to identify if the patient is currently receiving DSDM services.
dsdm_streak_groups AS (
    SELECT
        initial_encounter_id,
        date_of_visit,
        dsdm,
        ROW_NUMBER() OVER (
            PARTITION BY initial_encounter_id
            ORDER BY
                date_of_visit
        ) - ROW_NUMBER() OVER (
            PARTITION BY initial_encounter_id,
            dsdm
            ORDER BY
                date_of_visit
        ) AS streak,
        MAX(
            CASE
                WHEN dsdm = 1 THEN date_of_visit
            END
        ) OVER (PARTITION BY initial_encounter_id) AS last_active_date,
        MAX(date_of_visit) OVER (PARTITION BY initial_encounter_id) AS last_row_date
    FROM
        visits_dsdm
),
-- The dsdm_streak_filter CTE filters all visits to just DSDM visits and identifies if the last visist is DSDM. If the last visit is DSDM then the end date is set to the current_date to assume the patient is still part of the DSDM program.
dsdm_streak_filter AS (
    SELECT
        initial_encounter_id,
        date_of_visit,
        streak,
        CASE
            WHEN MAX(date_of_visit) OVER (PARTITION BY initial_encounter_id, streak) = last_active_date
            AND last_active_date = last_row_date THEN 1
            ELSE 0
        END AS ends_in_last_row
    FROM
        dsdm_streak_groups
    WHERE
        dsdm = 1
),
-- The dsdm_streaks CTE summarizes each DSDM enrollment streak with the start and end date. 
dsdm_streaks AS (
    SELECT
        initial_encounter_id,
        MIN(date_of_visit) AS dsdm_start,
        CASE
            WHEN MAX(ends_in_last_row) = 1 THEN CURRENT_DATE
            ELSE MAX(date_of_visit)
        END AS dsdm_end
    FROM
        dsdm_streak_filter
    GROUP BY
        initial_encounter_id,
        streak
    ORDER BY
        initial_encounter_id,
        dsdm_start
),
-- The following CTEs create the active patient calculation by counting each day the cumulative admissions and exits from the cohort.
range_values AS (
    SELECT
        date_trunc('day', MIN(ds.dsdm_start)) AS minval,
        date_trunc('day', MAX(CURRENT_DATE)) AS maxval
    FROM
        dsdm_streaks ds
),
day_range AS (
    SELECT
        generate_series(minval, maxval, '1 day' :: INTERVAL) AS DAY
    FROM
        range_values
),
daily_admissions_total AS (
    SELECT
        date_trunc('day', ds.dsdm_start) AS DAY,
        COUNT(*) AS patients
    FROM
        dsdm_streaks ds
    GROUP BY
        1
),
daily_exits_total AS (
    SELECT
        date_trunc('day', ds.dsdm_end) AS DAY,
        COUNT(*) AS patients
    FROM
        dsdm_streaks ds
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
    AND dap.reporting_day > date_trunc('MONTH', CURRENT_DATE) - INTERVAL '1 year'
GROUP BY
    dap.reporting_day,
    dap.active_patients_total;