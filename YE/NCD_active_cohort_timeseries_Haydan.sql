WITH initial AS (
    SELECT 
        patient_id, 
        encounter_id AS initial_encounter_id,
        date_of_visit AS initial_visit_date, 
        DENSE_RANK() OVER (PARTITION BY patient_id ORDER BY date_of_visit) AS initial_visit_order,
        LEAD(date_of_visit) OVER (PARTITION BY patient_id ORDER BY date_of_visit) AS next_initial_visit_date
    FROM ncd WHERE visit_type = 'Initial visit'
),

cohort AS (
    SELECT
        i.patient_id, 
        i.initial_encounter_id, 
        i.initial_visit_date,
        CASE WHEN i.initial_visit_order > 1 THEN 'Yes' END AS readmission, 
        d.encounter_id AS discharge_encounter_id, 
        d.discharge_date, 
        d.discharge_status
    FROM initial i
    LEFT JOIN (
        SELECT 
            patient_id, 
            encounter_id, 
            COALESCE(discharge_date::date, date_of_visit::date) AS discharge_date, 
            discharge_status 
        FROM ncd WHERE visit_type = 'discharge visit'
    ) d 
    ON i.patient_id = d.patient_id 
       AND d.discharge_date >= i.initial_visit_date 
       AND (d.discharge_date < i.next_initial_visit_date OR i.next_initial_visit_date IS NULL)
),

active_patients AS (
    SELECT
        c.initial_visit_date, 
        c.discharge_date
    FROM cohort c
),

range_values AS (
    SELECT 
        date_trunc('day', MIN(ap.initial_visit_date)) AS minval,
        date_trunc('day', MAX(CURRENT_DATE)) AS maxval
    FROM active_patients ap
),

day_range AS (
    SELECT 
        generate_series(minval, maxval, '1 day'::interval) AS day
    FROM range_values
),

daily_admissions_total AS (
    SELECT 
        date_trunc('day', ap.initial_visit_date) AS day,
        COUNT(*) AS patients
    FROM active_patients ap
    GROUP BY 1
),

daily_exits_total AS (
    SELECT
        date_trunc('day', ap.discharge_date) AS day,
        COUNT(*) AS patients
    FROM active_patients ap
    GROUP BY 1
),

daily_active_patients AS (
    SELECT 
        dr.day AS reporting_day,

        SUM(dat.patients) OVER (
            ORDER BY dr.day ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_admissions_total,

        COALESCE(
            SUM(det.patients) OVER (
                ORDER BY dr.day ASC
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ), 0
        ) AS cumulative_exits_total,

        (
            SUM(dat.patients) OVER (
                ORDER BY dr.day ASC
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )
            -
            COALESCE(
                SUM(det.patients) OVER (
                    ORDER BY dr.day ASC
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ), 0
            )
        ) AS active_patients_total,

        CASE 
            WHEN dr.day = (date_trunc('MONTH', dr.day) + INTERVAL '1 MONTH - 1 day') THEN 1
        END AS last_day_of_month

    FROM day_range dr
    LEFT OUTER JOIN daily_admissions_total dat
        ON dr.day = dat.day
    LEFT OUTER JOIN daily_exits_total det
        ON dr.day = det.day
)

SELECT 
    dap.reporting_day,
    dap.active_patients_total
FROM daily_active_patients dap
WHERE dap.last_day_of_month = 1
  AND dap.reporting_day > date_trunc('MONTH', CURRENT_DATE) - INTERVAL '1 year'
GROUP BY dap.reporting_day, dap.active_patients_total
ORDER BY dap.reporting_day;