WITH initial AS (
    SELECT 
        patient_id, 
        encounter_id AS initial_encounter_id, 
        visit_location AS initial_visit_location,
        date AS initial_visit_date, 
        DENSE_RANK() OVER (PARTITION BY patient_id ORDER BY date) AS initial_visit_order,
        LEAD(date) OVER (PARTITION BY patient_id ORDER BY date) AS next_initial_visit_date
    FROM public.ncd_initial_visit
),
cohort AS (
    SELECT
        i.patient_id, 
        i.initial_encounter_id, 
        i.initial_visit_location, 
        i.initial_visit_date,
        CASE WHEN i.initial_visit_order > 1 THEN 'Yes' END AS readmission, 
        d.encounter_id AS discharge_encounter_id, 
        d.discharge_date, 
        d.patient_outcome
    FROM initial i
    LEFT JOIN (
        SELECT 
            patient_id, 
            encounter_id, 
            COALESCE(discharge_date::date, date::date) AS discharge_date, 
            patient_outcome 
        FROM public.ncd_discharge_visit
    ) d 
    ON i.patient_id = d.patient_id 
       AND d.discharge_date >= i.initial_visit_date 
       AND (d.discharge_date < i.next_initial_visit_date OR i.next_initial_visit_date IS NULL)
),
last_form_location AS (    
    SELECT DISTINCT ON (c.patient_id, c.initial_encounter_id, c.initial_visit_date, c.discharge_date) 
        c.initial_encounter_id,
        nvsl.date_created AS last_form_date,
        nvsl.visit_location AS last_form_location
    FROM cohort c
    LEFT OUTER JOIN (
        SELECT 
            patient_id, 
            date_created::date AS date_created, 
            visit_location 
        FROM public.ncd_initial_visit
        UNION 
        SELECT 
            patient_id, 
            date_created::date AS date_created, 
            visit_location 
        FROM public.ncd_discharge_visit
    ) nvsl
    ON c.patient_id = nvsl.patient_id 
       AND c.initial_visit_date <= nvsl.date_created 
       AND COALESCE(c.discharge_date, CURRENT_DATE) >= nvsl.date_created
    WHERE nvsl.visit_location IS NOT NULL
    GROUP BY 
        c.patient_id, c.initial_encounter_id, c.initial_visit_date, c.discharge_date, 
        nvsl.date_created, nvsl.visit_location
    ORDER BY 
        c.patient_id, c.initial_encounter_id, c.initial_visit_date, c.discharge_date, nvsl.date_created DESC
),
last_visit_location AS (    
    SELECT 
        c.initial_encounter_id,
        lfl.last_form_location AS last_visit_location
    FROM cohort c
    LEFT OUTER JOIN last_form_location lfl
    ON c.initial_encounter_id = lfl.initial_encounter_id
),
active_patients AS (
    SELECT
        c.initial_visit_date, 
        c.discharge_date,
        lvl.last_visit_location
    FROM cohort c
    LEFT OUTER JOIN last_visit_location lvl
    ON c.initial_encounter_id = lvl.initial_encounter_id
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
daily_admissions_barakah AS (
    SELECT 
        date_trunc('day', ap.initial_visit_date) AS day,
        COUNT(*) AS patients
    FROM active_patients ap
    WHERE ap.last_visit_location = 'Al Barakah'
    GROUP BY 1
),
daily_admissions_karamah AS (
    SELECT 
        date_trunc('day', ap.initial_visit_date) AS day,
        COUNT(*) AS patients
    FROM active_patients ap
    WHERE ap.last_visit_location = 'Tal Al Karamah'
    GROUP BY 1
),
daily_exits_total AS (
    SELECT
        date_trunc('day', ap.discharge_date) AS day,
        COUNT(*) AS patients
    FROM active_patients ap
    GROUP BY 1
),
daily_exits_barakah AS (
    SELECT
        date_trunc('day', ap.discharge_date) AS day,
        COUNT(*) AS patients
    FROM active_patients ap
    WHERE ap.last_visit_location = 'Al Barakah'
    GROUP BY 1
),
daily_exits_karamah AS (
    SELECT
        date_trunc('day', ap.discharge_date) AS day,
        COUNT(*) AS patients
    FROM active_patients ap
    WHERE ap.last_visit_location = 'Tal Al Karamah'
    GROUP BY 1
),
daily_active_patients AS (
    SELECT 
        dr.day AS reporting_day,
        SUM(dat.patients) OVER (ORDER BY dr.day ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_admissions_total,
        COALESCE(SUM(det.patients) OVER (ORDER BY dr.day ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 0) AS cumulative_exits_total, 
        COALESCE(SUM(de2.patients) OVER (ORDER BY dr.day ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 0) AS cumulative_exits_barakah, 
        COALESCE(SUM(de3.patients) OVER (ORDER BY dr.day ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 0) AS cumulative_exits_karamah, 
        (SUM(dat.patients) OVER (ORDER BY dr.day ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) -
         COALESCE(SUM(det.patients) OVER (ORDER BY dr.day ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 0)) AS active_patients_total,
        (SUM(da2.patients) OVER (ORDER BY dr.day ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) -
         COALESCE(SUM(de2.patients) OVER (ORDER BY dr.day ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 0)) AS active_patients_barakah,
        (SUM(da3.patients) OVER (ORDER BY dr.day ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) -
         COALESCE(SUM(de3.patients) OVER (ORDER BY dr.day ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 0)) AS active_patients_karamah,
        CASE 
            WHEN dr.day = (date_trunc('MONTH', dr.day) + INTERVAL '1 MONTH - 1 day') THEN 1
        END AS last_day_of_month
    FROM day_range dr
    LEFT OUTER JOIN daily_admissions_total dat ON dr.day = dat.day
    LEFT OUTER JOIN daily_admissions_barakah da2 ON dr.day = da2.day
    LEFT OUTER JOIN daily_admissions_karamah da3 ON dr.day = da3.day
    LEFT OUTER JOIN daily_exits_total det ON dr.day = det.day
    LEFT OUTER JOIN daily_exits_barakah de2 ON dr.day = de2.day
    LEFT OUTER JOIN daily_exits_karamah de3 ON dr.day = de3.day
)
-- Main query --
SELECT 
    dap.reporting_day,
    dap.active_patients_total,
    dap.active_patients_barakah,
    dap.active_patients_karamah
FROM daily_active_patients dap
WHERE dap.last_day_of_month = 1 
  AND dap.reporting_day > date_trunc('MONTH', CURRENT_DATE) - INTERVAL '1 year'
GROUP BY dap.reporting_day, dap.active_patients_total, dap.active_patients_barakah, dap.active_patients_karamah;
