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
        nvsl.date AS last_form_date,
        nvsl.visit_location AS last_form_location
    FROM cohort c
    LEFT OUTER JOIN (
        SELECT 
            patient_id, 
            date::date AS date, 
            visit_location 
        FROM public.ncd_initial_visit
         UNION 
        SELECT 
            patient_id, 
            date::date AS date, 
            visit_location 
        FROM public.ncd_follow_up_visit
        UNION 
        SELECT 
            patient_id, 
            date::date AS date, 
            visit_location 
        FROM public.ncd_discharge_visit
    ) nvsl
    ON c.patient_id = nvsl.patient_id 
       AND c.initial_visit_date <= nvsl.date 
       AND COALESCE(c.discharge_date, CURRENT_DATE) >= nvsl.date
    WHERE nvsl.visit_location IS NOT NULL
    GROUP BY 
        c.patient_id, c.initial_encounter_id, c.initial_visit_date, c.discharge_date,
        nvsl.date, nvsl.visit_location
    ORDER BY 
        c.patient_id, c.initial_encounter_id, c.initial_visit_date, c.discharge_date, nvsl.date DESC
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
daily_admissions_alhadath AS (
    SELECT 
        date_trunc('day', ap.initial_visit_date) AS day,
        COUNT(*) AS patients
    FROM active_patients ap
    WHERE ap.last_visit_location = 'Al Hadath camp'
    GROUP BY 1
),
daily_admissions_qatar AS (
    SELECT 
        date_trunc('day', ap.initial_visit_date) AS day,
        COUNT(*) AS patients
    FROM active_patients ap
    WHERE ap.last_visit_location = 'Qatar village'
    GROUP BY 1
),
daily_admissions_sosinbat AS (
    SELECT 
        date_trunc('day', ap.initial_visit_date) AS day,
        COUNT(*) AS patients
    FROM active_patients ap
    WHERE ap.last_visit_location = 'Sosinbat camp'
    GROUP BY 1
),
daily_admissions_tadiffv AS (
    SELECT 
        date_trunc('day', ap.initial_visit_date) AS day,
        COUNT(*) AS patients
    FROM active_patients ap
    WHERE ap.last_visit_location = 'Tadiff village'
    GROUP BY 1
),
daily_admissions_tadiffc AS (
    SELECT 
        date_trunc('day', ap.initial_visit_date) AS day,
        COUNT(*) AS patients
    FROM active_patients ap
    WHERE ap.last_visit_location = 'Tadiff camp'
    GROUP BY 1
),
daily_exits_total AS (
    SELECT
        date_trunc('day', ap.discharge_date) AS day,
        COUNT(*) AS patients
    FROM active_patients ap
    GROUP BY 1
),
daily_exits_alhadath AS (
    SELECT
        date_trunc('day', ap.discharge_date) AS day,
        COUNT(*) AS patients
    FROM active_patients ap
    WHERE ap.last_visit_location = 'Al Hadath camp'
    GROUP BY 1
),
daily_exits_qatar AS (
    SELECT
        date_trunc('day', ap.discharge_date) AS day,
        COUNT(*) AS patients
    FROM active_patients ap
    WHERE ap.last_visit_location = 'Qatar village'
    GROUP BY 1
),
daily_exits_sosinbat AS (
    SELECT
        date_trunc('day', ap.discharge_date) AS day,
        COUNT(*) AS patients
    FROM active_patients ap
    WHERE ap.last_visit_location = 'Sosinbat camp'
    GROUP BY 1
),
daily_exits_taddifv AS (
    SELECT
        date_trunc('day', ap.discharge_date) AS day,
        COUNT(*) AS patients
    FROM active_patients ap
    WHERE ap.last_visit_location = 'Tadiff village'
    GROUP BY 1
),
daily_exits_taddifc AS (
    SELECT
        date_trunc('day', ap.discharge_date) AS day,
        COUNT(*) AS patients
    FROM active_patients ap
    WHERE ap.last_visit_location = 'Tadiff camp'
    GROUP BY 1
),
daily_active_patients AS (
    SELECT 
        dr.day AS reporting_day,
        
        SUM(dat.patients) OVER (ORDER BY dr.day ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_admissions_total,
        COALESCE(SUM(det.patients) OVER (ORDER BY dr.day ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 0) AS cumulative_exits_total, 
        COALESCE(SUM(dea.patients) OVER (ORDER BY dr.day ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 0) AS cumulative_exits_alhadath, 
        COALESCE(SUM(deq.patients) OVER (ORDER BY dr.day ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 0) AS cumulative_exits_qatar, 
        COALESCE(SUM(des.patients) OVER (ORDER BY dr.day ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 0) AS cumulative_exits_sosinbat,
        COALESCE(SUM(detv.patients) OVER (ORDER BY dr.day ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 0) AS cumulative_exits_tadiffv,
        COALESCE(SUM(detc.patients) OVER (ORDER BY dr.day ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 0) AS cumulative_exits_tadiffc,
        (SUM(dat.patients) OVER (ORDER BY dr.day ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) -
         COALESCE(SUM(det.patients) OVER (ORDER BY dr.day ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 0)) AS active_patients_total,
        (SUM(daa.patients) OVER (ORDER BY dr.day ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) -
         COALESCE(SUM(dea.patients) OVER (ORDER BY dr.day ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 0)) AS active_patients_alhadath,
        (SUM(daq.patients) OVER (ORDER BY dr.day ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) -
         COALESCE(SUM(deq.patients) OVER (ORDER BY dr.day ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 0)) AS active_patients_qatar,
         (SUM(das.patients) OVER (ORDER BY dr.day ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) -
         COALESCE(SUM(des.patients) OVER (ORDER BY dr.day ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 0)) AS active_patients_sosinbat,
         (SUM(datv.patients) OVER (ORDER BY dr.day ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) -
         COALESCE(SUM(detv.patients) OVER (ORDER BY dr.day ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 0)) AS active_patients_tadiffv,
         (SUM(datc.patients) OVER (ORDER BY dr.day ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) -
         COALESCE(SUM(detc.patients) OVER (ORDER BY dr.day ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 0)) AS active_patients_tadiffc,
        CASE 
            WHEN dr.day = (date_trunc('MONTH', dr.day) + INTERVAL '1 MONTH - 1 day') THEN 1
        END AS last_day_of_month
    FROM day_range dr
    LEFT OUTER JOIN daily_admissions_total dat ON dr.day = dat.day
    LEFT OUTER JOIN daily_admissions_alhadath daa ON dr.day = daa.day
    LEFT OUTER JOIN daily_admissions_qatar daq ON dr.day = daq.day
    LEFT OUTER JOIN daily_admissions_sosinbat das ON dr.day = das.day
    LEFT OUTER JOIN daily_admissions_tadiffv datv ON dr.day = datv.day
    LEFT OUTER JOIN daily_admissions_tadiffc datc ON dr.day = datc.day
    LEFT OUTER JOIN daily_exits_total det ON dr.day = det.day
    LEFT OUTER JOIN daily_exits_alhadath dea ON dr.day = dea.day
    LEFT OUTER JOIN daily_exits_qatar deq ON dr.day = deq.day
    LEFT OUTER JOIN daily_exits_sosinbat des ON dr.day = des.day
    LEFT OUTER JOIN daily_exits_taddifv detv ON dr.day = detv.day
    LEFT OUTER JOIN daily_exits_taddifc detc ON dr.day = detc.day
)
-- Main query --
SELECT 
    dap.reporting_day,
    dap.active_patients_total AS "Total Active",
    dap.active_patients_alhadath AS "Active Al Hadath Camp",
    dap.active_patients_qatar AS "Active Qatar Village",
    dap.active_patients_sosinbat AS "Active Sosinbat Camp",
    dap.active_patients_tadiffv AS " Active Tadiff Village",
    dap.active_patients_tadiffc AS "Active Tadiff Camp"
FROM daily_active_patients dap
WHERE dap.last_day_of_month = 1 
  AND dap.reporting_day > date_trunc('MONTH', CURRENT_DATE) - INTERVAL '1 year'
GROUP BY dap.reporting_day, dap.active_patients_total, dap.active_patients_alhadath, dap.active_patients_qatar, dap.active_patients_sosinbat, dap.active_patients_tadiffv, dap.active_patients_tadiffc;
