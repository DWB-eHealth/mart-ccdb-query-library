WITH RECURSIVE last_days AS (
    -- Get the last day of the current month and earlier months
    SELECT 
        DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 day' AS last_day
    UNION ALL
    SELECT 
        DATE_TRUNC('month', MIN(iv.date)) - INTERVAL '1 day'
    FROM 
        public.ncd_initial_visit iv
    WHERE 
        iv.date IS NOT NULL
    UNION ALL
    SELECT 
        DATE_TRUNC('month', MIN(dv.date)) - INTERVAL '1 day'
    FROM 
        public.ncd_discharge_visit dv
    WHERE 
        dv.date IS NOT NULL
),
recursive_months AS (
    -- Recursively go back in time, calculating each last day of the month until the minimum admission or discharge date
    SELECT 
        MAX(last_day) AS last_day
    FROM last_days
    UNION ALL
    SELECT 
        DATE_TRUNC('month', last_day) - INTERVAL '1 day'
    FROM recursive_months
    WHERE 
        DATE_TRUNC('month', last_day) - INTERVAL '1 day' >= 
        (SELECT MIN(last_day) FROM last_days)
),
-- Calculate new admissions and discharges within each month along with locations
monthly_data AS (
    SELECT 
        rm.last_day,
        -- Admissions within the month
        COUNT(DISTINCT iv.patient_id) FILTER (WHERE DATE_TRUNC('month', iv.date) = DATE_TRUNC('month', rm.last_day)) AS monthly_admissions,
        -- Discharges within the month
        COUNT(DISTINCT dv.patient_id) FILTER (WHERE DATE_TRUNC('month', dv.date) = DATE_TRUNC('month', rm.last_day)) AS monthly_discharges,
        -- Admissions per location within the month
        COUNT(DISTINCT CASE WHEN iv.visit_location = 'Al Barakah' AND DATE_TRUNC('month', iv.date) = DATE_TRUNC('month', rm.last_day) THEN iv.patient_id END) AS monthly_admissions_al_barakah,
        COUNT(DISTINCT CASE WHEN iv.visit_location = 'Tal Al Karamah' AND DATE_TRUNC('month', iv.date) = DATE_TRUNC('month', rm.last_day) THEN iv.patient_id END) AS monthly_admissions_tal_al_karamah,
        COUNT(DISTINCT CASE WHEN (iv.visit_location IS NULL OR iv.visit_location NOT IN ('Al Barakah', 'Tal Al Karamah')) AND DATE_TRUNC('month', iv.date) = DATE_TRUNC('month', rm.last_day) THEN iv.patient_id END) AS monthly_admissions_other,
        -- Discharges per location within the month
        COUNT(DISTINCT CASE WHEN iv.visit_location = 'Al Barakah' AND DATE_TRUNC('month', dv.date) = DATE_TRUNC('month', rm.last_day) THEN dv.patient_id END) AS monthly_discharges_al_barakah,
        COUNT(DISTINCT CASE WHEN iv.visit_location = 'Tal Al Karamah' AND DATE_TRUNC('month', dv.date) = DATE_TRUNC('month', rm.last_day) THEN dv.patient_id END) AS monthly_discharges_tal_al_karamah,
        COUNT(DISTINCT CASE WHEN (iv.visit_location IS NULL OR iv.visit_location NOT IN ('Al Barakah', 'Tal Al Karamah')) AND DATE_TRUNC('month', dv.date) = DATE_TRUNC('month', rm.last_day) THEN dv.patient_id END) AS monthly_discharges_other
    FROM 
        recursive_months rm
    LEFT JOIN 
        public.ncd_initial_visit iv ON DATE_TRUNC('month', iv.date) <= DATE_TRUNC('month', rm.last_day)
    LEFT JOIN 
        public.ncd_discharge_visit dv ON iv.patient_id = dv.patient_id AND DATE_TRUNC('month', dv.date) <= DATE_TRUNC('month', rm.last_day)
    GROUP BY 
        rm.last_day
),
-- Adjust active patients totals based on new admissions and discharges
active_patients AS (
    SELECT 
        last_day,
        monthly_admissions,
        monthly_discharges,
        monthly_admissions AS active_patients_total,
        monthly_admissions_al_barakah AS active_patients_al_barakah,
        monthly_admissions_tal_al_karamah AS active_patients_tal_al_karamah,
        monthly_admissions_other AS active_patients_other
    FROM 
        monthly_data
    WHERE last_day = (SELECT MIN(last_day) FROM monthly_data)
    
    UNION ALL
    
    SELECT 
        md.last_day,
        md.monthly_admissions,
        md.monthly_discharges,
        -- Calculation: Previous month's active patients + this month's new admissions - this month's discharges
        (ap.active_patients_total + md.monthly_admissions - md.monthly_discharges) AS active_patients_total,
        (ap.active_patients_al_barakah + md.monthly_admissions_al_barakah - md.monthly_discharges_al_barakah) AS active_patients_al_barakah,
        (ap.active_patients_tal_al_karamah + md.monthly_admissions_tal_al_karamah - md.monthly_discharges_tal_al_karamah) AS active_patients_tal_al_karamah,
        (ap.active_patients_other + md.monthly_admissions_other - md.monthly_discharges_other) AS active_patients_other
    FROM 
        monthly_data md
    JOIN 
        active_patients ap ON DATE_TRUNC('month', ap.last_day) = DATE_TRUNC('month', md.last_day) - INTERVAL '1 month'
)
SELECT 
    last_day,
    SUM(monthly_admissions) AS total_monthly_admissions,
    SUM(monthly_discharges) AS total_monthly_discharges,
    SUM(active_patients_total) AS total_active_patients,
    SUM(active_patients_al_barakah) AS total_active_patients_al_barakah,
    SUM(active_patients_tal_al_karamah) AS total_active_patients_tal_al_karamah,
    SUM(active_patients_other) AS total_active_patients_other
FROM 
    active_patients
GROUP BY 
    last_day
ORDER BY 
    last_day;
