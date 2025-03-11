WITH date_ranges AS (
  SELECT
    MIN(DATE_TRUNC('month', start_date)) AS min_month,
    MAX(DATE_TRUNC('month', COALESCE(date_stopped, CURRENT_DATE))) AS max_month
  FROM
    public.medication_data_default
  WHERE
    date_stopped IS NULL -- Only consider ongoing medications
),
months AS (
  SELECT
    generate_series(min_month, max_month, '1 month'::interval) AS month
  FROM
    date_ranges
),
filtered_medication AS (
  SELECT
    coded_drug_name,
    dose,
    dose_units,
    start_date,
    COALESCE(date_stopped, CURRENT_DATE) AS effective_end_date,
    -- To calculate the daily amount based on the frequency
    CASE
      WHEN LOWER(frequency) = 'once a day' THEN 1
      WHEN LOWER(frequency) = 'twice a day' THEN 2
      WHEN LOWER(frequency) = 'thrice a day' THEN 3
      WHEN LOWER(frequency) = 'four times a day' THEN 4
      WHEN LOWER(frequency) = 'five times a day' THEN 5
      WHEN LOWER(frequency) = 'immediately' AND "public"."medication_data_default"."coded_drug_name" LIKE '%ISOSORBIDE DINITRATE%' THEN 0.55
      WHEN LOWER(frequency) = 'immediately' AND "public"."medication_data_default"."coded_drug_name" LIKE '%SALBUTAMOL sulfate%' THEN 9
      WHEN LOWER(frequency) = 'on alternate days' THEN 0.5
      WHEN LOWER(frequency) = 'once a week' THEN 0.14
      WHEN LOWER(frequency) = 'four days a week' THEN 0.57
      WHEN LOWER(frequency) = 'five days a week' THEN 0.71
      WHEN LOWER(frequency) = 'six days a week' THEN 0.86
      WHEN LOWER(frequency) = 'twice a week' THEN 0.285
      WHEN LOWER(frequency) = 'every hour' THEN 24
      WHEN LOWER(frequency) = 'every 2 hours' THEN 12
      WHEN LOWER(frequency) = 'every 3 hours' THEN 8
      WHEN LOWER(frequency) = 'every 4 hours' THEN 6
      WHEN LOWER(frequency) = 'every 6 hours' THEN 4
      WHEN LOWER(frequency) = 'every 8 hours' THEN 3
      WHEN LOWER(frequency) = 'every 12 hours' THEN 2
      WHEN LOWER(frequency) = 'every 2 weeks' THEN 0.07
      WHEN LOWER(frequency) = 'every 3 weeks' THEN 0.047
      WHEN LOWER(frequency) = 'once a month' THEN 0.033
      ELSE 0
    END AS frequency
  FROM
    public.medication_data_default
  WHERE
    date_stopped IS NULL OR date_stopped > start_date -- Consider ongoing or just-stopped medications
),
medication_days AS (
  SELECT
    m.month,
    fm.coded_drug_name,
    fm.dose,
    fm.dose_units,
    fm.frequency,
    -- Calculate the number of active days within the month, ensuring the date_stopped is considered if available
    EXTRACT(DAY FROM LEAST(
      LEAST(fm.effective_end_date, m.month + INTERVAL '1 month - 1 day'),
      m.month + INTERVAL '1 month - 1 day'
    ) - GREATEST(fm.start_date, m.month) + INTERVAL '1 day') AS active_days_in_month
  FROM
    months m
  JOIN
    filtered_medication fm
  ON
    fm.start_date <= m.month + INTERVAL '1 month - 1 day'
    AND fm.effective_end_date >= m.month
),
monthly_quantity AS (
  SELECT
    coded_drug_name,
    dose_units,
    month,
    -- Calculation per month
    SUM(active_days_in_month * dose * frequency) AS monthly_quantity 
  FROM
    medication_days
  GROUP BY
    coded_drug_name, dose_units, month
)
SELECT
  coded_drug_name,
  dose_units,
  month,
  monthly_quantity
FROM
  monthly_quantity
ORDER BY
  coded_drug_name, month;
