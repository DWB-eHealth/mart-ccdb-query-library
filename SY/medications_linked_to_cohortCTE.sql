WITH date_ranges AS (
  SELECT
    MIN(DATE_TRUNC('month', start_date)) AS min_month, 
    MAX(DATE_TRUNC('month', COALESCE(date_stopped, CURRENT_DATE))) AS max_month,
    encounter_id
  FROM
    public.medication_data_default
  WHERE
    date_stopped IS NULL -- Only consider ongoing medications
  GROUP BY
    encounter_id -- Added GROUP BY for aggregation
),
months AS (
  SELECT
    generate_series(min_month, max_month, '1 month'::interval) AS month, encounter_id
  FROM
    date_ranges
),
filtered_medication AS (
  SELECT
    encounter_id,
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
      WHEN LOWER(frequency) = 'immediately' THEN 1
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
      ELSE 1
    END AS frequency
  FROM
    public.medication_data_default
  WHERE
    date_stopped IS NULL OR date_stopped > start_date -- Consider ongoing or just-stopped medications
),
medication_days AS (
  SELECT
    m.month, m.encounter_id,
    fm.coded_drug_name,
    fm.dose,
    fm.dose_units,
    fm.frequency,
    -- Calculate the number of active days within the month
    EXTRACT(DAY FROM LEAST(
      LEAST(fm.effective_end_date, m.month + INTERVAL '1 month - 1 day'),
      m.month + INTERVAL '1 month - 1 day'
    ) - GREATEST(fm.start_date, m.month) + INTERVAL '1 day') AS active_days_in_month
  FROM
    months m
  JOIN
    filtered_medication fm
  ON
    fm.encounter_id = m.encounter_id AND
    fm.start_date <= m.month + INTERVAL '1 month - 1 day'
    AND fm.effective_end_date >= m.month
),
monthly_quantity AS (
  SELECT
    medication_days.coded_drug_name, 
    medication_days.encounter_id,
    medication_days.dose_units,
    medication_days.month,
    -- Calculation per month
    SUM(medication_days.active_days_in_month * medication_days.dose * medication_days.frequency) AS monthly_quantity 
  FROM
    medication_days
  GROUP BY
    medication_days.coded_drug_name, 
    medication_days.encounter_id,
    medication_days.dose_units, 
    medication_days.month
),
initial AS (
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
        d.patient_outcome AS patient_outcome
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
)
SELECT
  monthly_quantity.coded_drug_name,
  monthly_quantity.dose_units,
  monthly_quantity.month,
  monthly_quantity.monthly_quantity
FROM cohort
LEFT OUTER JOIN monthly_quantity 
    ON monthly_quantity.encounter_id = cohort.initial_encounter_id
ORDER BY
  monthly_quantity.coded_drug_name, 
  monthly_quantity.month;
