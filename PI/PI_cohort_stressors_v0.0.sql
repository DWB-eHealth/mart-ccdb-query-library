-- Returns one row per unique stressor recorded for each intake encounter.
SELECT DISTINCT
  mhi.patient_id,
  mhi.encounter_id AS intake_encounter_id,
  mhi."date"::date AS intake_date,
  NULLIF(BTRIM(stressor.value), '') AS stressor
FROM mental_health_intake mhi
CROSS JOIN LATERAL (
  VALUES
    (mhi.stressor_1),
    (mhi.stressor_2),
    (mhi.stressor_3)
) AS stressor(value)
WHERE NULLIF(BTRIM(stressor.value), '') IS NOT NULL
ORDER BY
  mhi.patient_id,
  intake_date,
  intake_encounter_id,
  stressor;
