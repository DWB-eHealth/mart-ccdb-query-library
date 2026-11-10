SELECT
  "source"."id_resultat" AS "id_resultat",
  "source"."patient_id" AS "patient_id",
  "source"."encounter_id" AS "encounter_id",
  "source"."occurrence" AS "occurrence",
  "source"."date_prelevement" AS "date_prelevement",
  "source"."date_reception" AS "date_reception",
  "source"."resultat" AS "resultat",
  "source"."date_created" AS "date_created",
  "source"."version_formulaire" AS "version_formulaire"
FROM
  (
    SELECT
      source.id_resultat AS id_resultat,
      source.patient_id AS patient_id,
      source.encounter_id AS encounter_id,
      source.occurrence AS occurrence,
      source.date_prelevement AS date_prelevement,
      source.date_reception AS date_reception,
      source.resultat AS resultat,
      source.date_created AS date_created,
      source.version_formulaire AS version_formulaire
    FROM
      (
        /* ==========================================
               PLIPS v1 - CD4
               ========================================== */
        SELECT
          encounter_id :: text || '-v1-' || substring(
            form_field_path
            from
              '-(\d+)$'
          ) AS id_resultat,
          patient_id,
          encounter_id,
          substring(
            form_field_path
            from
              '-(\d+)$'
          ) :: int AS occurrence,
          MAX(date_de_prélèvement_cd4) AS date_prelevement,
          MAX(date_de_récéption_des_résultats_cd4) AS date_reception,
          MAX(résultat_brut_cd4) AS resultat,
          MIN(date_created) AS date_created,
          'plips.1' AS version_formulaire
        FROM
          public.signes_vitaux_et_laboratoire_cd4
        WHERE
          form_field_path ~ '^plips\.1/(58|59|60)-\d+$'
        GROUP BY
          patient_id,
          encounter_id,
          substring(
            form_field_path
            from
              '-(\d+)$'
          )
        UNION ALL
        /* ==========================================
               PLIPS v2 - CD4
               ========================================== */
        SELECT
          encounter_id :: text || '-v2-' || substring(
            form_field_path
            from
              '-(\d+)$'
          ) AS id_resultat,
          patient_id,
          encounter_id,
          substring(
            form_field_path
            from
              '-(\d+)$'
          ) :: int AS occurrence,
          date_de_prélèvement_cd4 AS date_prelevement,
          date_de_récéption_des_résultats_cd4 AS date_reception,
          résultat_brut_cd4 AS resultat,
          date_created,
          'plips.2' AS version_formulaire
        FROM
          public.signes_vitaux_et_laboratoire_cd4
        WHERE
          form_field_path ~ '^plips\.2/57-\d+$'
      ) AS source
    ORDER BY
      patient_id,
      encounter_id,
      occurrence
  ) AS "source"

ORDER BY
    patient_id,
    encounter_id,
    occurrence;