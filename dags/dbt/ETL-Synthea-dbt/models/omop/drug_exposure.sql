 {{ config(
    tags = 'STEM_tbl',
) }} 

SELECT
    row_number()OVER(ORDER BY person_id) AS drug_exposure_id,
    person_id,
    drug_concept_id,
    drug_exposure_start_date,
    drug_exposure_start_datetime,
    drug_exposure_end_date,
    drug_exposure_end_datetime,
    verbatim_end_date,
    drug_type_concept_id,
    stop_reason,
    refills,
    quantity,
    days_supply,
    sig,
    route_concept_id,
    lot_number,
    provider_id,
    visit_occurrence_id,
    visit_detail_id,
    drug_source_value,
    drug_source_concept_id,
    route_source_value,
    dose_unit_source_value
FROM (


    SELECT
        p.person_id AS person_id,
        srctostdvm.target_concept_id AS drug_concept_id,
        m.start AS drug_exposure_start_date,
        m.start AS drug_exposure_start_datetime,
        coalesce(m.stop, m.start) AS drug_exposure_end_date,
        coalesce(m.stop, m.start) AS drug_exposure_end_datetime,
        m.stop AS verbatim_end_date,
        32869 AS drug_type_concept_id,
        cast(NULL AS varchar) AS stop_reason,
        0 AS refills,
        0 AS quantity,
        coalesce(m.stop::date - m.start::date, 0) AS days_supply,
        cast(NULL AS varchar) AS sig,
        0 AS route_concept_id,
        0 AS lot_number,
        pr.provider_id AS provider_id,
        fv.visit_occurrence_id_new AS visit_occurrence_id,
        fv.visit_occurrence_id_new + 1000000 AS visit_detail_id,
        m.code AS drug_source_value,
        srctosrcvm.source_concept_id AS drug_source_concept_id,
        cast(NULL AS varchar) AS route_source_value,
        cast(NULL AS varchar) AS dose_unit_source_value
    FROM {{ source('synthea', 'medications') }} AS m
    INNER JOIN {{ ref('source_to_standard_vocab_map') }} AS srctostdvm
        ON srctostdvm.source_code = m.code
            AND srctostdvm.target_domain_id = 'Drug'
            AND srctostdvm.target_vocabulary_id = 'RxNorm'
            AND srctostdvm.target_standard_concept = 'S'
            AND srctostdvm.target_invalid_reason IS NULL
    INNER JOIN {{ ref('source_to_source_vocab_map') }} AS srctosrcvm
        ON srctosrcvm.source_code = m.code
            AND srctosrcvm.source_vocabulary_id = 'RxNorm'
    LEFT JOIN {{ ref('final_visit_ids') }} AS fv
        ON fv.encounter_id = m.encounter
    LEFT JOIN {{ source('synthea', 'encounters') }} AS e
        ON m.encounter = e.id
            AND m.patient = e.patient
    LEFT JOIN {{ ref('provider') }} AS pr 
        ON e.provider = pr.provider_source_value
    INNER JOIN {{ ref('person') }} AS p
        ON p.person_source_value = m.patient

    UNION ALL

    SELECT
        p.person_id AS person_id,
        srctostdvm.target_concept_id AS drug_concept_id,
        i.date AS drug_exposure_start_date,
        i.date AS drug_exposure_start_datetime,
        i.date AS drug_exposure_end_date,
        i.date AS drug_exposure_end_datetime,
        i.date AS verbatim_end_date,
        32869 AS drug_type_concept_id,
        cast(NULL AS varchar) AS stop_reason,
        0 AS refills,
        0 AS quantity,
        0 AS days_supply,
        cast(NULL AS varchar) AS sig,
        0 AS route_concept_id,
        0 AS lot_number, 
        pr.provider_id AS provider_id,
        fv.visit_occurrence_id_new AS visit_occurrence_id,
        fv.visit_occurrence_id_new + 1000000 AS visit_detail_id,
        i.code AS drug_source_value,
        srctosrcvm.source_concept_id AS drug_source_concept_id, 
        cast(NULL AS varchar) AS route_source_value,
        cast(NULL AS varchar) AS dose_unit_source_value
    FROM {{ source('synthea', 'immunizations') }} AS i
    INNER JOIN {{ ref('source_to_standard_vocab_map') }} AS srctostdvm
        ON srctostdvm.source_code = i.code
            AND srctostdvm.target_domain_id = 'Drug'
            AND srctostdvm.target_vocabulary_id = 'CVX'
            AND srctostdvm.target_standard_concept = 'S'
            AND srctostdvm.target_invalid_reason IS NULL
    INNER JOIN {{ ref('source_to_source_vocab_map') }} AS srctosrcvm
        ON srctosrcvm.source_code = i.code
            AND srctosrcvm.source_vocabulary_id = 'CVX'
    LEFT JOIN {{ ref('final_visit_ids') }} AS fv
        ON fv.encounter_id = i.encounter
    LEFT JOIN {{ source('synthea', 'encounters') }} AS e
        ON i.encounter = e.id
            AND i.patient = e.patient
    LEFT JOIN {{ ref('provider') }} AS pr 
        ON e.provider = pr.provider_source_value
    INNER JOIN {{ ref('person') }} AS p
        ON p.person_source_value = i.patient
    ) AS tmp
