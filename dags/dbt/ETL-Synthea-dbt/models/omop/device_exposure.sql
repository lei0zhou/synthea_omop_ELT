 {{ config(
    tags = 'STEM_tbl',
) }} 

SELECT
    row_number()OVER(ORDER BY person_id) AS device_exposure_id,
    p.person_id AS person_id,
    srctostdvm.target_concept_id AS device_concept_id,
    d.start AS device_exposure_start_date,
    d.start AS device_exposure_start_datetime,
    d.stop AS device_exposure_end_date,
    d.stop AS device_exposure_end_datetime,
    38000267 AS device_type_concept_id,
    d.udi AS unique_device_id,
    cast(NULL AS int) AS quantity,
    pr.provider_id AS provider_id,
    fv.visit_occurrence_id_new AS visit_occurrence_id,
    fv.visit_occurrence_id_new + 1000000 AS visit_detail_id,
    d.code AS device_source_value,
    srctosrcvm.source_concept_id AS device_source_concept_id,
    cast(NULL AS int) AS unit_concept_id,
    NULL AS unit_source_value,
    cast(NULL AS int) AS unit_source_concept_id

FROM {{ source('synthea', 'devices') }} AS d
INNER JOIN {{ ref('source_to_standard_vocab_map') }} AS srctostdvm
    ON srctostdvm.source_code = d.code
        AND srctostdvm.target_domain_id = 'Device'
        AND srctostdvm.target_vocabulary_id = 'SNOMED'
        AND srctostdvm.source_vocabulary_id = 'SNOMED'
        AND srctostdvm.target_standard_concept = 'S'
        AND srctostdvm.target_invalid_reason IS NULL
INNER JOIN {{ ref('source_to_source_vocab_map') }} AS srctosrcvm
    ON srctosrcvm.source_code = d.code
        AND srctosrcvm.source_vocabulary_id = 'SNOMED'
LEFT JOIN {{ ref('final_visit_ids') }} AS fv
    ON fv.encounter_id = d.encounter
LEFT JOIN {{ source('synthea', 'encounters') }} AS e
    ON d.encounter = e.id
        AND d.patient = e.patient
LEFT JOIN {{ ref('provider') }} AS pr 
    ON e.provider = pr.provider_source_value
INNER JOIN {{ ref('person') }} AS p
    ON p.person_source_value = d.patient
