 {{ config(
    tags = 'STEM_tbl',
) }} 

SELECT
    row_number()OVER(ORDER BY p.person_id) AS procedure_occurrence_id,
    p.person_id AS person_id,
    srctostdvm.target_concept_id AS procedure_concept_id,
    pr.date AS procedure_date,
    pr.date AS procedure_datetime,
    38000267 AS procedure_type_concept_id,
    0 AS modifier_concept_id,
    cast(NULL AS integer) AS quantity,
    prv.provider_id AS provider_id,
    fv.visit_occurrence_id_new AS visit_occurrence_id,
    fv.visit_occurrence_id_new + 1000000 AS visit_detail_id,
    pr.code AS procedure_source_value,
    srctosrcvm.source_concept_id AS procedure_source_concept_id,
    NULL AS modifier_source_value
FROM {{ source('synthea', 'procedures') }} AS pr
INNER JOIN {{ ref('source_to_standard_vocab_map') }} AS srctostdvm
    ON srctostdvm.source_code = pr.code
       AND srctostdvm.target_domain_id = 'Procedure'
       AND srctostdvm.target_vocabulary_id = 'SNOMED'
       AND srctostdvm.source_vocabulary_id = 'SNOMED'
       AND srctostdvm.target_standard_concept = 'S'
       AND srctostdvm.target_invalid_reason IS NULL
INNER JOIN {{ ref('source_to_source_vocab_map') }} AS srctosrcvm
    ON srctosrcvm.source_code = pr.code
       AND srctosrcvm.source_vocabulary_id = 'SNOMED'
LEFT JOIN {{ ref('final_visit_ids') }} AS fv
    ON fv.encounter_id = pr.encounter
LEFT JOIN {{ source('synthea', 'encounters') }} AS e
    ON pr.encounter = e.id
       AND pr.patient = e.patient
LEFT JOIN {{ ref('provider') }} AS prv 
    ON e.provider = prv.provider_source_value
INNER JOIN {{ ref('person') }} AS p
    ON p.person_source_value = pr.patient
