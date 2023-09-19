 {{ config(
    tags = 'STEM_tbl',
) }} 

SELECT 
    row_number()OVER(ORDER BY person_id) AS measurement_id,
    person_id,
    measurement_concept_id,
    measurement_date,
    measurement_datetime,
    measurement_time,
    measurement_type_concept_id,
    operator_concept_id,
    value_as_number,
    value_as_concept_id,
    unit_concept_id,
    range_low,
    range_high,
    provider_id,
    visit_occurrence_id,
    visit_detail_id,
    measurement_source_value,
    measurement_source_concept_id,
    unit_source_value,
    value_source_value
FROM (
    SELECT
        p.person_id AS person_id,
        srctostdvm.target_concept_id AS measurement_concept_id,
        pr.date AS measurement_date,
        pr.date AS measurement_datetime,
        pr.date AS measurement_time,
        38000267 AS measurement_type_concept_id,
        0 AS operator_concept_id,
        cast(NULL AS float) AS value_as_number,
        0 AS value_as_concept_id,
        0 AS unit_concept_id,
        cast(NULL AS float) AS range_low,
        cast(NULL AS float) AS range_high,
        prv.provider_id AS provider_id,
        fv.visit_occurrence_id_new AS visit_occurrence_id,
        fv.visit_occurrence_id_new + 1000000 AS visit_detail_id,
        pr.code AS measurement_source_value,
        srctosrcvm.source_concept_id AS measurement_source_concept_id,
        cast(NULL AS varchar) AS unit_source_value,
        cast(NULL AS varchar) AS value_source_value
    FROM {{ source('synthea', 'procedures') }} AS pr
    INNER JOIN {{ ref('source_to_standard_vocab_map') }} AS srctostdvm
        ON srctostdvm.source_code = pr.code
            AND srctostdvm.target_domain_id = 'Measurement'
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
  
    UNION ALL

    SELECT
        p.person_id AS person_id,
        srctostdvm.target_concept_id AS measurement_concept_id,
        o.date AS measurement_date,
        o.date AS measurement_datetime,
        o.date AS measurement_time,
        38000267 AS measurement_type_concept_id,
        0 AS operator_concept_id,
        CASE 
            WHEN o.value ~ '^([0-9]+[.]?[0-9]*|[.][0-9]+)$' = '1'
                THEN cast(o.value AS float) 
            ELSE cast(NULL AS float) 
        END AS value_as_number,
        coalesce(srcmap2.target_concept_id, 0) AS value_as_concept_id,
        coalesce(srcmap1.target_concept_id, 0) AS unit_concept_id,
        cast(NULL AS float) AS range_low,
        cast(NULL AS float) AS range_high,
        pr.provider_id AS provider_id,
        fv.visit_occurrence_id_new AS visit_occurrence_id,
        fv.visit_occurrence_id_new + 1000000 AS visit_detail_id,
        o.code AS measurement_source_value,
        coalesce(srctosrcvm.source_concept_id, 0) AS measurement_source_concept_id,
        o.units AS unit_source_value,
        o.value AS value_source_value
    FROM {{ source('synthea', 'observations') }} AS o
    INNER JOIN {{ ref('source_to_standard_vocab_map') }} AS srctostdvm
        ON srctostdvm.source_code = o.code
            AND srctostdvm.target_domain_id = 'Measurement'
            AND srctostdvm.source_vocabulary_id = 'LOINC'
            AND srctostdvm.target_standard_concept = 'S'
            AND srctostdvm.target_invalid_reason IS NULL
    LEFT JOIN {{ ref('source_to_standard_vocab_map') }} AS srcmap1
        ON srcmap1.source_code = o.units
            AND srcmap1.target_vocabulary_id = 'UCUM'
            AND srcmap1.source_vocabulary_id = 'UCUM'
            AND srcmap1.target_standard_concept = 'S'
            AND srcmap1.target_invalid_reason IS NULL 
    LEFT JOIN {{ ref('source_to_standard_vocab_map') }} AS srcmap2
        ON srcmap2.source_code = o.value
            AND srcmap2.target_domain_id = 'Meas value'
            AND srcmap2.target_standard_concept = 'S'
            AND srcmap2.target_invalid_reason IS NULL 
    LEFT JOIN {{ ref('source_to_source_vocab_map') }} AS srctosrcvm
        ON srctosrcvm.source_code = o.code
            AND srctosrcvm.source_vocabulary_id = 'LOINC'
    LEFT JOIN {{ ref('final_visit_ids') }} AS fv
        ON fv.encounter_id = o.encounter
    LEFT JOIN {{ source('synthea', 'encounters') }} AS e
        ON o.encounter = e.id
            AND o.patient = e.patient
    LEFT JOIN {{ ref('provider') }} AS pr 
        ON e.provider = pr.provider_source_value
    INNER JOIN {{ ref('person') }} AS p
        ON p.person_source_value = o.patient
    ) AS tmp
    