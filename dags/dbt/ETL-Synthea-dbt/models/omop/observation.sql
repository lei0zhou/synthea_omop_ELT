 {{ config(
    tags = 'STEM_tbl',
) }} 

SELECT
    row_number()OVER(ORDER BY person_id) AS observation_id,
    person_id,
    observation_concept_id,
    observation_date,
    observation_datetime,
    observation_type_concept_id,
    value_as_number,
    value_as_string,
    value_as_concept_id,
    qualifier_concept_id,
    unit_concept_id,
    provider_id,
    visit_occurrence_id,
    visit_detail_id,
    observation_source_value,
    observation_source_concept_id,
    unit_source_value,
    qualifier_source_value,
    NULL AS value_source_value,
    cast(NULL AS int) AS observation_event_id,
    cast(NULL AS int) AS obs_event_field_concept_id

FROM (
    SELECT
        p.person_id AS person_id,
        srctostdvm.target_concept_id AS observation_concept_id,
        a.start AS observation_date,
        a.start AS observation_datetime,
        38000280 AS observation_type_concept_id,
        cast(NULL AS float) AS value_as_number,
        cast(NULL AS varchar) AS value_as_string,
        0 AS value_as_concept_id,
        0 AS qualifier_concept_id,
        0 AS unit_concept_id,
        pr.provider_id AS provider_id,
        fv.visit_occurrence_id_new AS visit_occurrence_id,
        fv.visit_occurrence_id_new + 1000000 AS visit_detail_id,
        a.code AS observation_source_value,
        srctosrcvm.source_concept_id AS observation_source_concept_id,
        cast(NULL AS varchar) AS unit_source_value,
        cast(NULL AS varchar) AS qualifier_source_value
    FROM {{ source('synthea', 'allergies') }} AS a
    INNER JOIN {{ ref('source_to_standard_vocab_map') }} AS srctostdvm
        ON srctostdvm.source_code = a.code
            AND srctostdvm.target_domain_id = 'Observation'
            AND srctostdvm.target_vocabulary_id = 'SNOMED'
            AND srctostdvm.target_standard_concept = 'S'
            AND srctostdvm.target_invalid_reason IS NULL
    INNER JOIN {{ ref('source_to_source_vocab_map') }} AS srctosrcvm
        ON srctosrcvm.source_code = a.code
            AND srctosrcvm.source_vocabulary_id = 'SNOMED'
            AND srctosrcvm.source_domain_id = 'Observation'
    LEFT JOIN {{ ref('final_visit_ids') }} AS fv
        ON fv.encounter_id = a.encounter
    LEFT JOIN {{ source('synthea', 'encounters') }} AS e
        ON a.encounter = e.id
            AND a.patient = e.patient
    LEFT JOIN {{ ref('provider') }} AS pr 
        ON e.provider = pr.provider_source_value
    INNER JOIN {{ ref('person') }} AS p
        ON p.person_source_value = a.patient

    UNION ALL

    SELECT
        p.person_id AS person_id,
        srctostdvm.target_concept_id AS observation_concept_id,
        c.start AS observation_date,
        c.start AS observation_datetime,
        38000280 AS observation_type_concept_id,
        cast(NULL AS float) AS value_as_number,
        cast(NULL AS varchar) AS value_as_string,
        0 AS value_as_concept_id,
        0 AS qualifier_concept_id,
        0 AS unit_concept_id,
        pr.provider_id AS provider_id,
        fv.visit_occurrence_id_new AS visit_occurrence_id,
        fv.visit_occurrence_id_new + 1000000 AS visit_detail_id,
        c.code AS observation_source_value,
        srctosrcvm.source_concept_id AS observation_source_concept_id,
        cast(NULL AS varchar) AS unit_source_value,
        cast(NULL AS varchar) AS qualifier_source_value
    FROM {{ source('synthea', 'conditions') }} AS c
    INNER JOIN {{ ref('source_to_standard_vocab_map') }} AS srctostdvm
        ON srctostdvm.source_code = c.code
            AND srctostdvm.target_domain_id = 'Observation'
            AND srctostdvm.target_vocabulary_id = 'SNOMED'
            AND srctostdvm.target_standard_concept = 'S'
            AND srctostdvm.target_invalid_reason IS NULL
    INNER JOIN {{ ref('source_to_source_vocab_map') }} AS srctosrcvm
        ON srctosrcvm.source_code = c.code
            AND srctosrcvm.source_vocabulary_id = 'SNOMED'
            AND srctosrcvm.source_domain_id = 'Observation'
    LEFT JOIN {{ ref('final_visit_ids') }} AS fv
        ON fv.encounter_id = c.encounter
    LEFT JOIN {{ source('synthea', 'encounters') }} AS e
        ON c.encounter = e.id
            AND c.patient = e.patient
    LEFT JOIN {{ ref('provider') }} AS pr 
        ON e.provider = pr.provider_source_value
    INNER JOIN {{ ref('person') }} AS p
        ON p.person_source_value = c.patient
  
    UNION ALL

    SELECT
        p.person_id AS person_id,
        srctostdvm.target_concept_id AS observation_concept_id,
        o.date AS observation_date,
        o.date AS observation_datetime,
        38000280 AS observation_type_concept_id,
        cast(NULL AS float) AS value_as_number,
        cast(NULL AS varchar) AS value_as_string,
        0 AS value_as_concept_id,
        0 AS qualifier_concept_id,
        0 AS unit_concept_id,
        pr.provider_id AS provider_id,
        fv.visit_occurrence_id_new AS visit_occurrence_id,
        fv.visit_occurrence_id_new + 1000000 AS visit_detail_id,
        o.code AS observation_source_value,
        srctosrcvm.source_concept_id AS observation_source_concept_id,
        cast(NULL AS varchar) AS unit_source_value,
        cast(NULL AS varchar) AS qualifier_source_value

    FROM {{ source('synthea', 'observations') }} AS o
    INNER JOIN {{ ref('source_to_standard_vocab_map') }} AS srctostdvm
        ON srctostdvm.source_code = o.code
            AND srctostdvm.target_domain_id = 'Observation'
            AND srctostdvm.target_vocabulary_id = 'LOINC'
            AND srctostdvm.target_standard_concept = 'S'
            AND srctostdvm.target_invalid_reason IS NULL
    INNER JOIN {{ ref('source_to_source_vocab_map') }} AS srctosrcvm
        ON srctosrcvm.source_code = o.code
            AND srctosrcvm.source_vocabulary_id = 'LOINC'
            AND srctosrcvm.source_domain_id = 'Observation'
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
