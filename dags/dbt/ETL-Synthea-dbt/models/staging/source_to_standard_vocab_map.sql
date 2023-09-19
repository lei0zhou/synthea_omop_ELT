 {{ config(
    materialized = 'table',
    tags = 'mapping',
    indexes=[
      {'columns': ['source_code']},
      {'columns': ['target_domain_id', 'target_vocabulary_id', 'target_standard_concept', 'source_vocabulary_id', 'source_domain_id'], 'type': 'btree'}
    ]
) }} 


-- Create mapping table as per logic in 3.1.2 Source to Standard Terminology
-- found in Truven_CCAE_and_MDCR_ETL_CDM_V5.2.0.doc


WITH CTE_VOCAB_MAP AS (
    SELECT
        c.concept_code AS source_code,
        c.concept_id AS source_concept_id,
        c.concept_name AS source_code_description,
        c.vocabulary_id AS source_vocabulary_id,
        c.domain_id AS source_domain_id,
        c.concept_class_id AS source_concept_class_id,
        c.valid_start_date AS source_valid_start_date,
        c.valid_end_date AS source_valid_end_date,
        c.invalid_reason AS source_invalid_reason,
        c1.concept_id AS target_concept_id,
        c1.concept_name AS target_concept_name,
        c1.vocabulary_id AS target_vocabulary_id,
        c1.domain_id AS target_domain_id,
        c1.concept_class_id AS target_concept_class_id,
        c1.invalid_reason AS target_invalid_reason,
        c1.standard_concept AS target_standard_concept
    FROM {{ source('vocab', 'concept') }} AS c
    INNER JOIN {{ source('vocab', 'concept_relationship') }} AS cr
        ON c.concept_id = cr.concept_id_1
            AND cr.invalid_reason IS NULL
            AND lower(cr.relationship_id) = 'maps to'
    INNER JOIN {{ source('vocab', 'concept') }} AS c1
        ON cr.concept_id_2 = c1.concept_id
            AND c1.invalid_reason IS NULL
    UNION
    SELECT
        source_code,
        source_concept_id,
        source_code_description,
        source_vocabulary_id,
        c1.domain_id AS source_domain_id,
        c2.concept_class_id AS source_concept_class_id,
        c1.valid_start_date AS source_valid_start_date,
        c1.valid_end_date AS source_valid_end_date,
        stcm.invalid_reason AS source_invalid_reason,
        target_concept_id,
        c2.concept_name AS target_concept_name,
        target_vocabulary_id,
        c2.domain_id AS target_domain_id,
        c2.concept_class_id AS target_concept_class_id,
        c2.invalid_reason AS target_invalid_reason,
        c2.standard_concept AS target_standard_concept
    FROM {{ source('vocab', 'source_to_concept_map') }} AS stcm
    LEFT OUTER JOIN {{ source('vocab', 'concept') }} AS c1
        ON c1.concept_id = stcm.source_concept_id
    LEFT OUTER JOIN {{ source('vocab', 'concept') }} AS c2
        ON c2.concept_id = stcm.target_concept_id
    WHERE stcm.invalid_reason IS NULL
)

SELECT * FROM CTE_VOCAB_MAP
