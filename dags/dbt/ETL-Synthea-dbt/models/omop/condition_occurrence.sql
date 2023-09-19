 {{ config(
    tags = 'STEM_tbl',
) }} 

SELECT
    row_number()OVER(ORDER BY c.person_id) AS condition_occurrence_id,
    c.person_id AS person_id,
    srctostdvm.target_concept_id AS condition_concept_id,
    c.start AS condition_start_date,
    c.start AS condition_start_datetime,
    c.stop AS condition_end_date,
    c.stop AS condition_end_datetime,
    38000175 AS condition_type_concept_id,
    cast(NULL AS varchar) AS stop_reason,
    cast(NULL AS integer) AS provider_id,
    c.visit_occurrence_id_new AS visit_occurrence_id,
    c.visit_occurrence_id_new + 1000000 AS visit_detail_id,
    c.code AS condition_source_value,
    cast(srctosrcvm.source_concept_id as integer) AS condition_source_concept_id,
    cast(NULL as integer) AS condition_status_source_value,
    0 AS condition_status_concept_id
FROM {{ ref('stg_condition_occurrence') }} AS c
{{ map_src_to_std_vocab(alias="srctostdvm", from="c", target_domain_id="Condition", target_vocabulary_id="SNOMED", source_vocabulary_id="SNOMED") }}
{{ map_src_to_src_vocab(alias="srctosrcvm", from="c", source_vocabulary_id="SNOMED", source_domain_id="Condition") }}