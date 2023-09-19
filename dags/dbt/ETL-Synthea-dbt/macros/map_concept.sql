{%- macro map_concept(cdm_table="", vocabulary_id="", concept_code_field="") -%}

LEFT JOIN {{ source('vocab', 'concept') }} AS {{vocabulary_id}}_concept
ON 
    (
        {{cdm_table}}.{{concept_code_field}} = {{vocabulary_id}}_concept.concept_code
        AND {{vocabulary_id}}_concept.vocabulary_id = '{{vocabulary_id}}'
    )
    OR
	(
		{{cdm_table}}.{{concept_code_field}} = {{vocabulary_id}}_concept.concept_code
		AND {{vocabulary_id}}_concept.concept_code = 'No matching concept'
	)

{%- endmacro -%}

{%- macro map_src_to_std_vocab(alias="", from="", target_domain_id="", target_vocabulary_id="", source_vocabulary_id="") -%}

INNER JOIN {{ ref('source_to_standard_vocab_map') }} AS {{alias}}
    ON {{alias}}.source_code = {{from}}.code
        AND {{alias}}.target_domain_id = '{{target_domain_id}}'
        AND {{alias}}.target_vocabulary_id = '{{target_vocabulary_id}}'
        AND {{alias}}.source_vocabulary_id = '{{source_vocabulary_id}}'
        AND {{alias}}.target_standard_concept = 'S'
        AND {{alias}}.target_invalid_reason IS NULL

{%- endmacro -%}

{%- macro map_src_to_src_vocab(alias="", from="", source_vocabulary_id="", source_domain_id="") -%}
INNER JOIN {{ ref('source_to_source_vocab_map') }} AS {{alias}}
    ON {{alias}}.source_code = {{from}}.code
        AND {{alias}}.source_vocabulary_id = '{{source_vocabulary_id}}'
        AND {{alias}}.source_domain_id = '{{source_domain_id}}'
{%- endmacro -%}