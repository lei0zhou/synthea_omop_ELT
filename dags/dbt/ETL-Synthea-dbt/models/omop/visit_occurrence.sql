WITH cte_concept_code AS(
    SELECT
        *, 

        CASE av.encounterclass
            WHEN 'ambulatory' THEN 'OP'
            WHEN 'emergency' THEN 'ER'
            WHEN 'inpatient' THEN 'IP'
            WHEN 'wellness' THEN 'OP'
            WHEN 'urgentcare' THEN 'ER' 
            WHEN 'outpatient' THEN 'OP'
            ELSE 'No matching concept'
        END AS visit_concept_code
    FROM {{ ref('all_visits') }} AS av
)
SELECT
    av.visit_occurrence_id AS visit_occurrence_id,
    p.person_id AS person_id, 

    visit_concept.concept_id AS visit_concept_id,
    av.visit_start_date AS visit_start_date,
    av.visit_start_date AS visit_start_datetime,
    av.visit_end_date AS visit_end_date,
    av.visit_end_date AS visit_end_datetime,
    44818517 AS visit_type_concept_id, 
    pr.provider_id AS provider_id, 
    NULL AS care_site_id,
    av.encounter_id AS visit_source_value,
    0 AS visit_source_concept_id, 
    0 AS admitting_source_concept_id,
    NULL AS admitting_source_value,
    0 AS discharge_to_concept_id, 
    NULL AS discharge_to_source_value, 
    lag(av.visit_occurrence_id) 
    OVER(PARTITION BY p.person_id
                      ORDER BY av.visit_start_date) AS preceding_visit_occurrence_id
FROM cte_concept_code AS av
{{ map_concept(cdm_table='av', vocabulary_id='Visit', concept_code_field='visit_concept_code') }}
INNER JOIN {{ ref('person') }} AS p
    ON av.patient = p.person_source_value
INNER JOIN {{ source('synthea', 'encounters') }} AS e
    ON av.encounter_id = e.id
        AND av.patient = e.patient
INNER JOIN {{ ref('provider') }} AS pr 
    ON e.provider = pr.provider_source_value
WHERE av.visit_occurrence_id IN (
    SELECT DISTINCT visit_occurrence_id_new
    FROM {{ ref('final_visit_ids') }} )