-- For testing purposes, create populate VISIT_DETAIL
-- such that it's basically a copy of VISIT_OCCURRENCE


SELECT
    av.visit_occurrence_id + 1000000 AS visit_detail_id,
    p.person_id AS person_id, 

    CASE lower(av.encounterclass) 
        WHEN 'ambulatory' THEN 9202
        WHEN 'emergency' THEN 9203
        WHEN 'inpatient' THEN 9201
        WHEN 'wellness' THEN 9202
        WHEN 'urgentcare' THEN 9203 
        WHEN 'outpatient' THEN 9202
        ELSE 0
    END AS visit_detail_concept_id,
    
    av.visit_start_date AS visit_detail_start_date,
    av.visit_start_date AS visit_detail_start_datetime,
    av.visit_end_date AS visit_detail_end_date,
    av.visit_end_date AS visit_detail_end_datetime,
    44818517 AS visit_detail_type_concept_id, 
    pr.provider_id AS provider_id, 
    NULL AS care_site_id, 
    0 AS admitting_source_concept_id,
    0 AS discharge_to_concept_id, 
    lag(av.visit_occurrence_id) 
    OVER(PARTITION BY p.person_id
                      ORDER BY av.visit_start_date) + 1000000 AS preceding_visit_detail_id,
    av.encounter_id AS visit_detail_source_value,
    0 AS visit_detail_source_concept_id, 
    NULL AS admitting_source_value, 
    NULL AS discharge_to_source_value,
    cast(NULL as integer) AS parent_visit_detail_id,
    av.visit_occurrence_id AS visit_occurrence_id
FROM {{ ref('all_visits') }} AS av
INNER JOIN {{ ref('person') }} AS p
    ON av.patient = p.person_source_value
INNER JOIN {{ source('synthea', 'encounters') }} AS e
    ON av.encounter_id = e.id
        AND av.patient = e.patient
INNER JOIN {{ ref('provider') }} AS pr 
    ON e.provider = pr.provider_source_value
WHERE av.visit_occurrence_id IN (
    SELECT DISTINCT visit_occurrence_id_new
    FROM {{ ref('final_visit_ids') }})
