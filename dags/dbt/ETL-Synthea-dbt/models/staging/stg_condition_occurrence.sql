SELECT
    p.person_id AS person_id,
    c.code,
    c.start,
    c.stop,
    fv.visit_occurrence_id_new
FROM {{ source('synthea', 'conditions') }} AS c
LEFT JOIN {{ ref('final_visit_ids') }} AS fv
    ON fv.encounter_id = c.encounter
INNER JOIN {{ ref('person') }} AS p
    ON c.patient = p.person_source_value
