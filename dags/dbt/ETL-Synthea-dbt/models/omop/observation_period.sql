SELECT
    ROW_NUMBER()OVER(ORDER BY person_id) AS observation_period_id,
    person_id AS person_id,
    start_date AS observation_period_start_date,
    end_date AS observation_period_end_date,
    44814724 AS period_type_concept_id
FROM (
    SELECT
        p.person_id,
        MIN(e.start) AS start_date,
        MAX(e.stop) AS end_date
    FROM {{ ref('person') }} AS p
    INNER JOIN {{ source('synthea', 'encounters') }} AS e
        ON p.person_source_value = e.patient
    GROUP BY p.person_id
) AS tmp
