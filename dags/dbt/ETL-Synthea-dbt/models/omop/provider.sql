SELECT
    ROW_NUMBER()OVER(ORDER BY (SELECT NULL)) AS provider_id,
    name AS provider_name,
    CAST(NULL AS varchar(20)) AS npi,
    CAST(NULL AS varchar(20)) AS dea,
    38004446 AS specialty_concept_id,
    CAST(NULL AS integer) AS care_site_id,
    CAST(NULL AS integer) AS year_of_birth,
    CASE UPPER(gender)
        WHEN 'M' THEN 8507
        WHEN 'F' THEN 8532
    END AS gender_concept_id,
    id AS provider_source_value,
    speciality AS specialty_source_value,
    38004446 AS specialty_source_concept_id,
    gender AS gender_source_value,
    CASE UPPER(gender)
        WHEN 'M' THEN 8507
        WHEN 'F' THEN 8532
    END AS gender_source_concept_id
FROM {{ source('synthea', 'providers') }}
