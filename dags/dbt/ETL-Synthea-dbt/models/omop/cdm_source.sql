SELECT
    '@cdm_source_name' AS cdm_source_name,
    '@cdm_source_abbreviation' AS cdm_source_abbreviation,
    '@cdm_holder' AS cdm_holder,
    '@source_description' AS source_description,
    'https://synthetichealth.github.io/synthea/' AS source_documentation_reference,
    'https://github.com/OHDSI/ETL-Synthea' AS cdm_etl_reference,
    now() AS source_release_date, -- NB: Set this value to the day the source data was pulled
    now() AS cdm_release_date,
    '@cdm_version' AS cdm_version,
    vocabulary_version
FROM {{ source('vocab', 'vocabulary') }}
WHERE vocabulary_id = 'None'
