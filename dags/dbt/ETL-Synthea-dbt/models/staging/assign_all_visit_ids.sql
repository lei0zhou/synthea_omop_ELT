/*Assign VISIT_OCCURRENCE_ID to all encounters*/

SELECT
    E.id AS encounter_id,
    E.patient AS person_source_value,
    E.start AS date_service,
    E.stop AS date_service_end,
    E.encounterclass,
    AV.encounterclass AS VISIT_TYPE,
    AV.VISIT_START_DATE,
    AV.VISIT_END_DATE,
    AV.VISIT_OCCURRENCE_ID,
    CASE
        WHEN E.encounterclass = 'inpatient' AND AV.encounterclass = 'inpatient'
            THEN VISIT_OCCURRENCE_ID
        WHEN E.encounterclass IN ('emergency', 'urgent')
            THEN (
                CASE
                    WHEN AV.encounterclass = 'inpatient' AND E.start > AV.VISIT_START_DATE
                        THEN VISIT_OCCURRENCE_ID
                    WHEN AV.encounterclass IN ('emergency', 'urgent') AND E.start = AV.VISIT_START_DATE
                        THEN VISIT_OCCURRENCE_ID
                END
            )
        WHEN E.encounterclass IN ('ambulatory', 'wellness', 'outpatient')
            THEN (
                CASE
                    WHEN AV.encounterclass = 'inpatient' AND E.start >= AV.VISIT_START_DATE
                        THEN VISIT_OCCURRENCE_ID
                    WHEN AV.encounterclass IN ('ambulatory', 'wellness', 'outpatient')
                        THEN VISIT_OCCURRENCE_ID
                END
            )
    END AS VISIT_OCCURRENCE_ID_NEW
FROM {{ source('synthea', 'encounters') }} AS E
INNER JOIN {{ ref('all_visits') }} AS AV
    ON E.patient = AV.patient
        AND E.start >= AV.VISIT_START_DATE
        AND E.start <= AV.VISIT_END_DATE
