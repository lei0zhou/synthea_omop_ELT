/* Inpatient visits */
/* Collapse IP claim lines with <=1 day between them into one visit */

WITH CTE_END_DATES AS (
    SELECT
        patient,
        encounterclass,
        EVENT_DATE - INTERVAL '1 day' AS END_DATE
    FROM (
        SELECT
            patient,
            encounterclass,
            EVENT_DATE,
            EVENT_TYPE,
            max(
                START_ORDINAL
            ) OVER (
                PARTITION BY patient, encounterclass ORDER BY EVENT_DATE, EVENT_TYPE ROWS UNBOUNDED PRECEDING
            ) AS START_ORDINAL,
            row_number() OVER (PARTITION BY patient, encounterclass ORDER BY EVENT_DATE, EVENT_TYPE) AS OVERALL_ORD
        FROM (
            SELECT
                patient,
                encounterclass,
                start AS EVENT_DATE,
                -1 AS EVENT_TYPE,
                row_number() OVER (PARTITION BY patient, encounterclass ORDER BY start, stop) AS START_ORDINAL
            FROM {{ source('synthea', 'encounters') }}
            WHERE encounterclass = 'inpatient'
            UNION ALL
            SELECT
                patient,
                encounterclass,
                stop + INTERVAL '1 day' AS EVENT_DATE,
                1 AS EVENT_TYPE,
                NULL
            FROM {{ source('synthea', 'encounters') }}
            WHERE encounterclass = 'inpatient'
        ) AS RAWDATA
    ) AS E
    WHERE (2 * E.START_ORDINAL - E.OVERALL_ORD = 0)
),

CTE_VISIT_ENDS AS (
    SELECT
        min(V.id) AS encounter_id,
        V.patient,
        V.encounterclass,
        V.start AS VISIT_START_DATE,
        min(E.END_DATE) AS VISIT_END_DATE
    FROM {{ source('synthea', 'encounters') }} AS V
    INNER JOIN CTE_END_DATES AS E
        ON V.patient = E.patient
            AND V.encounterclass = E.encounterclass
            AND E.END_DATE >= V.start
    GROUP BY V.patient, V.encounterclass, V.start
),

IP_VISITS AS (
    SELECT
        T2.encounter_id,
        T2.patient,
        T2.encounterclass,
        T2.VISIT_START_DATE,
        T2.VISIT_END_DATE
        {# INTO {{ ref('IP_VISITS') }} #}
    FROM (
            SELECT
                encounter_id,
                patient,
                encounterclass,
                min(VISIT_START_DATE) AS VISIT_START_DATE,
                VISIT_END_DATE
            FROM CTE_VISIT_ENDS
            GROUP BY encounter_id, patient, encounterclass, VISIT_END_DATE
        ) AS T2
),


/* Emergency visits */
/* collapse ER claim lines with no days between them into one visit */

ER_VISITS AS (
    SELECT
        T2.encounter_id,
        T2.patient,
        T2.encounterclass,
        T2.VISIT_START_DATE,
        T2.VISIT_END_DATE
        {# INTO {{ ref('ER_VISITS') }} #}
    FROM (
            SELECT
                min(encounter_id) AS encounter_id,
                patient,
                encounterclass,
                VISIT_START_DATE,
                max(VISIT_END_DATE) AS VISIT_END_DATE
            FROM (
                    SELECT
                        CL1.id AS encounter_id,
                        CL1.patient,
                        CL1.encounterclass,
                        CL1.start AS VISIT_START_DATE,
                        CL2.stop AS VISIT_END_DATE
                    FROM {{ source('synthea', 'encounters') }} AS CL1
                    INNER JOIN {{ source('synthea', 'encounters') }} AS CL2
                        ON CL1.patient = CL2.patient
                            AND CL1.start = CL2.start
                            AND CL1.encounterclass = CL2.encounterclass
                    WHERE CL1.encounterclass IN ('emergency', 'urgent')
                ) AS T1
            GROUP BY patient, encounterclass, VISIT_START_DATE
        ) AS T2
),


/* Outpatient visits */

CTE_VISITS_DISTINCT AS (
    SELECT
        min(id) AS encounter_id,
        patient,
        encounterclass,
        start AS VISIT_START_DATE,
        stop AS VISIT_END_DATE
    FROM {{ source('synthea', 'encounters') }}
    WHERE encounterclass IN ('ambulatory', 'wellness', 'outpatient')
    GROUP BY patient, encounterclass, start, stop
),

OP_VISITS AS (
    SELECT
        min(encounter_id) AS encounter_id,
        patient,
        encounterclass,
        VISIT_START_DATE,
        max(VISIT_END_DATE) AS VISIT_END_DATE
        {# INTO {{ ref('OP_VISITS') }} #}
    FROM CTE_VISITS_DISTINCT
    GROUP BY patient, encounterclass, VISIT_START_DATE
)

/* All visits */

SELECT
    *,
    row_number()OVER(ORDER BY patient) AS visit_occurrence_id
FROM
    (
        SELECT * FROM IP_VISITS
    
        UNION ALL
        SELECT * FROM ER_VISITS
    
        UNION ALL
        SELECT * FROM OP_VISITS
    ) AS T1
