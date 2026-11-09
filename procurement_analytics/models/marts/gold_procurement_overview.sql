WITH release_dates AS (
    SELECT
        ocid,
        MIN(release_date) AS first_release_date
    FROM {{ ref('stg_releases') }}
    GROUP BY ocid
),

framework_flags AS (
    SELECT
        ocid,
        MAX(
            CASE
                WHEN json_extract_string(
                    raw_json,
                    '$.tender.techniques.hasFrameworkAgreement'
                ) = 'true'
                THEN 1 ELSE 0
            END
        ) AS has_framework,
        MAX(
            CASE
                WHEN json_extract_string(
                    raw_json,
                    '$.tender.techniques.hasFrameworkAgreement'
                ) = 'false'
                THEN 1 ELSE 0
            END
        ) AS explicitly_not_framework
    FROM {{ source('procurement', 'raw_releases') }}
    GROUP BY ocid
)

SELECT
    cp.ocid,
    cp.release_id,
    rd.first_release_date as first_release_timestamp,
    cp.release_date as latest_release_timestamp,
    CAST(rd.first_release_date AS DATE) AS first_release_date,
    CAST(cp.release_date AS DATE) AS latest_release_date,
    cp.buyer_name,
    cp.tender_title,
    cp.tender_status,
    cp.procurement_method,
    cp.tender_value,
    cp.currency,
    CASE
        WHEN ff.has_framework = 1 THEN 'Framework'
        WHEN ff.explicitly_not_framework = 1 THEN 'Not framework'
        ELSE 'Unknown'
    END AS framework_status
FROM {{ ref('current_procurements') }} AS cp
LEFT JOIN release_dates AS rd
    ON cp.ocid = rd.ocid
LEFT JOIN framework_flags AS ff
    ON cp.ocid = ff.ocid