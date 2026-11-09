WITH latest_classified_release AS (
    SELECT
        ocid,
        release_id,
        ROW_NUMBER() OVER (
            PARTITION BY ocid
            ORDER BY release_date DESC, release_id DESC
        ) AS row_num
    FROM (
        SELECT DISTINCT
            ocid,
            release_id,
            release_date
        FROM {{ ref('stg_procurement_cpv') }}
    )
),

procurement_cpv AS (
    SELECT
        s.ocid,
        s.cpv_code
    FROM {{ ref('stg_procurement_cpv') }} AS s
    INNER JOIN latest_classified_release AS l
        ON s.ocid = l.ocid
       AND s.release_id = l.release_id
    WHERE l.row_num = 1
)

SELECT DISTINCT
    ocid,
    cpv_code
FROM procurement_cpv