WITH first_releases AS (

    SELECT
        ocid,
        MIN(release_date) AS first_release_date
    FROM {{ ref('stg_releases') }}
    GROUP BY ocid

),

new_procurements AS (

    SELECT
        CAST(first_release_date AS DATE) AS activity_date,
        COUNT(*) AS new_procurements
    FROM first_releases
    GROUP BY CAST(first_release_date AS DATE)

),

updated_procurements AS (

    SELECT
        CAST(r.release_date AS DATE) AS activity_date,
        COUNT(DISTINCT r.ocid) AS updated_procurements
    FROM {{ ref('stg_releases') }} r
    INNER JOIN first_releases f
        ON r.ocid = f.ocid
    WHERE CAST(r.release_date AS DATE)
          > CAST(f.first_release_date AS DATE)
    GROUP BY CAST(r.release_date AS DATE)

)

SELECT
    n.activity_date,
    n.new_procurements,
    COALESCE(u.updated_procurements, 0) AS updated_procurements
FROM new_procurements n
LEFT JOIN updated_procurements u
    ON n.activity_date = u.activity_date
ORDER BY n.activity_date