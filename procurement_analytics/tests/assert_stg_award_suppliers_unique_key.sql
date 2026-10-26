SELECT
    release_id,
    award_id,
    supplier_id,
    COUNT(*) AS total
FROM {{ ref('stg_award_suppliers') }}
GROUP BY
    release_id,
    award_id,
    supplier_id
HAVING COUNT(*) > 1