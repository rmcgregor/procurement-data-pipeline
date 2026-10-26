SELECT
    ocid,
    award_id,
    supplier_key,
    COUNT(*)
FROM {{ ref('award_suppliers') }}
GROUP BY
    ocid,
    award_id,
    supplier_key
HAVING COUNT(*) > 1