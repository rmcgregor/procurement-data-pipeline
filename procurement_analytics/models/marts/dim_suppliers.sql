    WITH supplier_keys AS (
    SELECT
        s.supplier_id,
        s.supplier_name,
        s.release_id,
        r.release_date,
        CASE
            WHEN s.supplier_id = 'GB-COH-0'
                THEN UPPER(TRIM(s.supplier_name))
            ELSE s.supplier_id
        END AS supplier_key
    FROM {{ ref('stg_award_suppliers') }} s
    JOIN {{ ref('stg_releases') }} r
        ON s.release_id = r.release_id   
),
ranked_suppliers AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY supplier_key
            ORDER BY release_date DESC
        ) AS supplier_rank
    FROM supplier_keys
)

SELECT  supplier_key,
        supplier_id,
        supplier_name,
        release_id,
        release_date
FROM ranked_suppliers
WHERE supplier_rank = 1