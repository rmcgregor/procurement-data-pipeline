WITH divisions AS (
    SELECT DISTINCT
        cpv_division_code
    FROM {{ ref('dim_cpv_categories') }}
),

division_names AS (
    SELECT
        cpv_division_code,
        MAX(cpv_description) AS division_name
    FROM {{ ref('dim_cpv_categories') }}
    WHERE RIGHT(CAST(cpv_code AS VARCHAR), 6) = '000000'
    GROUP BY cpv_division_code
)

SELECT
    d.cpv_division_code,
    COALESCE(
        n.division_name,
        CASE d.cpv_division_code
            WHEN '14' THEN 'Mining, basic metals and related products'
            WHEN '19' THEN 'Leather and textile fabrics, plastic and rubber materials'
        END
    ) AS division_name
FROM divisions d
LEFT JOIN division_names n
    ON d.cpv_division_code = n.cpv_division_code