SELECT
    cpv_code,
    MAX(cpv_description) AS cpv_description,
    LEFT(CAST(cpv_code AS VARCHAR), 2) AS cpv_division_code
FROM {{ ref('stg_procurement_cpv') }}
GROUP BY cpv_code