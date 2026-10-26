SELECT
    a.ocid,
    a.release_id,
    a.award_id,
    CASE
    WHEN s.supplier_id = 'GB-COH-0'
        THEN UPPER(TRIM(s.supplier_name))
    ELSE s.supplier_id
    END AS supplier_key
FROM {{ ref('current_awards') }} a
JOIN {{ ref('stg_award_suppliers') }} s
    on a.release_id = s.release_id
    and a.award_id = s.award_id