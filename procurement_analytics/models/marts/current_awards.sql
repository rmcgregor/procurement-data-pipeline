SELECT
    p.ocid,
    p.release_id,
    p.release_date,
    p.buyer_name,
    p.tender_title,
    a.award_id,
    a.award_title,
    a.award_status,
    a.award_date,
    a.award_value,
    a.award_value_gross,
    a.currency
FROM {{ ref('current_procurements') }} p
JOIN {{ ref('stg_awards') }} a
    ON p.release_id = a.release_id