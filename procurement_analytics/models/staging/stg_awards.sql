SELECT
    r.release_id,
    r.ocid,
    r.release_date,
    a.value ->> '$.id' AS award_id,
    a.value ->> '$.title' AS award_title,
    a.value ->> '$.status' as award_status,
    a.value ->> '$.mainProcurementCategory' as procurement_category,
    CAST(a.value ->> '$.date' AS TIMESTAMPTZ) AS award_date,
    CAST(a.value ->> '$.value.amount' AS DECIMAL(19,2)) AS award_value,
    CAST(a.value ->> '$.value.amountGross' AS DECIMAL(19,2)) AS award_value_gross,
    a.value ->> '$.value.currency' AS currency
FROM {{ source('procurement', 'raw_releases') }} r,
     json_each(r.raw_json, '$.awards') a