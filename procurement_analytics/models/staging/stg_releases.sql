SELECT
    release_id,
    ocid,
    release_date,
    tags,
    raw_json ->> '$.buyer.name' AS buyer_name,
    raw_json ->> '$.tender.title' as tender_title,
    raw_json ->> '$.tender.status' as tender_status,
    raw_json ->> '$.tender.procurementMethod' as procurement_method,
    cast(raw_json ->> '$.tender.value.amount' as DECIMAL(19,2)) as tender_value,
    raw_json ->> '$.tender.value.currency' as currency
FROM {{ source('procurement', 'raw_releases') }}