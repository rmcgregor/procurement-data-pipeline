SELECT
    cp.ocid,
    cp.release_id,
    cp.release_date,
    cp.buyer_name,
    cp.tender_title,
    cp.tender_status,
    cp.procurement_method,
    cp.tender_value,
    cp.currency
FROM {{ ref('current_procurements') }} AS cp