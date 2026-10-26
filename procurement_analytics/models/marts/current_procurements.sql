with ranked_releases as(
    SELECT  
    ocid,
    release_id,
    release_date,
    buyer_name,
    tender_title,
    tender_status,
    procurement_method,
    tender_value,
    currency,
    ROW_NUMBER() OVER (
    PARTITION BY ocid
    ORDER BY release_date DESC
    ) AS release_rank
    FROM {{ ref('stg_releases') }})

    select ocid,
    release_id,
    release_date,
    buyer_name,
    tender_title,
    tender_status,
    procurement_method,
    tender_value,
    currency from ranked_releases where release_rank = 1