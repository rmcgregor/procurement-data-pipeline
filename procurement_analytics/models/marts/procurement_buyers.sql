SELECT DISTINCT
    ocid,
    buyer_id

FROM {{ ref('stg_buyers') }}