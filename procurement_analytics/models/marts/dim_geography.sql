SELECT
    postcode_key,
    postcode,
    country_code,
    country_name,
    region_code,
    region_name,
    local_authority_code,
    local_authority_name,
    itl1_code,
    itl1_name,
    itl2_code,
    itl2_name,
    itl3_code,
    itl3_name,
    latitude,
    longitude
FROM {{ source('procurement', 'ref_geography') }}