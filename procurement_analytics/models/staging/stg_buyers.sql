SELECT
    r.release_id,
    r.ocid,
    r.release_date,
    p.value ->> 'id' AS buyer_id,
    p.value ->> 'name' AS buyer_name,
    p.value -> 'address' ->> 'locality' AS locality,
    p.value -> 'address' ->> 'postalCode' AS postcode,
    p.value -> 'address' ->> 'region' AS region,
    p.value -> 'address' ->> 'country' AS country
FROM {{ source('procurement', 'raw_releases') }} r,
     json_each(r.raw_json, '$.parties') AS p
WHERE json_contains(p.value -> 'roles', '"buyer"')