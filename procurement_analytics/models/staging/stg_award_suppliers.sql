 SELECT
   r.release_id,
a.value ->> '$.id' AS award_id,
b.value ->> '$.id' AS supplier_id,
b.value ->> '$.name' AS supplier_name

FROM {{ source('procurement', 'raw_releases') }} r,
     json_each(r.raw_json, '$.awards') a,
     json_each(a.value, '$.suppliers') b