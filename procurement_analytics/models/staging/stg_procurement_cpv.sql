SELECT
    r.release_id,
    r.ocid,
    r.release_date,
    json_extract_string(c.value, '$.id') AS cpv_code,
    json_extract_string(c.value, '$.description') AS cpv_description
FROM {{ source('procurement', 'raw_releases') }} AS r,
     json_each(r.raw_json, '$.tender.items') AS i,
     json_each(i.value, '$.additionalClassifications') AS c
WHERE json_extract_string(c.value, '$.scheme') = 'CPV'
  AND json_extract_string(c.value, '$.id') IS NOT NULL