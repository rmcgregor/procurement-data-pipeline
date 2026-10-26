SELECT
    ocid,
    award_id,
    COUNT(*) AS total
FROM {{ ref('current_awards') }}
GROUP BY ocid, award_id
HAVING COUNT(*) > 1