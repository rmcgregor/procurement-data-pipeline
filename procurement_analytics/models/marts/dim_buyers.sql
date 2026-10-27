WITH ranked_buyers AS (

    SELECT
        buyer_id,
        buyer_name,
        locality,
        postcode,
        region,
        country,
        release_date,

        ROW_NUMBER() OVER (
            PARTITION BY buyer_id
            ORDER BY release_date DESC
        ) AS row_num

    FROM {{ ref('stg_buyers') }}

)

SELECT
    buyer_id,
    buyer_name,
    locality,
    postcode,
    region,
    country,
    release_date AS latest_release_date

FROM ranked_buyers

WHERE row_num = 1