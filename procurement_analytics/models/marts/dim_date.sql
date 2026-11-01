WITH dates AS (
    SELECT
        generate_series AS date
    FROM generate_series(
        DATE '2020-01-01',
        DATE '2035-12-31',
        INTERVAL 1 DAY
    )
)

SELECT
    date,
    CAST(STRFTIME(date, '%Y%m%d') AS INTEGER) AS date_key,
    YEAR(date) AS year,
    MONTH(date) AS month,
    DAY(date) AS day_num,
    DAYNAME(date) AS day_name,
    MONTHNAME(date) AS month_name,
    DATE_PART('week', date) AS week,
    DATE_PART('quarter', date) AS quarter_num,
    'Q' || CAST(DATE_PART('quarter', date) AS VARCHAR) AS quarter_name,
    STRFTIME(date, '%Y-%m') AS year_month,
    CASE
        WHEN DAYNAME(date) IN ('Saturday', 'Sunday') THEN TRUE
        ELSE FALSE
    END AS is_weekend
FROM dates