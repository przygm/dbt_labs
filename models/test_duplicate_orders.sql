WITH
source_data AS (
    SELECT
        *
    FROM
        {{ ref('stg_stripe__payments') }}
),

keys AS (
    SELECT
        {{ dbt_utils.surrogate_key(['payment_id', 'order_id']) }} AS surrogate_key,
        COUNT(*) AS row_count
    FROM
        source_data
    GROUP BY 1
)

SELECT * FROM keys WHERE row_count > 1