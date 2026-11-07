
CREATE OR REPLACE TABLE
    `clean-abacus-513514-c8.retailpulse_gold.fact_payments`

CLUSTER BY order_id, payment_status

OPTIONS (
    description = "Gold fact table for individual payment transactions. Grain: exactly one row per payment_id."
)

AS

WITH deduplicated_payments AS (
    SELECT
        payment_id,
        order_id,
        UPPER(TRIM(payment_method)) AS payment_method,
        UPPER(TRIM(status)) AS payment_status,
        CAST(amount AS NUMERIC) AS payment_amount,
        payment_date,
        created_at AS payment_created_at,
        updated_at AS payment_updated_at,

        ROW_NUMBER() OVER (
            PARTITION BY payment_id
            ORDER BY
                updated_at DESC NULLS LAST,
                created_at DESC NULLS LAST
        ) AS ranking

    FROM
        `clean-abacus-513514-c8.retailpulse_silver.payments`

    WHERE
        payment_id IS NOT NULL
        AND order_id IS NOT NULL
        AND amount > 0
)

SELECT
    p.payment_id,
    p.order_id,
    p.payment_method,
    p.payment_status,
    p.payment_amount,
    p.payment_date,
    p.payment_created_at,
    p.payment_updated_at

FROM
    deduplicated_payments p

INNER JOIN
    `clean-abacus-513514-c8.retailpulse_gold.fact_orders` fo
    ON p.order_id = fo.order_id

WHERE
    p.ranking = 1;



