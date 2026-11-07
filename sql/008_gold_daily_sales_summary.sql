
CREATE OR REPLACE TABLE
    `clean-abacus-513514-c8.retailpulse_gold.daily_sales_summary`

PARTITION BY sales_date
CLUSTER BY sales_date

OPTIONS (
    description = "Gold daily sales summary. Grain: exactly one row per order creation calendar date. Includes order, item revenue and payment metrics."
)

AS

-- 1. Aggregate order metrics independently.
WITH order_daily AS (
    SELECT
        DATE(order_created_at) AS sales_date,

        COUNT(*) AS total_orders,

        COUNTIF(status = 'DELIVERED') AS delivered_orders,
        COUNTIF(status = 'CANCELLED') AS cancelled_orders,
        COUNTIF(status = 'RETURNED') AS returned_orders,
        COUNTIF(status = 'PROCESSING') AS processing_orders,
        COUNTIF(status = 'SHIPPED') AS shipped_orders,
        COUNTIF(status = 'PENDING') AS pending_orders,

        SUM(
            CASE
                WHEN status NOT IN ('CANCELLED', 'RETURNED')
                THEN total_amount
                ELSE 0
            END
        ) AS eligible_order_total

    FROM
        `clean-abacus-513514-c8.retailpulse_gold.fact_orders`

    GROUP BY
        sales_date
),

-- 2. Aggregate order item revenue independently.
item_daily AS (
    SELECT
        DATE(fo.order_created_at) AS sales_date,

        SUM(
            CASE
                WHEN fo.status NOT IN ('CANCELLED', 'RETURNED')
                THEN foi.line_revenue
                ELSE 0
            END
        ) AS gross_sales,

        SUM(
            CASE
                WHEN fo.status = 'DELIVERED'
                THEN foi.line_revenue
                ELSE 0
            END
        ) AS delivered_sales,

        SUM(
            CASE
                WHEN fo.status = 'RETURNED'
                THEN foi.line_revenue
                ELSE 0
            END
        ) AS returned_sales

    FROM
        `clean-abacus-513514-c8.retailpulse_gold.fact_order_items` foi

    INNER JOIN
        `clean-abacus-513514-c8.retailpulse_gold.fact_orders` fo
        ON foi.order_id = fo.order_id

    GROUP BY
        sales_date
),

-- 3. Aggregate payment metrics independently.
payment_daily AS (
    SELECT
        DATE(fo.order_created_at) AS sales_date,

        COUNTIF(fp.payment_status = 'COMPLETED')
            AS completed_payments,

        COUNTIF(fp.payment_status = 'FAILED')
            AS failed_payments,

        COUNTIF(fp.payment_status = 'PENDING')
            AS pending_payments,

        COUNTIF(fp.payment_status = 'REFUNDED')
            AS refunded_payments,

        SUM(
            CASE
                WHEN fp.payment_status = 'COMPLETED'
                THEN fp.payment_amount
                ELSE 0
            END
        ) AS completed_payment_amount,

        SUM(
            CASE
                WHEN fp.payment_status = 'REFUNDED'
                THEN fp.payment_amount
                ELSE 0
            END
        ) AS refunded_amount

    FROM
        `clean-abacus-513514-c8.retailpulse_gold.fact_payments` fp

    INNER JOIN
        `clean-abacus-513514-c8.retailpulse_gold.fact_orders` fo
        ON fp.order_id = fo.order_id

    GROUP BY
        sales_date
)

-- 4. Combine the daily aggregates.
SELECT
    o.sales_date,

    o.total_orders,
    o.delivered_orders,
    o.cancelled_orders,
    o.returned_orders,
    o.processing_orders,
    o.shipped_orders,
    o.pending_orders,

    COALESCE(i.gross_sales, 0) AS gross_sales,
    COALESCE(i.delivered_sales, 0) AS delivered_sales,
    COALESCE(i.returned_sales, 0) AS returned_sales,

    o.eligible_order_total,

    o.eligible_order_total
        - COALESCE(i.gross_sales, 0) AS order_total_variance,

    COALESCE(p.completed_payments, 0) AS completed_payments,
    COALESCE(p.failed_payments, 0) AS failed_payments,
    COALESCE(p.pending_payments, 0) AS pending_payments,
    COALESCE(p.refunded_payments, 0) AS refunded_payments,

    COALESCE(p.completed_payment_amount, 0)
        AS completed_payment_amount,

    COALESCE(p.refunded_amount, 0) AS refunded_amount,

    COALESCE(p.completed_payment_amount, 0)
        - COALESCE(p.refunded_amount, 0)
        AS net_collected_revenue

FROM
    order_daily o

LEFT JOIN
    item_daily i
    ON o.sales_date = i.sales_date

LEFT JOIN
    payment_daily p
    ON o.sales_date = p.sales_date;
