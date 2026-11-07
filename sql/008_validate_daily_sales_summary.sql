
WITH summary_totals AS (
    SELECT
        SUM(total_orders) AS total_orders,
        SUM(delivered_orders) AS delivered_orders,
        SUM(cancelled_orders) AS cancelled_orders,
        SUM(returned_orders) AS returned_orders,
        SUM(processing_orders) AS processing_orders,
        SUM(shipped_orders) AS shipped_orders,
        SUM(pending_orders) AS pending_orders,
        SUM(gross_sales) AS gross_sales,
        SUM(delivered_sales) AS delivered_sales,
        SUM(returned_sales) AS returned_sales,
        SUM(completed_payments) AS completed_payments,
        SUM(failed_payments) AS failed_payments,
        SUM(pending_payments) AS pending_payments,
        SUM(refunded_payments) AS refunded_payments,
        SUM(completed_payment_amount) AS completed_payment_amount,
        SUM(refunded_amount) AS refunded_amount,
        SUM(net_collected_revenue) AS net_collected_revenue,
        COUNT(*) AS summary_days
    FROM
        `clean-abacus-513514-c8.retailpulse_gold.daily_sales_summary`
),

order_totals AS (
    SELECT
        COUNT(*) AS source_orders,
        COUNTIF(status = 'DELIVERED') AS source_delivered_orders,
        COUNTIF(status = 'CANCELLED') AS source_cancelled_orders,
        COUNTIF(status = 'RETURNED') AS source_returned_orders,
        COUNTIF(status = 'PROCESSING') AS source_processing_orders,
        COUNTIF(status = 'SHIPPED') AS source_shipped_orders,
        COUNTIF(status = 'PENDING') AS source_pending_orders
    FROM
        `clean-abacus-513514-c8.retailpulse_gold.fact_orders`
),

payment_totals AS (
    SELECT
        COUNTIF(payment_status = 'COMPLETED') AS source_completed_payments,
        COUNTIF(payment_status = 'FAILED') AS source_failed_payments,
        COUNTIF(payment_status = 'PENDING') AS source_pending_payments,
        COUNTIF(payment_status = 'REFUNDED') AS source_refunded_payments,
        SUM(IF(payment_status = 'COMPLETED', payment_amount, 0))
            AS source_completed_amount,
        SUM(IF(payment_status = 'REFUNDED', payment_amount, 0))
            AS source_refunded_amount
    FROM
        `clean-abacus-513514-c8.retailpulse_gold.fact_payments`
)

SELECT
    s.summary_days,

    s.total_orders,
    o.source_orders,
    s.total_orders - o.source_orders AS order_count_difference,

    s.delivered_orders - o.source_delivered_orders
        AS delivered_order_difference,

    s.cancelled_orders - o.source_cancelled_orders
        AS cancelled_order_difference,

    s.returned_orders - o.source_returned_orders
        AS returned_order_difference,

    s.processing_orders - o.source_processing_orders
        AS processing_order_difference,

    s.shipped_orders - o.source_shipped_orders
        AS shipped_order_difference,

    s.pending_orders - o.source_pending_orders
        AS pending_order_difference,

    s.completed_payments - p.source_completed_payments
        AS completed_payment_difference,

    s.failed_payments - p.source_failed_payments
        AS failed_payment_difference,

    s.pending_payments - p.source_pending_payments
        AS pending_payment_difference,

    s.refunded_payments - p.source_refunded_payments
        AS refunded_payment_difference,

    s.completed_payment_amount - p.source_completed_amount
        AS completed_amount_difference,

    s.refunded_amount - p.source_refunded_amount
        AS refunded_amount_difference,

    s.net_collected_revenue
        - (p.source_completed_amount - p.source_refunded_amount)
        AS net_revenue_difference,

    s.gross_sales,
    s.delivered_sales,
    s.returned_sales

FROM summary_totals s
CROSS JOIN order_totals o
CROSS JOIN payment_totals p;
