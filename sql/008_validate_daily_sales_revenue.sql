
WITH item_daily AS (
    SELECT
        DATE(fo.order_created_at) AS sales_date,

        SUM(
            CASE
                WHEN fo.status NOT IN ('CANCELLED', 'RETURNED')
                THEN foi.line_revenue
                ELSE 0
            END
        ) AS expected_gross_sales,

        SUM(
            CASE
                WHEN fo.status = 'DELIVERED'
                THEN foi.line_revenue
                ELSE 0
            END
        ) AS expected_delivered_sales,

        SUM(
            CASE
                WHEN fo.status = 'RETURNED'
                THEN foi.line_revenue
                ELSE 0
            END
        ) AS expected_returned_sales

    FROM
        `clean-abacus-513514-c8.retailpulse_gold.fact_order_items` foi

    INNER JOIN
        `clean-abacus-513514-c8.retailpulse_gold.fact_orders` fo
        ON foi.order_id = fo.order_id

    GROUP BY
        sales_date
),

summary_checks AS (
    SELECT
        COUNT(*) AS summary_days,
        COUNT(DISTINCT sales_date) AS distinct_sales_dates,
        COUNTIF(sales_date IS NULL) AS null_sales_dates,
        SUM(gross_sales) AS actual_gross_sales,
        SUM(delivered_sales) AS actual_delivered_sales,
        SUM(returned_sales) AS actual_returned_sales,
        SUM(eligible_order_total) AS eligible_order_total,
        SUM(order_total_variance) AS total_order_variance
    FROM
        `clean-abacus-513514-c8.retailpulse_gold.daily_sales_summary`
),

reconciliation AS (
    SELECT
        COUNT(*) AS compared_days,

        COUNTIF(
            ABS(s.gross_sales - i.expected_gross_sales) > 0.01
        ) AS gross_sales_mismatch_days,

        COUNTIF(
            ABS(s.delivered_sales - i.expected_delivered_sales) > 0.01
        ) AS delivered_sales_mismatch_days,

        COUNTIF(
            ABS(s.returned_sales - i.expected_returned_sales) > 0.01
        ) AS returned_sales_mismatch_days,

        SUM(i.expected_gross_sales) AS expected_gross_sales,
        SUM(i.expected_delivered_sales) AS expected_delivered_sales,
        SUM(i.expected_returned_sales) AS expected_returned_sales

    FROM
        `clean-abacus-513514-c8.retailpulse_gold.daily_sales_summary` s

    INNER JOIN
        item_daily i
        ON s.sales_date = i.sales_date
)

SELECT
    sc.summary_days,
    sc.distinct_sales_dates,
    sc.null_sales_dates,

    r.compared_days,

    sc.actual_gross_sales,
    r.expected_gross_sales,

    sc.actual_delivered_sales,
    r.expected_delivered_sales,

    sc.actual_returned_sales,
    r.expected_returned_sales,

    r.gross_sales_mismatch_days,
    r.delivered_sales_mismatch_days,
    r.returned_sales_mismatch_days,

    sc.eligible_order_total,
    sc.total_order_variance

FROM
    summary_checks sc
CROSS JOIN
    reconciliation r;
