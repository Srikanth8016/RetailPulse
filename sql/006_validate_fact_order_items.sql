SELECT
    COUNT(*) AS total_order_items,
    COUNT(DISTINCT foi.order_item_id) AS distinct_order_item_ids,
    COUNT(*) - COUNT(DISTINCT foi.order_item_id) AS duplicate_order_item_ids,
    COUNTIF(foi.order_item_id IS NULL
        OR foi.order_id IS NULL
        OR foi.product_id IS NULL) AS null_identifiers,
    COUNTIF(foi.quantity <= 0 OR foi.quantity IS NULL) AS invalid_quantities,
    COUNTIF(foi.unit_price <= 0 OR foi.unit_price IS NULL) AS invalid_unit_prices,
    COUNTIF(fo.order_id IS NULL) AS orphan_orders,
    COUNTIF(dp.product_id IS NULL) AS orphan_products,
    COUNTIF(
        foi.line_revenue <= 0
        OR foi.line_revenue IS NULL
        OR foi.line_revenue IS DISTINCT FROM
            CAST(foi.quantity AS NUMERIC) * foi.unit_price
    ) AS invalid_line_revenues
FROM `clean-abacus-513514-c8.retailpulse_gold.fact_order_items` foi
LEFT JOIN `clean-abacus-513514-c8.retailpulse_gold.fact_orders` fo
    ON foi.order_id = fo.order_id
LEFT JOIN `clean-abacus-513514-c8.retailpulse_gold.dim_products` dp
    ON foi.product_id = dp.product_id;