-- =============================================================================
-- RetailPulse Data Engineering Platform - Gold Layer (Dimensional Modeling)
-- File: sql/006_gold_fact_order_items.sql
-- Entity: Fact - fact_order_items
-- Target Table: clean-abacus-513514-c8.retailpulse_gold.fact_order_items
-- Source Tables:
--   - clean-abacus-513514-c8.retailpulse_silver.order_items (Line-item source)
--   - clean-abacus-513514-c8.retailpulse_gold.fact_orders (Parent order fact)
--   - clean-abacus-513514-c8.retailpulse_gold.dim_products (Conformed product dimension)
-- =============================================================================
-- Description:
--   Granular line-item sales fact table capturing product quantities, locked
--   unit prices, and calculated line-item revenues for each order line.
--   Enables basket analysis, product affinity, cross-sell/up-sell reporting,
--   and category revenue rollups.
--
-- Business Rules & Data Quality Invariants:
--   1. Grain: Exactly one row per order_item_id (one product line within an order).
--   2. Exclude invalid records where order_item_id, order_id, or product_id IS NULL.
--   3. Exclude non-positive quantities (quantity <= 0) and unit prices (unit_price <= 0).
--   4. Referential Integrity: Enforce that order_id exists in fact_orders and
--      product_id exists in dim_products via INNER JOIN, preventing orphan records.
--   5. Deduplication: Guarantee exactly one row per order_item_id using ROW_NUMBER(),
--      prioritizing the latest record by created_at DESC NULLS LAST.
--   6. Financial Precision: Preserve unit_price and line_revenue as exact NUMERIC
--      (line_revenue = CAST(quantity AS NUMERIC) * CAST(unit_price AS NUMERIC)).
--   7. Timestamp Disambiguation: Alias created_at -> order_item_created_at.
-- =============================================================================

CREATE OR REPLACE TABLE `clean-abacus-513514-c8.retailpulse_gold.fact_order_items`
CLUSTER BY order_id, product_id
OPTIONS (
    description = "Gold fact table for RetailPulse individual order line items. Grain: exactly one row per order_item_id (one product line within an order)."
)
AS
WITH deduplicated_order_items AS (
    SELECT
        order_item_id,
        order_id,
        product_id,
        quantity,
        CAST(unit_price AS NUMERIC) AS unit_price,
        CAST(quantity AS NUMERIC) * CAST(unit_price AS NUMERIC) AS line_revenue,
        created_at AS order_item_created_at,
        ROW_NUMBER() OVER (
            PARTITION BY order_item_id
            ORDER BY created_at DESC NULLS LAST
        ) AS ranking
    FROM
        `clean-abacus-513514-c8.retailpulse_silver.order_items`
    WHERE
        order_item_id IS NOT NULL
        AND order_id IS NOT NULL
        AND product_id IS NOT NULL
        AND quantity > 0
        AND unit_price > 0
)
SELECT
    oi.order_item_id,
    oi.order_id,
    oi.product_id,
    oi.quantity,
    oi.unit_price,
    oi.line_revenue,
    oi.order_item_created_at
FROM
    deduplicated_order_items oi
INNER JOIN
    `clean-abacus-513514-c8.retailpulse_gold.fact_orders` fo
    ON oi.order_id = fo.order_id
INNER JOIN
    `clean-abacus-513514-c8.retailpulse_gold.dim_products` dp
    ON oi.product_id = dp.product_id
WHERE
    oi.ranking = 1;


-- =============================================================================
-- POST-LOAD VALIDATION / DATA QUALITY AUDIT QUERY
-- Execute the query below after running the DDL to verify line item grain,
-- primary key uniqueness, foreign key validity against orders and products,
-- and monetary arithmetic consistency.
-- =============================================================================
/*
SELECT
    COUNT(*) AS total_order_items,
    COUNT(DISTINCT foi.order_item_id) AS distinct_order_item_ids,
    COUNT(*) - COUNT(DISTINCT foi.order_item_id) AS duplicate_order_item_ids,
    COUNTIF(foi.order_item_id IS NULL OR foi.order_id IS NULL OR foi.product_id IS NULL) AS null_identifiers,
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
FROM
    `clean-abacus-513514-c8.retailpulse_gold.fact_order_items` foi
LEFT JOIN
    `clean-abacus-513514-c8.retailpulse_gold.fact_orders` fo
    ON foi.order_id = fo.order_id
LEFT JOIN
    `clean-abacus-513514-c8.retailpulse_gold.dim_products` dp
    ON foi.product_id = dp.product_id;
*/
