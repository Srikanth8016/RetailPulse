-- =============================================================================
-- RetailPulse Data Engineering Platform - Gold Layer (Dimensional Modeling)
-- File: sql/005_gold_fact_orders.sql
-- Entity: Fact - fact_orders
-- Target Table: clean-abacus-513514-c8.retailpulse_gold.fact_orders
-- Source Tables:
--   - clean-abacus-513514-c8.retailpulse_silver.orders (Transactional sales source)
--   - clean-abacus-513514-c8.retailpulse_gold.dim_customers (Conformed customer dimension)
-- =============================================================================
-- Description:
--   Core transactional order fact table capturing header-level e-commerce sales.
--   Enables sales performance analytics, conversion funnel tracking, customer
--   lifetime value (LTV) calculations, and full order lifecycle analysis.
--
-- Business Rules & Data Quality Invariants:
--   1. Grain: Exactly one row per order_id.
--   2. Exclude invalid records where order_id IS NULL or customer_id IS NULL.
--   3. Referential Integrity: Enforce that every customer_id exists in dim_customers
--      via INNER JOIN, preventing orphan fact records.
--   4. Deduplication: Guarantee exactly one row per order_id using ROW_NUMBER(),
--      prioritizing latest record by updated_at DESC NULLS LAST, created_at DESC NULLS LAST.
--   5. Status Normalization: Standardize order status as UPPER(TRIM(status)).
--      All lifecycle statuses (PENDING, PROCESSING, SHIPPED, DELIVERED, CANCELLED,
--      RETURNED) are strictly preserved for lifecycle funnel analysis.
--   6. Financial Precision: Preserve total_amount as exact NUMERIC.
--   7. Timestamp Disambiguation: Alias created_at -> order_created_at and
--      updated_at -> order_updated_at.
-- =============================================================================

CREATE OR REPLACE TABLE `clean-abacus-513514-c8.retailpulse_gold.fact_orders`
CLUSTER BY customer_id
OPTIONS (
    description = "Gold fact table for RetailPulse customer orders. Grain: exactly one row per order_id."
)
AS
WITH deduplicated_orders AS (
    SELECT
        order_id,
        customer_id,
        order_date,
        UPPER(TRIM(status)) AS status,
        CAST(total_amount AS NUMERIC) AS total_amount,
        created_at AS order_created_at,
        updated_at AS order_updated_at,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY
                updated_at DESC NULLS LAST,
                created_at DESC NULLS LAST
        ) AS ranking
    FROM
        `clean-abacus-513514-c8.retailpulse_silver.orders`
    WHERE
        order_id IS NOT NULL
        AND customer_id IS NOT NULL
)
SELECT
    o.order_id,
    o.customer_id,
    o.order_date,
    o.status,
    o.total_amount,
    o.order_created_at,
    o.order_updated_at
FROM
    deduplicated_orders o
INNER JOIN
    `clean-abacus-513514-c8.retailpulse_gold.dim_customers` c
    ON o.customer_id = c.customer_id
WHERE
    o.ranking = 1;


-- =============================================================================
-- POST-LOAD VALIDATION / DATA QUALITY AUDIT QUERY
-- Execute the query below after running the DDL to verify order grain, primary key
-- uniqueness, absence of orphans against dim_customers, and monetary sanity.
-- =============================================================================
/*
SELECT
    COUNT(*) AS total_orders,
    COUNT(DISTINCT f.order_id) AS distinct_order_ids,
    COUNT(*) - COUNT(DISTINCT f.order_id) AS duplicate_order_ids,
    COUNTIF(f.order_id IS NULL) AS null_order_ids,
    COUNTIF(f.customer_id IS NULL) AS null_customer_ids,
    COUNTIF(c.customer_id IS NULL) AS orphan_customer_ids,
    COUNTIF(f.total_amount < 0 OR f.total_amount IS NULL) AS invalid_order_amounts
FROM
    `clean-abacus-513514-c8.retailpulse_gold.fact_orders` f
LEFT JOIN
    `clean-abacus-513514-c8.retailpulse_gold.dim_customers` c
    ON f.customer_id = c.customer_id;
*/
