-- =============================================================================
-- RetailPulse Data Engineering Platform - Gold Layer (Dimensional Modeling)
-- File: sql/004_gold_dim_products.sql
-- Entity: Dimension - dim_products
-- Target Table: clean-abacus-513514-c8.retailpulse_gold.dim_products
-- Source Table: clean-abacus-513514-c8.retailpulse_silver.products
-- =============================================================================
-- Description:
--   Transforms cleaned Silver product catalog records into a conformed product
--   dimension (dim_products) for analytical querying, category reporting,
--   inventory monitoring, and dimensional joins with order item sales facts.
--
-- Business Rules & Data Quality Invariants:
--   1. Grain: Exactly one row per product (product_id).
--   2. Exclude invalid records where product_id IS NULL.
--   3. Deduplicate multiple versions of the same product using ROW_NUMBER(),
--      prioritizing the latest version by updated_at DESC NULLS LAST,
--      followed by created_at DESC NULLS LAST.
--   4. Preserve financial accuracy by maintaining price as exact NUMERIC.
--   5. Disambiguate timestamp lineage by aliasing created_at to product_created_at
--      and updated_at to product_updated_at.
-- =============================================================================

CREATE OR REPLACE TABLE `clean-abacus-513514-c8.retailpulse_gold.dim_products`
CLUSTER BY product_id, category
OPTIONS (
    description = "Gold dimensional model for RetailPulse products catalog. Grain: one row per product."
)
AS
WITH ranked_products AS (
    SELECT
        product_id,
        product_name,
        category,
        CAST(price AS NUMERIC) AS price,
        stock_quantity,
        created_at AS product_created_at,
        updated_at AS product_updated_at,
        ROW_NUMBER() OVER (
            PARTITION BY product_id
            ORDER BY
                updated_at DESC NULLS LAST,
                created_at DESC NULLS LAST
        ) AS ranking
    FROM
        `clean-abacus-513514-c8.retailpulse_silver.products`
    WHERE
        product_id IS NOT NULL
)
SELECT
    product_id,
    product_name,
    category,
    price,
    stock_quantity,
    product_created_at,
    product_updated_at
FROM
    ranked_products
WHERE
    ranking = 1;


-- =============================================================================
-- POST-LOAD VALIDATION / DATA QUALITY AUDIT QUERY
-- Execute the query below after running the DDL to verify entity grain,
-- primary key uniqueness, price validity, and inventory integrity.
-- =============================================================================
/*
SELECT
    COUNT(*) AS total_products,
    COUNT(DISTINCT product_id) AS distinct_product_ids,
    COUNT(*) - COUNT(DISTINCT product_id) AS duplicate_product_ids,
    COUNTIF(product_id IS NULL) AS null_product_ids,
    COUNTIF(price <= 0 OR price IS NULL) AS invalid_prices,
    COUNTIF(stock_quantity < 0 OR stock_quantity IS NULL) AS negative_stock_quantities
FROM
    `clean-abacus-513514-c8.retailpulse_gold.dim_products`;
*/
