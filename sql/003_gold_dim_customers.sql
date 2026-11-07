-- =============================================================================
-- RetailPulse Data Engineering Platform - Gold Layer (Dimensional Modeling)
-- File: sql/003_gold_dim_customers.sql
-- Entity: Dimension - dim_customers
-- Target Table: clean-abacus-513514-c8.retailpulse_gold.dim_customers
-- Source Table: clean-abacus-513514-c8.retailpulse_silver.customers
-- =============================================================================
-- Description:
--   Transforms cleaned Silver customer entity into a standardized Conformed 
--   Dimension for analytical querying, customer 360 dashboards, and dimensional
--   joins against fact tables (fct_orders, fct_order_items, fct_payments).
--
-- Business Rules & Data Quality Invariants:
--   1. Exclude records where customer_id IS NULL (Primary Key integrity).
--   2. Enforce grain of exactly one record per customer_id by taking the most
--      recent record based on updated_at and created_at timestamps.
--   3. Clarify timestamp provenance by aliasing created_at to customer_created_at
--      and updated_at to customer_updated_at.
-- =============================================================================

CREATE OR REPLACE TABLE `clean-abacus-513514-c8.retailpulse_gold.dim_customers`
CLUSTER BY customer_id
OPTIONS (
    description = "Gold dimensional model for RetailPulse customer accounts. Grain: 1 row per customer_id."
)
AS
WITH ranked_customers AS (
    SELECT
        customer_id,
        first_name,
        last_name,
        email,
        city,
        country,
        created_at AS customer_created_at,
        updated_at AS customer_updated_at,
        -- Deterministically break ties using updated_at and created_at
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY
                updated_at DESC,
                created_at DESC
        ) AS ranking
    FROM
        `clean-abacus-513514-c8.retailpulse_silver.customers`
    WHERE
        customer_id IS NOT NULL
)
SELECT
    customer_id,
    first_name,
    last_name,
    email,
    city,
    country,
    customer_created_at,
    customer_updated_at
FROM
    ranked_customers
WHERE
    ranking = 1;


-- =============================================================================
-- POST-LOAD VALIDATION / DATA QUALITY AUDIT QUERY
-- Execute the query below to verify table row counts, distinctness, duplicate counts,
-- and NULL constraint adherence.
-- =============================================================================
/*
SELECT
    COUNT(*) AS total_customer_count,
    COUNT(DISTINCT customer_id) AS distinct_customer_count,
    COUNT(*) - COUNT(DISTINCT customer_id) AS duplicate_customer_ids,
    COUNTIF(customer_id IS NULL) AS null_customer_ids
FROM
    `clean-abacus-513514-c8.retailpulse_gold.dim_customers`;
*/
