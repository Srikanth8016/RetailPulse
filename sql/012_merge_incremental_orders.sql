
-- RetailPulse: Incremental orders MERGE
-- Source: retailpulse_silver.orders_incremental_staging
-- Target: retailpulse_gold.fact_orders
-- Grain: one row per order_id

-- STEP 1: Validate source keys.
ASSERT (
    SELECT COUNT(*) = COUNT(DISTINCT order_id)
    FROM `clean-abacus-513514-c8.retailpulse_silver.orders_incremental_staging`
) AS 'Duplicate order_id values found in staging. MERGE stopped.';

-- STEP 2: Validate required fields and amounts.
ASSERT (
    SELECT COUNTIF(
        order_id IS NULL
        OR customer_id IS NULL
        OR order_date IS NULL
        OR status IS NULL
        OR total_amount IS NULL
        OR created_at IS NULL
        OR updated_at IS NULL
        OR total_amount < 0
    ) = 0
    FROM `clean-abacus-513514-c8.retailpulse_silver.orders_incremental_staging`
) AS 'Invalid or missing order fields found. MERGE stopped.';

-- STEP 3: Validate allowed order statuses.
ASSERT (
    SELECT COUNTIF(
        status NOT IN (
            'PENDING',
            'PROCESSING',
            'SHIPPED',
            'DELIVERED',
            'CANCELLED',
            'RETURNED'
        )
    ) = 0
    FROM `clean-abacus-513514-c8.retailpulse_silver.orders_incremental_staging`
) AS 'Unexpected order status found. MERGE stopped.';

-- STEP 4: Validate customer references.
ASSERT (
    SELECT COUNT(*) = 0
    FROM `clean-abacus-513514-c8.retailpulse_silver.orders_incremental_staging` S
    LEFT JOIN `clean-abacus-513514-c8.retailpulse_gold.dim_customers` C
        ON S.customer_id = C.customer_id
    WHERE C.customer_id IS NULL
) AS 'Staging contains invalid customer references. MERGE stopped.';

-- STEP 5: Validate Gold target key uniqueness.
ASSERT (
    SELECT COUNT(*) = COUNT(DISTINCT order_id)
    FROM `clean-abacus-513514-c8.retailpulse_gold.fact_orders`
) AS 'Duplicate order_id values found in Gold. MERGE stopped.';

-- STEP 6: Merge new and changed orders into Gold.
MERGE
    `clean-abacus-513514-c8.retailpulse_gold.fact_orders` AS T
USING
    `clean-abacus-513514-c8.retailpulse_silver.orders_incremental_staging` AS S
ON
    T.order_id = S.order_id

WHEN MATCHED AND (
    T.order_updated_at IS NULL
    OR S.updated_at > T.order_updated_at
)
THEN UPDATE SET
    T.customer_id = S.customer_id,
    T.order_date = S.order_date,
    T.status = S.status,
    T.total_amount = S.total_amount,
    T.order_created_at = S.created_at,
    T.order_updated_at = S.updated_at

WHEN NOT MATCHED THEN
    INSERT (
        order_id,
        customer_id,
        order_date,
        status,
        total_amount,
        order_created_at,
        order_updated_at
    )
    VALUES (
        S.order_id,
        S.customer_id,
        S.order_date,
        S.status,
        S.total_amount,
        S.created_at,
        S.updated_at
    );
