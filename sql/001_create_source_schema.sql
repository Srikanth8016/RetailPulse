-- ==============================================================================
-- RetailPulse Data Engineering Platform - Source OLTP Database Schema
-- File: sql/001_create_source_schema.sql
-- Description: DDL script for transactional e-commerce entities (PostgreSQL 16)
-- Target Database: retailpulse
-- ==============================================================================

-- ------------------------------------------------------------------------------
-- 1. Table: customers
-- Description: Stores registered e-commerce customers, identity details, and location.
-- ETL Role: Dimension source (dim_customers) and user segmentation.
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS customers (
    customer_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    email VARCHAR(255) NOT NULL,
    city VARCHAR(100),
    country VARCHAR(100) NOT NULL DEFAULT 'United States',
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- Enforce uniqueness of customer email addresses
    CONSTRAINT uq_customers_email UNIQUE (email),

    -- Format validation check on email
    CONSTRAINT chk_customers_email_format CHECK (email ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$')
);

-- Comments on customers table and columns
COMMENT ON TABLE customers IS 'Primary entity storing e-commerce customer accounts.';
COMMENT ON COLUMN customers.customer_id IS 'Surrogate identity primary key for the customer.';
COMMENT ON COLUMN customers.updated_at IS 'Watermark timestamp used for downstream incremental CDC / batch extraction.';


-- ------------------------------------------------------------------------------
-- 2. Table: products
-- Description: Product catalog detailing categories, unit prices, and inventory levels.
-- ETL Role: Dimension source (dim_products) and inventory velocity tracking.
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS products (
    product_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    category VARCHAR(100) NOT NULL,
    price NUMERIC(12, 2) NOT NULL,
    stock_quantity INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- Business logic constraints: positive pricing and non-negative stock levels
    CONSTRAINT chk_products_price_positive CHECK (price > 0),
    CONSTRAINT chk_products_stock_non_negative CHECK (stock_quantity >= 0)
);

-- Comments on products table and columns
COMMENT ON TABLE products IS 'E-commerce product catalog with inventory tracking.';
COMMENT ON COLUMN products.price IS 'Unit retail price represented as fixed-point NUMERIC(12,2) to eliminate floating-point rounding errors.';
COMMENT ON COLUMN products.updated_at IS 'Watermark timestamp for catalog changes in the ETL pipeline.';


-- ------------------------------------------------------------------------------
-- 3. Table: orders
-- Description: Top-level transactional sales orders placed by customers.
-- ETL Role: Core fact source (fct_orders) and lifecycle funnel analysis.
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS orders (
    order_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id BIGINT NOT NULL,
    order_date TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(50) NOT NULL DEFAULT 'PENDING',
    total_amount NUMERIC(12, 2) NOT NULL DEFAULT 0.00,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- Foreign Key: Preserve order history if customer records are modified (RESTRICT)
    CONSTRAINT fk_orders_customer_id 
        FOREIGN KEY (customer_id) 
        REFERENCES customers (customer_id) 
        ON DELETE RESTRICT 
        ON UPDATE CASCADE,

    -- Ensure total monetary amount is non-negative
    CONSTRAINT chk_orders_total_amount_non_negative CHECK (total_amount >= 0),

    -- Validate allowable order lifecycle statuses
    CONSTRAINT chk_orders_status_valid CHECK (
        status IN ('PENDING', 'PROCESSING', 'SHIPPED', 'DELIVERED', 'CANCELLED', 'RETURNED')
    )
);

-- Comments on orders table and columns
COMMENT ON TABLE orders IS 'E-commerce transactional orders header table.';
COMMENT ON COLUMN orders.status IS 'Lifecycle status (PENDING, PROCESSING, SHIPPED, DELIVERED, CANCELLED, RETURNED).';
COMMENT ON COLUMN orders.updated_at IS 'High-frequency watermark column used for incremental ETL extraction into GCS.';


-- ------------------------------------------------------------------------------
-- 4. Table: order_items
-- Description: Line-item granularity for each order specifying products and quantities.
-- ETL Role: Grain fact source (fct_order_items) and basket market analysis.
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS order_items (
    order_item_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_id BIGINT NOT NULL,
    product_id BIGINT NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price NUMERIC(12, 2) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- Foreign Key: Order line items belong strictly to the order lifecycle (CASCADE on order deletion)
    CONSTRAINT fk_order_items_order_id 
        FOREIGN KEY (order_id) 
        REFERENCES orders (order_id) 
        ON DELETE CASCADE 
        ON UPDATE CASCADE,

    -- Foreign Key: Product cannot be deleted if referenced in historical sales items (RESTRICT)
    CONSTRAINT fk_order_items_product_id 
        FOREIGN KEY (product_id) 
        REFERENCES products (product_id) 
        ON DELETE RESTRICT 
        ON UPDATE CASCADE,

    -- Prevent duplicate entries of the same product within a single order
    CONSTRAINT uq_order_items_order_product UNIQUE (order_id, product_id),

    -- Quantity must be strictly positive; unit price non-negative (supports promotional items)
    CONSTRAINT chk_order_items_quantity_positive CHECK (quantity > 0),
    CONSTRAINT chk_order_items_unit_price_non_negative CHECK (unit_price >= 0)
);

-- Comments on order_items table and columns
COMMENT ON TABLE order_items IS 'Line-item breakdown capturing granular product sales per order.';
COMMENT ON COLUMN order_items.unit_price IS 'Historical point-in-time unit price locked at order placement.';


-- ------------------------------------------------------------------------------
-- 5. Table: payments
-- Description: Financial transaction attempts, payment methods, and settlement status.
-- ETL Role: Fact source (fct_payments) and financial reconciliation.
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS payments (
    payment_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_id BIGINT NOT NULL,
    payment_method VARCHAR(50) NOT NULL,
    amount NUMERIC(12, 2) NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'PENDING',
    transaction_reference VARCHAR(255) NOT NULL,
    payment_date TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- Foreign Key: Payment records are critical audit trails (RESTRICT deletion of paid orders)
    CONSTRAINT fk_payments_order_id 
        FOREIGN KEY (order_id) 
        REFERENCES orders (order_id) 
        ON DELETE RESTRICT 
        ON UPDATE CASCADE,

    -- External gateway reference must be unique across all transactions
    CONSTRAINT uq_payments_transaction_reference UNIQUE (transaction_reference),

    -- Payment amount must be strictly positive
    CONSTRAINT chk_payments_amount_positive CHECK (amount > 0),

    -- Allowable payment processing methods
    CONSTRAINT chk_payments_method_valid CHECK (
        payment_method IN ('CREDIT_CARD', 'DEBIT_CARD', 'PAYPAL', 'BANK_TRANSFER', 'APPLE_PAY', 'GOOGLE_PAY', 'GIFT_CARD')
    ),

    -- Allowable payment statuses
    CONSTRAINT chk_payments_status_valid CHECK (
        status IN ('PENDING', 'COMPLETED', 'FAILED', 'REFUNDED')
    )
);

-- Comments on payments table and columns
COMMENT ON TABLE payments IS 'Payment transactions and settlement logs for orders.';
COMMENT ON COLUMN payments.transaction_reference IS 'Unique reference ID generated by payment gateways (Stripe, PayPal, etc.).';


-- ==============================================================================
-- INDEX STRATEGY
-- PostgreSQL automatically creates unique indexes for PRIMARY KEY and UNIQUE constraints.
-- Below are critical secondary indexes on Foreign Keys and High-Cardinality / Filter columns.
-- ==============================================================================

-- ------------------------------------------------------------------------------
-- Foreign Key Indexes (Prevents full-table scans during joins and cascades)
-- ------------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_orders_customer_id 
    ON orders (customer_id);

CREATE INDEX IF NOT EXISTS idx_order_items_order_id 
    ON order_items (order_id);

CREATE INDEX IF NOT EXISTS idx_order_items_product_id 
    ON order_items (product_id);

CREATE INDEX IF NOT EXISTS idx_payments_order_id 
    ON payments (order_id);

-- ------------------------------------------------------------------------------
-- Analytical, Filtering & Watermark Indexes (Optimizes CDC & query performance)
-- ------------------------------------------------------------------------------
-- Incremental ETL watermarks for CDC / delta extraction into GCS
CREATE INDEX IF NOT EXISTS idx_orders_updated_at 
    ON orders (updated_at);

CREATE INDEX IF NOT EXISTS idx_customers_updated_at 
    ON customers (updated_at);

CREATE INDEX IF NOT EXISTS idx_products_updated_at 
    ON products (updated_at);

-- Business analytics filtering on order status and transaction dates
CREATE INDEX IF NOT EXISTS idx_orders_order_date 
    ON orders (order_date);

CREATE INDEX IF NOT EXISTS idx_payments_payment_date 
    ON payments (payment_date);

CREATE INDEX IF NOT EXISTS idx_products_category 
    ON products (category);
