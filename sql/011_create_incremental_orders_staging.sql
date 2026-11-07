
CREATE OR REPLACE TABLE
  `clean-abacus-513514-c8.retailpulse_silver.orders_incremental_staging`
(
  order_id INT64 NOT NULL,
  customer_id INT64 NOT NULL,
  order_date TIMESTAMP NOT NULL,
  status STRING NOT NULL,
  total_amount NUMERIC NOT NULL,
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL
)
OPTIONS (
  description = 'Staging table for incremental PostgreSQL orders ingestion. Loaded from GCS before MERGE into Gold fact_orders.'
);
