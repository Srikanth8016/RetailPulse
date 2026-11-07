
-- Create the pipeline watermark control table.
CREATE TABLE IF NOT EXISTS
    `clean-abacus-513514-c8.retailpulse_silver.pipeline_watermarks`
(
    pipeline_name STRING,
    watermark_timestamp TIMESTAMP,
    updated_at TIMESTAMP
);

-- Initialize the orders pipeline watermark.
-- MERGE prevents accidentally resetting an existing watermark.
MERGE
    `clean-abacus-513514-c8.retailpulse_silver.pipeline_watermarks` AS target

USING (
    SELECT
        'orders' AS pipeline_name,
        TIMESTAMP('2026-11-02 12:00:00+00')
            AS watermark_timestamp
) AS source

ON target.pipeline_name = source.pipeline_name

WHEN NOT MATCHED THEN
    INSERT (
        pipeline_name,
        watermark_timestamp,
        updated_at
    )
    VALUES (
        source.pipeline_name,
        source.watermark_timestamp,
        CURRENT_TIMESTAMP()
    );
