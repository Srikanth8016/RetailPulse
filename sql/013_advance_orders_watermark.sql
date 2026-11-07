BEGIN TRANSACTION;

ASSERT (
    SELECT COUNT(*) = 1
    FROM `clean-abacus-513514-c8.retailpulse_silver.pipeline_watermarks`
    WHERE pipeline_name = 'orders'
      AND watermark_timestamp =
          TIMESTAMP '2026-11-03 12:46:27.887899+00'
) AS 'Watermark missing, duplicated, or unexpectedly changed.';

ASSERT (
    SELECT TIMESTAMP '2026-11-03 12:56:51.930293+00'
           > watermark_timestamp
    FROM `clean-abacus-513514-c8.retailpulse_silver.pipeline_watermarks`
    WHERE pipeline_name = 'orders'
) AS 'New watermark must be later than current watermark.';

UPDATE
    `clean-abacus-513514-c8.retailpulse_silver.pipeline_watermarks`
SET
    watermark_timestamp =
        TIMESTAMP '2026-11-03 12:56:51.930293+00',
    updated_at = CURRENT_TIMESTAMP()
WHERE
    pipeline_name = 'orders'
    AND watermark_timestamp =
        TIMESTAMP '2026-11-03 12:46:27.887899+00';

ASSERT @@row_count = 1
    AS 'Expected exactly one watermark row to be updated.';

COMMIT TRANSACTION;