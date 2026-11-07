
BEGIN;

-- Check the original timestamp.
SELECT
    order_id,
    status,
    updated_at AS before_update
FROM public.orders
WHERE order_id = 1;

-- Update the row without changing its business status.
-- The trigger should automatically change updated_at.
UPDATE public.orders
SET status = status
WHERE order_id = 1
RETURNING
    order_id,
    status,
    updated_at AS after_update;

-- Read the timestamp again inside the transaction.
SELECT
    order_id,
    updated_at AS verified_updated_at
FROM public.orders
WHERE order_id = 1;

-- Undo the test. Original data remains unchanged.
ROLLBACK;
