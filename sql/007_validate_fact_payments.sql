SELECT
    COUNT(*) AS total_payments,

    COUNT(DISTINCT fp.payment_id) AS distinct_payment_ids,

    COUNT(*) - COUNT(DISTINCT fp.payment_id)
        AS duplicate_payment_ids,

    COUNTIF(
        fp.payment_id IS NULL
        OR fp.order_id IS NULL
    ) AS null_identifiers,

    COUNTIF(
        fp.payment_amount <= 0
        OR fp.payment_amount IS NULL
    ) AS invalid_payment_amounts,

    COUNTIF(fo.order_id IS NULL)
        AS orphan_orders,

    COUNTIF(
        fp.payment_status NOT IN (
            'COMPLETED',
            'FAILED',
            'PENDING',
            'REFUNDED'
        )
        OR fp.payment_status IS NULL
    ) AS invalid_payment_statuses,

    COUNTIF(
        fp.payment_method NOT IN (
            'CREDIT_CARD',
            'DEBIT_CARD',
            'PAYPAL',
            'APPLE_PAY',
            'GOOGLE_PAY',
            'BANK_TRANSFER'
        )
        OR fp.payment_method IS NULL
    ) AS invalid_payment_methods,

    COUNTIF(fp.payment_date IS NULL)
        AS null_payment_dates

FROM
    `clean-abacus-513514-c8.retailpulse_gold.fact_payments` fp

LEFT JOIN
    `clean-abacus-513514-c8.retailpulse_gold.fact_orders` fo
    ON fp.order_id = fo.order_id;
