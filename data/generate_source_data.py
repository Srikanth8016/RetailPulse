#!/usr/bin/env python3
"""
RetailPulse Data Engineering Platform - Source Data Generator
File: data/generate_source_data.py

Description:
    Generates realistic, deterministic e-commerce source data for PostgreSQL
    matching the schema defined in sql/001_create_source_schema.sql.

Record Targets:
    - Customers: Exactly 1,000 records
    - Products: Exactly 200 records
    - Orders: Exactly 10,000 records
    - Order Items: Between 20,000 and 30,000 records
    - Payments: Between 10,000 and 12,000 records

Key Architecture Principles:
    - Strict adherence to PostgreSQL 16 schema & foreign keys.
    - Fully deterministic output via fixed pseudorandom seeds and SHA-256 hash tokens.
    - Bulletproof ID mapping: Zero reliance on undocumented INSERT ... RETURNING row ordering.
      Uses unique SKU tracking for products and microsecond sequence indexing for orders.
    - Duplicate Prevention: Strict in-memory uniqueness assertions on (order_id, product_id)
      before executing database batch inserts.
    - Chronological consistency: All event timestamps across all 5 tables are strictly <= ANCHOR_NOW.
    - Business Integrity: Full refunds enforced - every REFUNDED payment must match an earlier
      COMPLETED payment for the same order with the exact same amount.
    - Top-Level Atomic Transaction Safety: Preflight read queries are explicitly committed so
      with conn.transaction() begins a genuine top-level transaction (not a nested savepoint).
      Truncation, all bulk inserts, and pre-commit validations commit or roll back together atomically.
    - Sequence Reset Guarantee: --truncate always executes TRUNCATE ... RESTART IDENTITY CASCADE,
      even when table counts are 0, ensuring sequence counters reset after rollbacks.
    - Comprehensive pre-commit validation: Counts, foreign keys, LEFT JOIN totals check with
      IS DISTINCT FROM, 5-table timestamp bounds, and refund pairings.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
import hashlib
import logging
import os
import random
import re
import sys
from typing import Any, Dict, List, Sequence, Set, Tuple

from dotenv import load_dotenv
from faker import Faker
import psycopg
from psycopg import sql

# ------------------------------------------------------------------------------
# Logging Setup
# ------------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("RetailPulse.DataGenerator")

# ------------------------------------------------------------------------------
# Configuration & Constants
# ------------------------------------------------------------------------------
DEFAULT_SEED = 42
NUM_CUSTOMERS = 1_000
NUM_PRODUCTS = 200
NUM_ORDERS = 10_000

# Base time anchor for historical generation (spanning previous 6 months)
# Anchored to early November 2026 to align with platform operational timeline
ANCHOR_NOW = datetime(2026, 11, 2, 12, 0, 0, tzinfo=timezone.utc)
HISTORICAL_WINDOW_DAYS = 180

# Catalog blueprint for generating 200 distinct, realistic SKUs
PRODUCT_CATALOG_TEMPLATES: Dict[str, List[Tuple[str, Decimal, Decimal]]] = {
    "Electronics": [
        ("Noise-Cancelling Wireless Headphones", Decimal("199.99"), Decimal("379.99")),
        ("4K Ultra HD Gaming Monitor 27-inch", Decimal("289.99"), Decimal("529.99")),
        ("Mechanical RGB Gaming Keyboard", Decimal("79.99"), Decimal("159.99")),
        ("Ergonomic Vertical Wireless Mouse", Decimal("29.99"), Decimal("79.99")),
        ("Smart Fitness Watch Series 6", Decimal("149.99"), Decimal("319.99")),
        ("Portable Waterproof Bluetooth Speaker", Decimal("49.99"), Decimal("129.99")),
        ("USB-C Multi-Port Thunderbolt Hub", Decimal("34.99"), Decimal("69.99")),
        ("True Wireless Earbuds with Active ANC", Decimal("69.99"), Decimal("189.99")),
        ("Dual-Band Wi-Fi 6 Mesh System", Decimal("99.99"), Decimal("249.99")),
        ("MagSafe Ultra Power Bank 10000mAh", Decimal("39.99"), Decimal("79.99")),
    ],
    "Home & Kitchen": [
        ("Stainless Steel Espresso Machine", Decimal("149.99"), Decimal("449.99")),
        ("Cast Iron Dutch Oven 6-Quart", Decimal("49.99"), Decimal("129.99")),
        ("Air Fryer Max Digital 5.8-Quart", Decimal("69.99"), Decimal("169.99")),
        ("Cold Brew Pitcher & Filter 1.5L", Decimal("19.99"), Decimal("39.99")),
        ("Japanese Damascus Chef Knife 8-inch", Decimal("49.99"), Decimal("119.99")),
        ("Non-Stick Ceramic Cookware Set 10-pc", Decimal("89.99"), Decimal("279.99")),
        ("Ultrasonic Cool Mist Essential Oil Diffuser", Decimal("21.99"), Decimal("45.99")),
        ("Smart Robot Vacuum with Laser Mapping", Decimal("179.99"), Decimal("429.99")),
        ("Electric Gooseneck Variable-Temp Kettle", Decimal("42.99"), Decimal("89.99")),
        ("Double-Wall Insulated Tumbler 32oz", Decimal("19.99"), Decimal("36.99")),
    ],
    "Apparel & Footwear": [
        ("Organic Cotton Heavyweight Crewneck T-Shirt", Decimal("22.99"), Decimal("38.99")),
        ("Slim-Fit Performance Stretch Jeans", Decimal("54.99"), Decimal("98.99")),
        ("Waterproof All-Weather Windbreaker", Decimal("69.99"), Decimal("139.99")),
        ("Lightweight Breathable Running Shoes", Decimal("74.99"), Decimal("159.99")),
        ("Merino Wool Outdoor Cushion Socks 3-Pack", Decimal("21.99"), Decimal("38.99")),
        ("Full-Zip Cotton Athleisure Hoodie", Decimal("44.99"), Decimal("79.99")),
        ("Polarized Classic Wayfarer Sunglasses", Decimal("29.99"), Decimal("64.99")),
        ("Quick-Dry Athletic Training Shorts", Decimal("24.99"), Decimal("46.99")),
        ("Full-Grain Italian Leather Belt", Decimal("32.99"), Decimal("58.99")),
        ("Waterproof Trail Hiking Boots", Decimal("89.99"), Decimal("179.99")),
    ],
    "Beauty & Personal Care": [
        ("Hyaluronic Acid Daily Hydrating Serum", Decimal("18.99"), Decimal("46.99")),
        ("Rechargeable Sonic Whitening Toothbrush", Decimal("39.99"), Decimal("94.99")),
        ("Cold-Pressed Moroccan Argan Hair Oil", Decimal("15.99"), Decimal("32.99")),
        ("Broad Spectrum Mineral Sunscreen SPF 50", Decimal("17.99"), Decimal("34.99")),
        ("Cedarwood & Sandalwood Beard Care Kit", Decimal("24.99"), Decimal("52.99")),
        ("Activated Charcoal Detoxifying Facial Mask", Decimal("13.99"), Decimal("26.99")),
        ("Niacinamide Pore Refining Exfoliating Toner", Decimal("14.99"), Decimal("29.99")),
        ("Intense Ceramide Moisture Repair Cream", Decimal("22.99"), Decimal("44.99")),
    ],
    "Sports & Outdoors": [
        ("High-Density Non-Slip Yoga Mat 1/2-Inch", Decimal("24.99"), Decimal("54.99")),
        ("Adjustable Quick-Dial Dumbbell Set (Pair)", Decimal("159.99"), Decimal("369.99")),
        ("Ultralight 2-Person Backpacking Dome Tent", Decimal("99.99"), Decimal("219.99")),
        ("Vacuum-Insulated Sport Water Bottle 40oz", Decimal("24.99"), Decimal("48.99")),
        ("Fabric Resistance Glute Bands 3-Pack", Decimal("14.99"), Decimal("28.99")),
        ("Collapsible Carbon Fiber Trekking Poles", Decimal("34.99"), Decimal("69.99")),
        ("Waterproof Bike Commuter Saddle Bag", Decimal("39.99"), Decimal("79.99")),
        ("LED Rechargeable Trail Headlamp 1200LM", Decimal("22.99"), Decimal("42.99")),
    ],
    "Books & Media": [
        ("Designing Data-Intensive Applications", Decimal("39.99"), Decimal("49.99")),
        ("Clean Architecture: Software Craftsman Guide", Decimal("34.99"), Decimal("44.99")),
        ("Python for Data Analysis (3rd Edition)", Decimal("36.99"), Decimal("48.99")),
        ("The Data Warehouse Toolkit (3rd Edition)", Decimal("44.99"), Decimal("58.99")),
        ("Database Internals: Distributed Systems", Decimal("42.99"), Decimal("54.99")),
        ("System Design Interview: Insider Roadmap", Decimal("35.99"), Decimal("47.99")),
    ],
}


class DataValidationError(Exception):
    """Raised when generated source data fails integrity or boundary validations."""
    pass


# ------------------------------------------------------------------------------
# Deterministic Payment Reference Generator
# ------------------------------------------------------------------------------
def make_deterministic_payment_ref(seed: int, payment_seq: int, tag: str = "") -> str:
    """
    Constructs a deterministic, unique transaction reference string without random UUIDs.
    Format: TXN-<seed>-<seq:08d>[-<tag>]-<hash:6>
    """
    raw_key = f"{seed}:{payment_seq}:{tag}"
    checksum = hashlib.sha256(raw_key.encode("ascii")).hexdigest()[:6].upper()
    if tag:
        return f"TXN-{seed:04d}-{payment_seq:08d}-{tag}-{checksum}"
    return f"TXN-{seed:04d}-{payment_seq:08d}-{checksum}"


# ------------------------------------------------------------------------------
# Database Connectivity Helper
# ------------------------------------------------------------------------------
def get_db_connection() -> psycopg.Connection:
    """
    Loads environment variables from .env and establishes a connection to PostgreSQL.
    Credentials and secrets are strictly excluded from logging.
    autocommit is set to False for explicit transaction management.
    """
    load_dotenv()

    host = os.getenv("POSTGRES_HOST", "localhost")
    port = int(os.getenv("POSTGRES_PORT", "5433"))
    database = os.getenv("POSTGRES_DB", "retailpulse")
    user = os.getenv("POSTGRES_USER", "retailpulse_user")
    password = os.getenv("POSTGRES_PASSWORD", "")

    if not password:
        logger.warning(
            "POSTGRES_PASSWORD is empty or unset in environment. Attempting connection without password."
        )

    # Note: Password is never logged
    logger.info(
        "Connecting to PostgreSQL at %s:%d/%s as user '%s'...",
        host,
        port,
        database,
        user,
    )
    try:
        conn = psycopg.connect(
            host=host,
            port=port,
            dbname=database,
            user=user,
            password=password,
            autocommit=False,
        )
        return conn
    except psycopg.OperationalError as err:
        logger.error("Failed to connect to PostgreSQL: %s", err)
        raise


# ------------------------------------------------------------------------------
# Data Sanitation & Idempotency Guards
# ------------------------------------------------------------------------------
def check_existing_data(conn: psycopg.Connection) -> Dict[str, int]:
    """
    Inspects existing row counts for all five source tables.
    """
    tables = ["customers", "products", "orders", "order_items", "payments"]
    counts = {}
    with conn.cursor() as cur:
        for tbl in tables:
            cur.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(tbl)))
            counts[tbl] = cur.fetchone()[0]
    return counts


def truncate_tables(cur: psycopg.Cursor) -> None:
    """
    Safely truncates all five source tables and resets identity sequences.
    Must be executed within an active transaction so rollback can restore state on error.
    Always restarts sequence counters (RESTART IDENTITY CASCADE).
    """
    logger.info("Executing TRUNCATE on source tables (with sequence restart) inside transaction...")
    cur.execute(
        """
        TRUNCATE TABLE customers, products, orders, order_items, payments 
        RESTART IDENTITY CASCADE;
        """
    )


# ------------------------------------------------------------------------------
# High-Performance Batch Insertion Helper
# ------------------------------------------------------------------------------
def bulk_insert_with_returning(
    cur: psycopg.Cursor,
    table_name: str,
    columns: Sequence[str],
    rows: Sequence[Sequence[Any]],
    returning_cols: Sequence[str],
    max_batch_params: int = 40_000,
) -> List[Tuple[Any, ...]]:
    """
    Executes chunked parameterized multi-row INSERT statements with RETURNING clause.
    Returns requested returning columns for each inserted row.
    """
    if not rows:
        return []

    num_cols = len(columns)
    batch_size = max(1, max_batch_params // num_cols)
    returned_tuples: List[Tuple[Any, ...]] = []

    for i in range(0, len(rows), batch_size):
        chunk = rows[i : i + batch_size]
        query = sql.SQL(
            "INSERT INTO {table} ({fields}) VALUES {values} RETURNING {returning};"
        ).format(
            table=sql.Identifier(table_name),
            fields=sql.SQL(", ").join(map(sql.Identifier, columns)),
            values=sql.SQL(", ").join(
                sql.SQL("({})").format(
                    sql.SQL(", ").join(sql.Placeholder() for _ in range(num_cols))
                )
                for _ in range(len(chunk))
            ),
            returning=sql.SQL(", ").join(map(sql.Identifier, returning_cols)),
        )

        flat_params = [val for row in chunk for val in row]
        cur.execute(query, flat_params)
        returned_tuples.extend(cur.fetchall())

    return returned_tuples


def bulk_insert(
    cur: psycopg.Cursor,
    table_name: str,
    columns: Sequence[str],
    rows: Sequence[Sequence[Any]],
    max_batch_params: int = 40_000,
) -> None:
    """
    Executes chunked parameterized multi-row INSERT statements without RETURNING.
    """
    if not rows:
        return

    num_cols = len(columns)
    batch_size = max(1, max_batch_params // num_cols)

    for i in range(0, len(rows), batch_size):
        chunk = rows[i : i + batch_size]
        query = sql.SQL(
            "INSERT INTO {table} ({fields}) VALUES {values};"
        ).format(
            table=sql.Identifier(table_name),
            fields=sql.SQL(", ").join(map(sql.Identifier, columns)),
            values=sql.SQL(", ").join(
                sql.SQL("({})").format(
                    sql.SQL(", ").join(sql.Placeholder() for _ in range(num_cols))
                )
                for _ in range(len(chunk))
            ),
        )

        flat_params = [val for row in chunk for val in row]
        cur.execute(query, flat_params)


# ------------------------------------------------------------------------------
# Entity Generation Functions
# ------------------------------------------------------------------------------
def generate_customers(
    fake: Faker, count: int
) -> List[Tuple[str, str, str, str, str, datetime, datetime]]:
    """
    Generates deterministic customer accounts with unique, regex-compliant email addresses.
    Returns list of row tuples.
    """
    logger.info("Generating %d synthetic customers...", count)
    rows = []

    # Distribution: registration occurred between 210 and 15 days ago
    start_date = ANCHOR_NOW - timedelta(days=210)
    window_seconds = int((ANCHOR_NOW - timedelta(days=15) - start_date).total_seconds())

    for idx in range(1, count + 1):
        first_name = fake.first_name()
        last_name = fake.last_name()

        # Sanitize names to guarantee compliance with email format regex
        clean_first = re.sub(r"[^a-zA-Z0-9]", "", first_name).lower() or "user"
        clean_last = re.sub(r"[^a-zA-Z0-9]", "", last_name).lower() or "customer"
        email = f"{clean_first}.{clean_last}.{idx}@retailpulse.example.com"

        city = fake.city()
        country = "United States"

        created_sec = random.randint(0, window_seconds)
        created_at = start_date + timedelta(seconds=created_sec)
        
        # updated_at is at least created_at and never exceeds ANCHOR_NOW
        updated_at = min(created_at + timedelta(hours=random.randint(0, 72)), ANCHOR_NOW)

        rows.append((first_name, last_name, email, city, country, created_at, updated_at))

    return rows


def generate_products(
    count: int
) -> List[Tuple[str, str, Decimal, int, datetime, datetime]]:
    """
    Generates exactly 200 distinct, realistic products across 6 core categories.
    Appends an explicit SKU identifier to guarantee 100% unique product names.
    """
    logger.info("Generating %d catalog products...", count)
    rows = []

    categories = list(PRODUCT_CATALOG_TEMPLATES.keys())
    product_idx = 1
    catalog_start = ANCHOR_NOW - timedelta(days=240)

    while product_idx <= count:
        cat = categories[(product_idx - 1) % len(categories)]
        template_list = PRODUCT_CATALOG_TEMPLATES[cat]
        base_name, min_p, max_p = template_list[(product_idx - 1) % len(template_list)]

        variant_edition = ((product_idx - 1) // len(template_list)) + 1
        modifiers = ["Standard", "Pro Edition", "V2", "Elite", "Compact", "Plus"]
        modifier = modifiers[(variant_edition - 1) % len(modifiers)]

        # Explicit SKU tag guarantees 100% uniqueness across product names
        product_name = f"{base_name} - {modifier} [SKU-{product_idx:03d}]"

        price_float = random.uniform(float(min_p), float(max_p))
        price = Decimal(str(price_float)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

        stock_quantity = random.choices(
            [0, random.randint(5, 50), random.randint(51, 500)],
            weights=[0.05, 0.40, 0.55],
        )[0]

        created_at = catalog_start + timedelta(days=random.randint(0, 30))
        updated_at = min(created_at + timedelta(days=random.randint(1, 180)), ANCHOR_NOW)

        rows.append((product_name, cat, price, stock_quantity, created_at, updated_at))
        product_idx += 1

    return rows


def generate_orders_and_items(
    customer_ids: List[int],
    customer_reg_map: Dict[int, datetime],
    product_ids: List[int],
    product_prices: Dict[int, Decimal],
    count: int,
) -> Tuple[
    List[Tuple[int, datetime, str, Decimal, datetime, datetime]],  # order rows
    List[List[Tuple[int, int, Decimal, datetime]]],                # items grouped per order
    List[str],                                                     # order status per order
    List[datetime],                                                # order dates
]:
    """
    Generates orders spanning the past 6 months and corresponding line items.
    Enforces that order.total_amount matches sum(quantity * unit_price) exactly.

    ARCHITECTURAL NOTE ON CORRELATION STRATEGY:
        This microsecond indexing technique (order_date.microsecond = order_seq * 100)
        is a deterministic synthetic-data correlation mechanism designed specifically
        to map database-generated identity primary keys back to in-memory child record groups
        during multi-row bulk INSERTs without mutating or expanding the target database schema
        (e.g., avoiding transient correlation columns or custom staging tables).
        In a production transactional system, parent-child record correlation should be
        established using natural domain identifiers, pre-generated UUID primary keys,
        or distributed sequence generators, rather than overloading timestamp precision.
    """
    logger.info("Generating %d orders and associated line items...", count)

    # Ensure product IDs passed in are strictly unique
    unique_product_ids = sorted(list(set(product_ids)))
    if len(unique_product_ids) != len(product_ids):
        raise ValueError(
            f"Input product_ids list contains duplicates: {len(product_ids)} items, {len(unique_product_ids)} unique"
        )

    order_rows = []
    order_items_by_order: List[List[Tuple[int, int, Decimal, datetime]]] = []
    order_statuses: List[str] = []
    order_dates: List[datetime] = []

    status_choices_historical = ["DELIVERED", "RETURNED", "CANCELLED"]
    status_weights_historical = [0.85, 0.08, 0.07]

    status_choices_recent = ["PENDING", "PROCESSING", "SHIPPED", "DELIVERED", "CANCELLED"]
    status_weights_recent = [0.15, 0.25, 0.35, 0.20, 0.05]

    for order_seq in range(count):
        cust_idx = random.choices(
            range(len(customer_ids)),
            weights=[(1.0 + (i % 5)) for i in range(len(customer_ids))],
        )[0]
        cust_id = customer_ids[cust_idx]
        cust_reg_date = customer_reg_map[cust_id]

        time_since_reg = int((ANCHOR_NOW - cust_reg_date).total_seconds())
        if time_since_reg < 3600:
            base_order_date = cust_reg_date + timedelta(minutes=15)
        else:
            base_order_date = cust_reg_date + timedelta(
                seconds=random.randint(1800, max(3600, time_since_reg))
            )

        # Bound base date at least 1 hour before ANCHOR_NOW
        if base_order_date > (ANCHOR_NOW - timedelta(hours=1)):
            base_order_date = ANCHOR_NOW - timedelta(hours=1, minutes=random.randint(1, 55))

        # Microsecond sequence indexing:
        # order_seq (0 to 9,999) * 100 maps to microseconds 0 to 999,900.
        # This guarantees:
        # 1. Every order has a globally unique microsecond timestamp.
        # 2. order_seq can be recovered directly: seq = returned_order_date.microsecond // 100.
        order_date = base_order_date.replace(microsecond=order_seq * 100)

        # Context-aware lifecycle status based on order age
        age_days = (ANCHOR_NOW - order_date).total_seconds() / 86400.0
        if age_days > 14:
            status = random.choices(status_choices_historical, weights=status_weights_historical)[0]
        else:
            status = random.choices(status_choices_recent, weights=status_weights_recent)[0]

        # 1 to 5 distinct items per order (average ~2.27 items -> ~22,700 items total)
        num_items = random.choices([1, 2, 3, 4, 5], weights=[0.25, 0.45, 0.20, 0.08, 0.02])[0]
        chosen_product_ids = random.sample(unique_product_ids, num_items)

        # Defensive assertion: No duplicate product IDs within this order
        if len(chosen_product_ids) != len(set(chosen_product_ids)):
            raise DataValidationError(
                f"Duplicate products chosen for order_seq={order_seq}: {chosen_product_ids}"
            )

        current_order_items = []
        running_total = Decimal("0.00")

        for prod_id in chosen_product_ids:
            quantity = random.choices([1, 2, 3, 4], weights=[0.70, 0.20, 0.07, 0.03])[0]
            unit_price = product_prices[prod_id]
            line_total = (Decimal(quantity) * unit_price).quantize(
                Decimal("0.01"), rounding=ROUND_HALF_UP
            )
            running_total += line_total
            item_created_at = order_date

            current_order_items.append((prod_id, quantity, unit_price, item_created_at))

        created_at = order_date
        updated_at = min(order_date + timedelta(hours=random.randint(1, 48)), ANCHOR_NOW)

        order_rows.append(
            (cust_id, order_date, status, running_total, created_at, updated_at)
        )
        order_items_by_order.append(current_order_items)
        order_statuses.append(status)
        order_dates.append(order_date)

    return order_rows, order_items_by_order, order_statuses, order_dates


def generate_payments(
    order_ids: List[int],
    order_totals: List[Decimal],
    order_statuses: List[str],
    order_dates: List[datetime],
    seed: int,
) -> List[Tuple[int, str, Decimal, str, str, datetime, datetime, datetime]]:
    """
    Generates realistic payment attempts tied to orders.
    Produces between 10,000 and 12,000 payment records by accounting for retries/refunds.

    Business Rule & Assumption - Full Refunds:
        All refunded payments in the RetailPulse source schema represent 100% full
        reimbursements of the original transaction amount (refund_amount == total_amount).
        Partial refunds, restocking fees, and shipping deductions are intentionally not
        modeled in this initial OLTP generation phase.
        
        Refund Eligibility:
        - RETURNED orders: Delivered products returned by the customer; initial COMPLETED
          payment is followed by a REFUNDED payment.
        - CANCELLED orders (post-charge): Customer paid, then cancelled before shipment;
          initial COMPLETED payment is followed by a REFUNDED payment.
        - CANCELLED orders (pre-charge): Payment attempt FAILED at checkout; no refund record.
    """
    logger.info("Generating payment transaction attempts...")
    payment_methods = [
        "CREDIT_CARD",
        "DEBIT_CARD",
        "PAYPAL",
        "APPLE_PAY",
        "GOOGLE_PAY",
        "BANK_TRANSFER",
    ]
    method_weights = [0.45, 0.25, 0.15, 0.08, 0.05, 0.02]

    payments = []
    payment_seq = 1

    for o_id, total, status, o_date in zip(
        order_ids, order_totals, order_statuses, order_dates
    ):
        p_method = random.choices(payment_methods, weights=method_weights)[0]
        # Primary payment timestamp bounded by ANCHOR_NOW
        p_date = min(o_date + timedelta(minutes=random.randint(1, 20)), ANCHOR_NOW)
        p_created = p_date
        p_updated = min(p_created + timedelta(minutes=random.randint(0, 60)), ANCHOR_NOW)

        # ----------------------------------------------------------------------
        # Scenario 1: DELIVERED, SHIPPED, PROCESSING
        # ----------------------------------------------------------------------
        if status in ("DELIVERED", "SHIPPED", "PROCESSING"):
            # ~8% of successful orders experienced an initial declined card before successful retry
            if random.random() < 0.08:
                fail_date = min(o_date + timedelta(minutes=1), ANCHOR_NOW)
                fail_ref = make_deterministic_payment_ref(seed, payment_seq, tag="FAIL")
                payment_seq += 1
                payments.append(
                    (
                        o_id,
                        p_method,
                        total,
                        "FAILED",
                        fail_ref,
                        fail_date,
                        fail_date,
                        fail_date,
                    )
                )
                # Successful retry occurred after the failed attempt
                p_date = min(fail_date + timedelta(minutes=random.randint(2, 10)), ANCHOR_NOW)
                if p_date < fail_date:
                    p_date = fail_date
                p_created = p_date
                p_updated = p_date

            succ_ref = make_deterministic_payment_ref(seed, payment_seq, tag="COMPL")
            payment_seq += 1
            payments.append(
                (
                    o_id,
                    p_method,
                    total,
                    "COMPLETED",
                    succ_ref,
                    p_date,
                    p_created,
                    p_updated,
                )
            )

        # ----------------------------------------------------------------------
        # Scenario 2: RETURNED (Order was paid, delivered, then refunded)
        # ----------------------------------------------------------------------
        elif status == "RETURNED":
            # Original successful payment
            orig_ref = make_deterministic_payment_ref(seed, payment_seq, tag="COMPL")
            payment_seq += 1
            payments.append(
                (
                    o_id,
                    p_method,
                    total,
                    "COMPLETED",
                    orig_ref,
                    p_date,
                    p_created,
                    p_updated,
                )
            )

            # Refund record (happens 5 to 14 days later, strictly <= ANCHOR_NOW)
            # Full refund: amount must strictly match original payment total
            refund_date = min(p_date + timedelta(days=random.randint(5, 14)), ANCHOR_NOW)
            if refund_date < p_date:
                refund_date = p_date
            refund_ref = make_deterministic_payment_ref(seed, payment_seq, tag="REF")
            payment_seq += 1
            payments.append(
                (
                    o_id,
                    p_method,
                    total,
                    "REFUNDED",
                    refund_ref,
                    refund_date,
                    refund_date,
                    refund_date,
                )
            )

        # ----------------------------------------------------------------------
        # Scenario 3: CANCELLED Orders
        # Case A (70%): Card declined / failed authorization at checkout.
        # Case B (30%): Paid successfully, then cancelled before shipment -> 100% full refund.
        # ----------------------------------------------------------------------
        elif status == "CANCELLED":
            is_post_payment_cancellation = random.random() < 0.30

            if is_post_payment_cancellation:
                # 1. Original completed payment
                orig_ref = make_deterministic_payment_ref(seed, payment_seq, tag="COMPL")
                payment_seq += 1
                payments.append(
                    (
                        o_id,
                        p_method,
                        total,
                        "COMPLETED",
                        orig_ref,
                        p_date,
                        p_created,
                        p_updated,
                    )
                )

                # 2. Subsequent refund (1 hour to 2 days later, strictly <= ANCHOR_NOW)
                # Full refund: amount must strictly match original payment total
                refund_date = min(p_date + timedelta(hours=random.randint(1, 48)), ANCHOR_NOW)
                if refund_date < p_date:
                    refund_date = p_date
                refund_ref = make_deterministic_payment_ref(seed, payment_seq, tag="REF")
                payment_seq += 1
                payments.append(
                    (
                        o_id,
                        p_method,
                        total,
                        "REFUNDED",
                        refund_ref,
                        refund_date,
                        refund_date,
                        refund_date,
                    )
                )
            else:
                # Checkout failure: single FAILED payment attempt (no refund)
                fail_ref = make_deterministic_payment_ref(seed, payment_seq, tag="FAIL")
                payment_seq += 1
                payments.append(
                    (
                        o_id,
                        p_method,
                        total,
                        "FAILED",
                        fail_ref,
                        p_date,
                        p_created,
                        p_updated,
                    )
                )

        # ----------------------------------------------------------------------
        # Scenario 4: PENDING (Awaiting gateway authorization)
        # ----------------------------------------------------------------------
        else:  # PENDING
            tx_ref = make_deterministic_payment_ref(seed, payment_seq, tag="PEND")
            payment_seq += 1
            payments.append(
                (
                    o_id,
                    p_method,
                    total,
                    "PENDING",
                    tx_ref,
                    p_date,
                    p_created,
                    p_updated,
                )
            )

    return payments


# ------------------------------------------------------------------------------
# Pre-Insertion Integrity Validations
# ------------------------------------------------------------------------------
def validate_order_items_uniqueness(
    flattened_item_rows: List[Tuple[int, int, int, Decimal, datetime]]
) -> None:
    """
    Validates that no duplicate (order_id, product_id) pairs exist before sending
    batches to PostgreSQL. Logs specific order and product IDs on violation.
    """
    seen_pairs: Set[Tuple[int, int]] = set()
    duplicates: List[Tuple[int, int]] = []

    for row in flattened_item_rows:
        order_id = row[0]
        product_id = row[1]
        pair = (order_id, product_id)
        if pair in seen_pairs:
            duplicates.append(pair)
            logger.error(
                "Duplicate line item detected: order_id=%d, product_id=%d already exists in batch!",
                order_id,
                product_id,
            )
        else:
            seen_pairs.add(pair)

    if duplicates:
        raise DataValidationError(
            f"Pre-insert validation failed: Found {len(duplicates)} duplicate (order_id, product_id) "
            f"combinations. Offending pairs (sample): {duplicates[:5]}"
        )
    logger.info(
        "Pre-insert validation passed: all %d line items have unique (order_id, product_id) pairs.",
        len(flattened_item_rows),
    )


# ------------------------------------------------------------------------------
# Pre-Commit Comprehensive Data Validation
# ------------------------------------------------------------------------------
def validate_generated_data(cur: psycopg.Cursor, anchor_now: datetime) -> None:
    """
    Executes pre-commit assertions inside the transaction.
    Raises DataValidationError on any violation, automatically triggering transaction rollback.
    """
    logger.info("Running pre-commit database validation checks...")

    # 1. Exact record count checks
    counts: Dict[str, int] = {}
    for tbl in ["customers", "products", "orders", "order_items", "payments"]:
        cur.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(tbl)))
        counts[tbl] = cur.fetchone()[0]

    logger.info("Verification row counts: %s", counts)

    if counts["customers"] != NUM_CUSTOMERS:
        raise DataValidationError(
            f"Validation Failed: Expected exactly {NUM_CUSTOMERS} customers, found {counts['customers']}."
        )
    if counts["products"] != NUM_PRODUCTS:
        raise DataValidationError(
            f"Validation Failed: Expected exactly {NUM_PRODUCTS} products, found {counts['products']}."
        )
    if counts["orders"] != NUM_ORDERS:
        raise DataValidationError(
            f"Validation Failed: Expected exactly {NUM_ORDERS} orders, found {counts['orders']}."
        )
    if not (20_000 <= counts["order_items"] <= 30_000):
        raise DataValidationError(
            f"Validation Failed: Expected between 20,000 and 30,000 order items, found {counts['order_items']}."
        )
    if not (10_000 <= counts["payments"] <= 12_000):
        raise DataValidationError(
            f"Validation Failed: Expected between 10,000 and 12,000 payments, found {counts['payments']}."
        )

    # 2. Strengthened order total validation:
    # Uses LEFT JOIN between orders and aggregated order_items.
    # Detects orders with zero items or mismatched totals using IS DISTINCT FROM.
    cur.execute(
        """
        SELECT COUNT(*)
        FROM orders o
        LEFT JOIN (
            SELECT order_id, SUM(quantity * unit_price) AS calculated_total
            FROM order_items
            GROUP BY order_id
        ) i ON o.order_id = i.order_id
        WHERE i.order_id IS NULL 
           OR o.total_amount IS DISTINCT FROM i.calculated_total;
        """
    )
    mismatched_totals = cur.fetchone()[0]
    if mismatched_totals > 0:
        cur.execute(
            """
            SELECT 
                o.order_id, 
                o.total_amount, 
                COALESCE(i.calculated_total, 0.00) AS items_total
            FROM orders o
            LEFT JOIN (
                SELECT order_id, SUM(quantity * unit_price) AS calculated_total
                FROM order_items
                GROUP BY order_id
            ) i ON o.order_id = i.order_id
            WHERE i.order_id IS NULL 
               OR o.total_amount IS DISTINCT FROM i.calculated_total
            LIMIT 5;
            """
        )
        sample_errors = cur.fetchall()
        raise DataValidationError(
            f"Validation Failed: Found {mismatched_totals} orders with missing line items or mismatched totals. "
            f"Sample discrepancies (order_id, order_total, items_total): {sample_errors}"
        )

    # 3. Foreign key referential integrity checks
    fk_checks = [
        (
            "orders.customer_id -> customers",
            "SELECT COUNT(*) FROM orders o LEFT JOIN customers c ON o.customer_id = c.customer_id WHERE c.customer_id IS NULL;",
        ),
        (
            "order_items.order_id -> orders",
            "SELECT COUNT(*) FROM order_items oi LEFT JOIN orders o ON oi.order_id = o.order_id WHERE o.order_id IS NULL;",
        ),
        (
            "order_items.product_id -> products",
            "SELECT COUNT(*) FROM order_items oi LEFT JOIN products p ON oi.product_id = p.product_id WHERE p.product_id IS NULL;",
        ),
        (
            "payments.order_id -> orders",
            "SELECT COUNT(*) FROM payments py LEFT JOIN orders o ON py.order_id = o.order_id WHERE o.order_id IS NULL;",
        ),
    ]
    for desc, query in fk_checks:
        cur.execute(query)
        orphans = cur.fetchone()[0]
        if orphans > 0:
            raise DataValidationError(
                f"Validation Failed: Foreign key integrity broken for {desc}: found {orphans} orphaned records."
            )

    # 4. Comprehensive timestamp boundaries across all five tables (none exceeding ANCHOR_NOW)
    timestamp_checks = [
        (
            "customers (created_at/updated_at)",
            "SELECT COUNT(*) FROM customers WHERE created_at > %s OR updated_at > %s;",
        ),
        (
            "products (created_at/updated_at)",
            "SELECT COUNT(*) FROM products WHERE created_at > %s OR updated_at > %s;",
        ),
        (
            "orders (order_date/created_at/updated_at)",
            "SELECT COUNT(*) FROM orders WHERE order_date > %s OR created_at > %s OR updated_at > %s;",
        ),
        (
            "order_items (created_at)",
            "SELECT COUNT(*) FROM order_items WHERE created_at > %s;",
        ),
        (
            "payments (payment_date/created_at/updated_at)",
            "SELECT COUNT(*) FROM payments WHERE payment_date > %s OR created_at > %s OR updated_at > %s;",
        ),
    ]
    for desc, query in timestamp_checks:
        num_placeholders = query.count("%s")
        cur.execute(query, [anchor_now] * num_placeholders)
        future_rows = cur.fetchone()[0]
        if future_rows > 0:
            raise DataValidationError(
                f"Validation Failed: Found {future_rows} records in {desc} with timestamps exceeding ANCHOR_NOW ({anchor_now})."
            )

    # 5. Full refund validation:
    # Every REFUNDED payment must match an earlier COMPLETED payment for the same order with identical amount
    cur.execute(
        """
        SELECT COUNT(*)
        FROM payments r
        WHERE r.status = 'REFUNDED'
          AND NOT EXISTS (
              SELECT 1 FROM payments c
              WHERE c.order_id = r.order_id
                AND c.status = 'COMPLETED'
                AND c.payment_date <= r.payment_date
                AND c.amount = r.amount
          );
        """
    )
    unmatched_refunds = cur.fetchone()[0]
    if unmatched_refunds > 0:
        cur.execute(
            """
            SELECT r.payment_id, r.order_id, r.amount, r.payment_date
            FROM payments r
            WHERE r.status = 'REFUNDED'
              AND NOT EXISTS (
                  SELECT 1 FROM payments c
                  WHERE c.order_id = r.order_id
                    AND c.status = 'COMPLETED'
                    AND c.payment_date <= r.payment_date
                    AND c.amount = r.amount
              )
            LIMIT 5;
            """
        )
        sample_refund_errors = cur.fetchall()
        raise DataValidationError(
            f"Validation Failed: Found {unmatched_refunds} REFUNDED payments without an exact matching earlier COMPLETED payment. "
            f"Sample invalid refunds (payment_id, order_id, amount, payment_date): {sample_refund_errors}"
        )

    logger.info("All pre-commit data validation checks passed successfully!")


# ------------------------------------------------------------------------------
# Orchestration & Execution
# ------------------------------------------------------------------------------
def run_generation(seed: int = DEFAULT_SEED, truncate: bool = False) -> None:
    """
    Main driver executing deterministic generation and transactional database persistence.
    Atomically wraps truncation, multi-table insertion, and validations in one transaction.
    """
    logger.info("Initializing synthetic source data generation (seed=%d)...", seed)
    random.seed(seed)
    Faker.seed(seed)
    fake = Faker()

    conn = get_db_connection()

    try:
        # Step 1: Idempotency & existing data check (read-only preflight query)
        existing_counts = check_existing_data(conn)
        total_existing = sum(existing_counts.values())

        if total_existing > 0 and not truncate:
            logger.error(
                "Database already contains records: %s. Aborting generation to prevent duplicate data.",
                existing_counts,
            )
            logger.info("Rerun with the '--truncate' flag if you wish to reset and re-seed.")
            # Explicitly commit the preflight read transaction before exiting
            conn.commit()
            sys.exit(1)

        # Explicitly commit the read-only preflight transaction.
        # This returns the connection to IDLE state so with conn.transaction() initiates
        # a genuine top-level BEGIN/COMMIT transaction (avoiding nested SAVEPOINTs).
        conn.commit()

        # Step 2: In-memory generation
        # 2a. Customers
        cust_data = generate_customers(fake, NUM_CUSTOMERS)

        # 2b. Products
        prod_data = generate_products(NUM_PRODUCTS)

        # Step 3: Atomic Top-Level Transaction Block
        # Since connection is in IDLE state, with conn.transaction() starts a top-level BEGIN transaction.
        # Truncation, insertion of all entities, and data validation occur in one transaction.
        with conn.transaction():
            with conn.cursor() as cur:
                # 3a. Truncate inside transaction if requested
                # Always executes when --truncate is supplied to reset sequences, even if tables are empty
                if truncate:
                    truncate_tables(cur)

                # 3b. Insert Customers and build safe ID-to-registration map via unique email
                logger.info("Inserting customers into PostgreSQL...")
                cust_cols = [
                    "first_name",
                    "last_name",
                    "email",
                    "city",
                    "country",
                    "created_at",
                    "updated_at",
                ]
                returned_customers = bulk_insert_with_returning(
                    cur, "customers", cust_cols, cust_data, ["customer_id", "email"]
                )
                
                # Map email to customer_id (eliminates row-ordering assumptions)
                cust_id_by_email = {row[1]: row[0] for row in returned_customers}
                if len(cust_id_by_email) != NUM_CUSTOMERS:
                    raise DataValidationError(
                        f"Customer email collision: expected {NUM_CUSTOMERS} unique emails, got {len(cust_id_by_email)}"
                    )

                customer_ids: List[int] = []
                customer_reg_map: Dict[int, datetime] = {}
                for c_row in cust_data:
                    c_email = c_row[2]
                    c_id = cust_id_by_email[c_email]
                    c_reg_dt = c_row[5]
                    customer_ids.append(c_id)
                    customer_reg_map[c_id] = c_reg_dt

                # 3c. Insert Products and build safe ID-to-price map via unique product_name
                logger.info("Inserting products into PostgreSQL...")
                prod_cols = [
                    "product_name",
                    "category",
                    "price",
                    "stock_quantity",
                    "created_at",
                    "updated_at",
                ]
                returned_products = bulk_insert_with_returning(
                    cur, "products", prod_cols, prod_data, ["product_id", "product_name"]
                )
                
                # Map product_name to product_id (eliminates row-ordering assumptions)
                prod_id_by_name = {row[1]: row[0] for row in returned_products}
                if len(prod_id_by_name) != NUM_PRODUCTS:
                    raise DataValidationError(
                        f"Product name collision: expected {NUM_PRODUCTS} unique products, got {len(prod_id_by_name)}"
                    )

                product_ids: List[int] = []
                product_price_map: Dict[int, Decimal] = {}
                for p_row in prod_data:
                    p_name = p_row[0]
                    p_id = prod_id_by_name[p_name]
                    p_price = p_row[2]
                    product_ids.append(p_id)
                    product_price_map[p_id] = p_price

                # 3d. Generate Orders and Line Items
                (
                    order_data,
                    items_by_order,
                    order_statuses,
                    order_dates,
                ) = generate_orders_and_items(
                    customer_ids,
                    customer_reg_map,
                    product_ids,
                    product_price_map,
                    NUM_ORDERS,
                )

                # Validate order totals match line items in-memory prior to insert
                for o_idx, (o_row, items) in enumerate(zip(order_data, items_by_order)):
                    expected_total = o_row[3]
                    actual_total = sum(
                        (Decimal(item[1]) * item[2] for item in items), Decimal("0.00")
                    ).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
                    if expected_total != actual_total:
                        raise DataValidationError(
                            f"In-memory check failed for order index {o_idx}: expected total {expected_total}, items sum to {actual_total}"
                        )

                logger.info("Inserting orders into PostgreSQL...")
                order_cols = [
                    "customer_id",
                    "order_date",
                    "status",
                    "total_amount",
                    "created_at",
                    "updated_at",
                ]
                # Returning customer_id and order_date allows 100% reliable correlation via microsecond indexing
                returned_orders = bulk_insert_with_returning(
                    cur,
                    "orders",
                    order_cols,
                    order_data,
                    ["order_id", "customer_id", "order_date"],
                )
                
                # Decode order_seq directly from microsecond offset (order_seq * 100)
                resolved_order_ids: List[int] = [0] * NUM_ORDERS
                for r in returned_orders:
                    ret_order_id = r[0]
                    ret_cust_id = r[1]
                    ret_order_date = r[2]
                    
                    seq = ret_order_date.microsecond // 100
                    if not (0 <= seq < NUM_ORDERS):
                        raise DataValidationError(
                            f"Invalid order sequence decoded from timestamp {ret_order_date}: seq={seq}"
                        )
                    if order_data[seq][0] != ret_cust_id:
                        raise DataValidationError(
                            f"Correlation mismatch at seq={seq}: expected customer {order_data[seq][0]}, got {ret_cust_id}"
                        )
                    resolved_order_ids[seq] = ret_order_id

                if len(set(resolved_order_ids)) != NUM_ORDERS or 0 in resolved_order_ids:
                    raise DataValidationError(
                        f"Order correlation failed: resolved {len(set(resolved_order_ids))} unique IDs out of {NUM_ORDERS}"
                    )

                # 3e. Assemble, Validate and Insert Order Items
                logger.info("Assembling and validating order items prior to insertion...")
                flattened_item_rows = []
                for o_id, items in zip(resolved_order_ids, items_by_order):
                    for prod_id, qty, price, dt in items:
                        flattened_item_rows.append((o_id, prod_id, qty, price, dt))

                # Pre-insert validation: Detect and log any duplicate (order_id, product_id)
                validate_order_items_uniqueness(flattened_item_rows)

                logger.info("Inserting order items into PostgreSQL...")
                item_cols = ["order_id", "product_id", "quantity", "unit_price", "created_at"]
                bulk_insert(cur, "order_items", item_cols, flattened_item_rows)

                # 3f. Generate and Insert Payments
                order_totals = [row[3] for row in order_data]
                payment_rows = generate_payments(
                    resolved_order_ids, order_totals, order_statuses, order_dates, seed=seed
                )

                logger.info("Inserting payments into PostgreSQL...")
                payment_cols = [
                    "order_id",
                    "payment_method",
                    "amount",
                    "status",
                    "transaction_reference",
                    "payment_date",
                    "created_at",
                    "updated_at",
                ]
                bulk_insert(cur, "payments", payment_cols, payment_rows)

                # 3g. Comprehensive Database Validation (triggers rollback if any rule fails)
                validate_generated_data(cur, ANCHOR_NOW)

        # Step 4: Verification & Final Report
        # The top-level transaction is now permanently committed to PostgreSQL.
        final_counts = check_existing_data(conn)
        conn.commit()  # Close the reporting query transaction cleanly

        logger.info("=" * 60)
        logger.info("SOURCE DATA GENERATION SUMMARY (SUCCESS)")
        logger.info("=" * 60)
        logger.info("Customers inserted   : %d", final_counts.get("customers", 0))
        logger.info("Products inserted    : %d", final_counts.get("products", 0))
        logger.info("Orders inserted      : %d", final_counts.get("orders", 0))
        logger.info("Order Items inserted : %d", final_counts.get("order_items", 0))
        logger.info("Payments inserted    : %d", final_counts.get("payments", 0))
        logger.info("=" * 60)

    except psycopg.Error as db_err:
        logger.error(
            "Database error during generation. Transaction rolled back: %s (SQLSTATE: %s)",
            db_err,
            getattr(db_err, "sqlstate", "N/A"),
        )
        if conn and not conn.closed:
            conn.rollback()
        raise
    except DataValidationError as val_err:
        logger.error(
            "Data validation failed. Transaction rolled back: %s",
            val_err,
        )
        if conn and not conn.closed:
            conn.rollback()
        raise
    except Exception as exc:
        logger.error("Unexpected error during data generation: %s", exc)
        if conn and not conn.closed:
            conn.rollback()
        raise
    finally:
        if conn and not conn.closed:
            conn.close()
            logger.info("PostgreSQL connection closed.")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="RetailPulse synthetic e-commerce source data generator."
    )
    parser.add_argument(
        "--truncate",
        action="store_true",
        help="Wipe existing records in source tables before generation.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=DEFAULT_SEED,
        help="Pseudorandom seed for deterministic data generation (default: 42).",
    )
    args = parser.parse_args()

    run_generation(seed=args.seed, truncate=args.truncate)


if __name__ == "__main__":
    main()
