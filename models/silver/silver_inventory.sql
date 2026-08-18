{{ config(
    materialized='table'
) }}

WITH product_history AS (

    SELECT

        product_history_key,
        product_id,
        source_snapshot_date,
        stock_quantity,
        reorder_level,
        supplier_id,
        cost_price

    FROM {{ ref('silver_product_history_data') }}

),

-- 1. PRODUCT / STORE RELATIONSHIP

-- Product History has no store_id.
-- Store association comes from Order Items.

product_store AS (

    SELECT DISTINCT

        product_id,
        store_id

    FROM {{ ref('silver_order_items') }}

    WHERE product_id IS NOT NULL
      AND store_id IS NOT NULL

),

-- 2. PRODUCT HISTORY + PRODUCT/STORE RELATIONSHIP

-- Creates the product + store + snapshot date inventory grain.

inventory_snapshots AS (

    SELECT

        ph.product_id,
        ps.store_id,

        TRY_TO_DATE(
            ph.source_snapshot_date
        ) AS inventory_date,

        TRY_TO_NUMBER(
            ph.stock_quantity
        ) AS ending_stock,

        TRY_TO_NUMBER(
            ph.reorder_level
        ) AS reorder_level,

        ph.supplier_id,

        TRY_TO_DECIMAL(
            ph.cost_price,
            18,
            2
        ) AS cost_price

    FROM product_history ph

    INNER JOIN product_store ps
        ON ph.product_id = ps.product_id

),

-- 3. BEGINNING INVENTORY

-- Previous snapshot ending stock for the same product/store.

with_beginning_inventory AS (

    SELECT

        product_id,
        store_id,
        inventory_date,

        LAG(
            ending_stock
        ) OVER (

            PARTITION BY
                product_id,
                store_id

            ORDER BY
                inventory_date

        ) AS beginning_stock,

        ending_stock,

        reorder_level,
        supplier_id,
        cost_price

    FROM inventory_snapshots

),

-- 4. COMPLETED ORDER ITEM SALES

-- Only completed and delivered orders are considered as sales.

completed_sales AS (

    SELECT

        product_id,
        store_id,
        order_date AS inventory_date,

        SUM(quantity) AS sold_quantity

    FROM {{ ref('silver_order_items') }}

    WHERE LOWER(order_status) IN (
        'completed',
        'delivered'
    )

      AND product_id IS NOT NULL
      AND store_id IS NOT NULL
      AND order_date IS NOT NULL

    GROUP BY

        product_id,
        store_id,
        order_date

),

-- 5. COMBINE STOCK + SALES

-- Inventory records are retained even when there are no sales.

combined AS (

    SELECT

        b.product_id,
        b.store_id,
        b.inventory_date,

        b.beginning_stock,

        COALESCE(
            s.sold_quantity,
            0
        ) AS sold_quantity,

        b.ending_stock,

        b.reorder_level,
        b.supplier_id,
        b.cost_price

    FROM with_beginning_inventory b

    LEFT JOIN completed_sales s

        ON b.product_id = s.product_id

       AND b.store_id = s.store_id

       AND b.inventory_date = s.inventory_date

),

-- 6. INVENTORY BUSINESS CALCULATIONS

calculated AS (

    SELECT

        product_id,
        store_id,
        inventory_date,

        beginning_stock,

        sold_quantity,

        ending_stock,

        -- Purchased quantity
        -- Ending Stock - Beginning Stock + Sold Quantity

        CASE

            WHEN beginning_stock IS NOT NULL
             AND ending_stock IS NOT NULL

            THEN
                ending_stock
                - beginning_stock
                + sold_quantity

            ELSE NULL

        END AS purchased_quantity,

        -- Inventory value

        CASE

            WHEN ending_stock IS NOT NULL
             AND cost_price IS NOT NULL

            THEN
                ending_stock * cost_price

            ELSE NULL

        END AS inventory_value,

        -- Average inventory

        CASE

            WHEN beginning_stock IS NOT NULL
             AND ending_stock IS NOT NULL

            THEN
                (
                    beginning_stock
                    + ending_stock
                ) / 2.0

            ELSE NULL

        END AS average_inventory,

        reorder_level,
        supplier_id,
        cost_price

    FROM combined

),

-- 7. SUPPLIER CONTRIBUTION

-- Supplier contribution is based on purchased quantity
-- against total purchased quantity for the inventory date.

supplier_contribution AS (

    SELECT

        *,

        CASE

            WHEN SUM(
                purchased_quantity
            ) OVER (
                PARTITION BY
                    inventory_date
            ) > 0

            THEN
                (
                    purchased_quantity
                    / SUM(
                        purchased_quantity
                    ) OVER (
                        PARTITION BY
                            inventory_date
                    )
                ) * 100

            ELSE 0

        END AS supplier_contribution_percentage

    FROM calculated

),

-- 8. SNAPSHOT GAP

-- Previous inventory date for the same product/store.

with_snapshot_gap AS (

    SELECT

        *,

        LAG(
            inventory_date
        ) OVER (

            PARTITION BY
                product_id,
                store_id

            ORDER BY
                inventory_date

        ) AS previous_inventory_date

    FROM supplier_contribution

),

-- 9. FINAL INVENTORY METRICS

final AS (

    SELECT

        -- Product + Store + Date

        {{ dbt_utils.generate_surrogate_key([
            'product_id',
            'store_id',
            'inventory_date'
        ]) }} AS inventory_key,

        product_id,
        store_id,
        inventory_date,

        beginning_stock,
        purchased_quantity,
        sold_quantity,
        ending_stock,

        inventory_value,

        -- Stock turnover ratio

        CASE

            WHEN average_inventory > 0

            THEN
                sold_quantity / average_inventory

            ELSE NULL

        END AS stock_turnover_ratio,

        -- Supplier contribution percentage

        supplier_contribution_percentage,

        -- Snapshot gap flag

        CASE

            WHEN previous_inventory_date IS NULL

            THEN FALSE

            WHEN DATEDIFF(
                DAY,
                previous_inventory_date,
                inventory_date
            ) > 1

            THEN TRUE

            ELSE FALSE

        END AS snapshot_gap_flag,

        -- Snapshot gap days

        CASE

            WHEN previous_inventory_date IS NULL

            THEN 0

            ELSE DATEDIFF(
                DAY,
                previous_inventory_date,
                inventory_date
            )

        END AS snapshot_gap_days,

        -- Low stock flag

        CASE

            WHEN ending_stock IS NOT NULL
             AND reorder_level IS NOT NULL
             AND ending_stock < reorder_level

            THEN TRUE

            ELSE FALSE

        END AS low_stock_flag,

        -- Negative inferred purchase flag

        CASE

            WHEN purchased_quantity < 0

            THEN TRUE

            ELSE FALSE

        END AS negative_inferred_purchase_flag,

        reorder_level,
        supplier_id

    FROM with_snapshot_gap

)

-- FINAL SILVER INVENTORY TABLE

SELECT

    inventory_key,

    product_id,
    store_id,
    inventory_date,

    beginning_stock,
    purchased_quantity,
    sold_quantity,
    ending_stock,

    inventory_value,
    stock_turnover_ratio,
    supplier_contribution_percentage,

    reorder_level,
    supplier_id,

    snapshot_gap_flag,
    snapshot_gap_days,

    low_stock_flag,
    negative_inferred_purchase_flag

FROM final