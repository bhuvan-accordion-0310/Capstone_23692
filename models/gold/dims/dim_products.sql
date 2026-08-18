{{ config(
    materialized = 'table'
) }}

-- Product dimension.
--
-- NOTE: silver_products.sql does not carry supplier_id, brand,
-- color, or size, but silver_product_history_data.sql does (it is
-- sourced from the same br_products bronze table but was not
-- pruned to the latest-snapshot column set). This model pulls the
-- most recent snapshot per product from silver_product_history_data
-- so the supplier relationship (needed by fact_inventory) is
-- available.

WITH latest_product_snapshot AS (

    SELECT

        product_id,
        product_name,
        category,
        subcategory,
        product_line,
        brand,
        color,
        size,
        unit_price,
        cost_price,
        supplier_id,
        source_snapshot_date

    FROM {{ ref('silver_product_history_data') }}

    QUALIFY ROW_NUMBER() OVER (

        PARTITION BY
            product_id

        ORDER BY
            source_snapshot_date DESC

    ) = 1

),

suppliers AS (

    SELECT

        supplier_id,
        supplier_name

    FROM {{ ref('silver_suppliers') }}

),

final AS (

    SELECT

        -- Surrogate key generated from the natural product ID

        {{ dbt_utils.generate_surrogate_key([
            'p.product_id'
        ]) }} AS product_key,

        p.product_id,

        p.product_name,
        p.category,
        p.subcategory,
        p.product_line,
        p.brand,
        p.color,
        p.size,

        p.unit_price,
        p.cost_price,

        p.supplier_id,
        s.supplier_name

    FROM latest_product_snapshot p

    LEFT JOIN suppliers s
        ON p.supplier_id = s.supplier_id

)

SELECT *

FROM final
