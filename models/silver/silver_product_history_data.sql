{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='product_history_key',
    on_schema_change='sync_all_columns'
) }}

WITH source_files AS (

    SELECT

        PRODUCT_ID,
        RAW_PAYLOAD,
        _SOURCE_FILE,
        _LOADED_AT,
        _BATCH_ID,

        TRY_TO_DATE(
            REGEXP_SUBSTR(
                _SOURCE_FILE,
                '[0-9]{4}-[0-9]{2}-[0-9]{2}'
            )
        ) AS source_snapshot_date

    FROM {{ ref('br_products') }}

    {% if is_incremental() %}

        WHERE TRY_TO_DATE(
            REGEXP_SUBSTR(
                _SOURCE_FILE,
                '[0-9]{4}-[0-9]{2}-[0-9]{2}'
            )
        ) > (

            SELECT COALESCE(
                MAX(source_snapshot_date),
                DATE '1900-01-01'
            )

            FROM {{ this }}

        )

    {% endif %}

),

-- 1. EXTRACT PRODUCT SNAPSHOT DATA

cleaned AS (

    SELECT

        -- Product history key

        {{ dbt_utils.generate_surrogate_key([
            'PRODUCT_ID',
            'source_snapshot_date'
        ]) }} AS product_history_key,

        -- Bronze metadata

        _SOURCE_FILE,
        source_snapshot_date,
        _LOADED_AT,
        _BATCH_ID,

        -- Product ID

        NULLIF(
            TRIM(PRODUCT_ID),
            ''
        ) AS product_id,

        -- Product name

        REGEXP_REPLACE(
            INITCAP(
                TRIM(
                    RAW_PAYLOAD:name::VARCHAR
                )
            ),
            '[^A-Za-z0-9]',
            ''
        ) AS product_name,

        -- Full product description

        TRIM(
            CONCAT_WS(
                ' - ',

                NULLIF(
                    TRIM(
                        RAW_PAYLOAD:name::VARCHAR
                    ),
                    ''
                ),

                NULLIF(
                    TRIM(
                        RAW_PAYLOAD:short_description::VARCHAR
                    ),
                    ''
                ),

                NULLIF(
                    TRIM(
                        RAW_PAYLOAD:technical_specs::VARCHAR
                    ),
                    ''
                )

            )
        ) AS full_description,

        -- Description components

        TRIM(
            REGEXP_REPLACE(
                RAW_PAYLOAD:short_description::VARCHAR,
                '[^A-Za-z0-9 ''.,;:/()&%-]',
                ''
            )
        ) AS short_description,

        TRIM(
            REGEXP_REPLACE(
                RAW_PAYLOAD:technical_specs::VARCHAR,
                '[^A-Za-z0-9 ''.,;:/()&%_=-]',
                ''
            )
        ) AS technical_specs,

        -- Product hierarchy

        REGEXP_REPLACE(
            INITCAP(
                TRIM(
                    RAW_PAYLOAD:category::VARCHAR
                )
            ),
            '[^A-Za-z0-9]',
            ''
        ) AS category,

        REGEXP_REPLACE(
            INITCAP(
                TRIM(
                    RAW_PAYLOAD:subcategory::VARCHAR
                )
            ),
            '[^A-Za-z0-9]',
            ''
        ) AS subcategory,

        REGEXP_REPLACE(
            INITCAP(
                TRIM(
                    RAW_PAYLOAD:product_line::VARCHAR
                )
            ),
            '[^A-Za-z0-9]',
            ''
        ) AS product_line,

        -- Product attributes

        INITCAP(
            TRIM(
                RAW_PAYLOAD:brand::VARCHAR
            )
        ) AS brand,

        INITCAP(
            TRIM(
                RAW_PAYLOAD:color::VARCHAR
            )
        ) AS color,

        INITCAP(
            TRIM(
                RAW_PAYLOAD:size::VARCHAR
            )
        ) AS size,

        -- Money

        TRY_TO_DECIMAL(
            NULLIF(
                REGEXP_REPLACE(
                    TRIM(
                        RAW_PAYLOAD:unit_price::VARCHAR
                    ),
                    '[$,]',
                    ''
                ),
                ''
            ),
            18,
            2
        ) AS unit_price,

        TRY_TO_DECIMAL(
            NULLIF(
                REGEXP_REPLACE(
                    TRIM(
                        RAW_PAYLOAD:cost_price::VARCHAR
                    ),
                    '[$,]',
                    ''
                ),
                ''
            ),
            18,
            2
        ) AS cost_price,

        -- Inventory

        TRY_TO_NUMBER(
            NULLIF(
                TRIM(
                    RAW_PAYLOAD:stock_quantity::VARCHAR
                ),
                ''
            )
        ) AS stock_quantity,

        TRY_TO_NUMBER(
            NULLIF(
                TRIM(
                    RAW_PAYLOAD:reorder_level::VARCHAR
                ),
                ''
            )
        ) AS reorder_level,

        -- Supplier

        NULLIF(
            TRIM(
                RAW_PAYLOAD:supplier_id::VARCHAR
            ),
            ''
        ) AS supplier_id,

        -- Other product attributes

        TRIM(
            RAW_PAYLOAD:dimensions::VARCHAR
        ) AS dimensions,

        TRIM(
            RAW_PAYLOAD:warranty_period::VARCHAR
        ) AS warranty_period,

        TRY_TO_DECIMAL(
            NULLIF(
                REGEXP_REPLACE(
                    LOWER(
                        TRIM(
                            RAW_PAYLOAD:weight::VARCHAR
                        )
                    ),
                    '[^0-9.\-]',
                    ''
                ),
                ''
            ),
            10,
            2
        ) AS weight_kg,

        TRY_TO_DATE(
            NULLIF(
                TRIM(
                    RAW_PAYLOAD:launch_date::VARCHAR
                ),
                ''
            )
        ) AS launch_date,

        COALESCE(
            RAW_PAYLOAD:is_featured::BOOLEAN,
            FALSE
        ) AS is_featured,

        -- Source modification date

        TRY_TO_DATE(
            NULLIF(
                TRIM(
                    RAW_PAYLOAD:last_modified_date::VARCHAR
                ),
                ''
            )
        ) AS last_modified_date

    FROM source_files

    WHERE PRODUCT_ID IS NOT NULL

),

-- 2. DERIVED ATTRIBUTES

derived AS (

    SELECT

        c.*,

        -- Product hierarchy

        TRIM(
            CONCAT_WS(
                ' > ',

                NULLIF(c.category, ''),
                NULLIF(c.subcategory, ''),
                NULLIF(c.product_line, '')

            )
        ) AS product_hierarchy,

        -- Profit margin

        CASE

            WHEN c.unit_price > 0
             AND c.cost_price IS NOT NULL

            THEN (
                (c.unit_price - c.cost_price)
                / c.unit_price
            ) * 100

            ELSE NULL

        END AS profit_margin_percentage,

        -- Low stock flag

        CASE

            WHEN c.stock_quantity IS NULL
              OR c.reorder_level IS NULL

            THEN NULL

            WHEN c.stock_quantity < c.reorder_level

            THEN TRUE

            ELSE FALSE

        END AS low_stock_flag

    FROM cleaned c

),

-- 3. DEDUPLICATION

deduplicated AS (

    SELECT *

    FROM derived

    QUALIFY ROW_NUMBER() OVER (

        PARTITION BY
            product_id,
            source_snapshot_date

        ORDER BY
            _SOURCE_FILE DESC,
            _LOADED_AT DESC

    ) = 1

)

-- FINAL PRODUCT HISTORY TABLE

SELECT

    product_history_key,

    _SOURCE_FILE,
    source_snapshot_date,
    _LOADED_AT,
    _BATCH_ID,

    product_id,

    product_name,
    full_description,
    short_description,
    technical_specs,

    category,
    subcategory,
    product_line,
    product_hierarchy,

    brand,
    color,
    size,

    unit_price,
    cost_price,
    profit_margin_percentage,

    stock_quantity,
    reorder_level,
    low_stock_flag,

    supplier_id,

    dimensions,
    weight_kg,
    warranty_period,

    is_featured,
    launch_date,
    last_modified_date

FROM deduplicated