{{ config(
    materialized = 'table',
    schema = 'SILVER',
    alias = 'SILVER_PRODUCTS'
) }}

WITH bronze_data AS (

    SELECT
        product_id,
        raw_payload,
        _source_file,
        _loaded_at,
        _batch_id,

        TRY_TO_DATE(
            REGEXP_SUBSTR(
                _source_file,
                '[0-9]{4}-[0-9]{2}-[0-9]{2}'
            )
        ) AS source_snapshot_date

    FROM {{ ref('br_products') }}

),

cleaned AS (

    SELECT

        -- Product key

        NULLIF(
            TRIM(product_id),
            ''
        ) AS product_id,


        -- Product details

        NULLIF(
            INITCAP(
                REGEXP_REPLACE(
                    TRIM(raw_payload:name::STRING),
                    '[^A-Za-z0-9'' -]',
                    ''
                )
            ),
            ''
        ) AS product_name,

        NULLIF(
            TRIM(raw_payload:short_description::STRING),
            ''
        ) AS short_description,

        NULLIF(
            TRIM(raw_payload:technical_specs::STRING),
            ''
        ) AS technical_specs,


        -- Product hierarchy

        NULLIF(
            INITCAP(
                REGEXP_REPLACE(
                    TRIM(raw_payload:category::STRING),
                    '[^A-Za-z0-9'' -]',
                    ''
                )
            ),
            ''
        ) AS category,

        NULLIF(
            INITCAP(
                REGEXP_REPLACE(
                    TRIM(raw_payload:subcategory::STRING),
                    '[^A-Za-z0-9'' -]',
                    ''
                )
            ),
            ''
        ) AS subcategory,

        NULLIF(
            INITCAP(
                REGEXP_REPLACE(
                    TRIM(raw_payload:product_line::STRING),
                    '[^A-Za-z0-9'' -]',
                    ''
                )
            ),
            ''
        ) AS product_line,


        -- Monetary values

        TRY_TO_DECIMAL(
            REGEXP_REPLACE(
                TRIM(raw_payload:unit_price::STRING),
                '[$,]',
                ''
            ),
            18,
            2
        ) AS unit_price,

        TRY_TO_DECIMAL(
            REGEXP_REPLACE(
                TRIM(raw_payload:cost_price::STRING),
                '[$,]',
                ''
            ),
            18,
            2
        ) AS cost_price,


        -- Inventory

        TRY_TO_NUMBER(
            NULLIF(
                TRIM(raw_payload:stock_quantity::STRING),
                ''
            )
        ) AS stock_quantity,

        TRY_TO_NUMBER(
            NULLIF(
                TRIM(raw_payload:reorder_level::STRING),
                ''
            )
        ) AS reorder_level,


        -- Bronze metadata

        _source_file,
        _loaded_at,
        _batch_id,
        source_snapshot_date

    FROM bronze_data

),

transformed AS (

    SELECT

        product_id,

        product_name,
        short_description,
        technical_specs,

        -- Product full description

        NULLIF(
            CONCAT_WS(
                ' | ',
                NULLIF(product_name, ''),
                NULLIF(short_description, ''),
                NULLIF(technical_specs, '')
            ),
            ''
        ) AS product_full_description,


        -- Product hierarchy

        NULLIF(
            CONCAT_WS(
                ' > ',
                NULLIF(category, ''),
                NULLIF(subcategory, ''),
                NULLIF(product_line, '')
            ),
            ''
        ) AS product_hierarchy,

        category,
        subcategory,
        product_line,

        unit_price,
        cost_price,

        -- Profit margin percentage

        -- the logic behind this was simple maths
        -- sp-cp

        CAST(
            CASE
                WHEN unit_price > 0
                THEN ((unit_price - cost_price) / unit_price) * 100
                ELSE NULL
            END
            AS DECIMAL(18, 2)
        ) AS profit_margin_percentage,


        stock_quantity,
        reorder_level,

        -- Low stock flag

        CASE
            WHEN stock_quantity IS NOT NULL
             AND reorder_level IS NOT NULL
             AND stock_quantity < reorder_level
            THEN TRUE
            ELSE FALSE
        END AS low_stock_flag,


        _source_file,
        _loaded_at,
        _batch_id,
        source_snapshot_date

    FROM cleaned

),

deduplicated AS (

    SELECT *

    FROM transformed

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY product_id
        ORDER BY
            source_snapshot_date DESC NULLS LAST,
            _loaded_at DESC
    ) = 1

)

SELECT

    product_id,

    product_name,
    short_description,
    technical_specs,
    product_full_description,

    category,
    subcategory,
    product_line,
    product_hierarchy,

    unit_price,
    cost_price,
    profit_margin_percentage,

    stock_quantity,
    reorder_level,
    low_stock_flag,

    _source_file,
    _loaded_at,
    _batch_id,
    source_snapshot_date

FROM deduplicated