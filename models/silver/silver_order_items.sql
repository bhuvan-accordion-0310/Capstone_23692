{{ config(
    materialized = 'table',
    schema = 'SILVER',
    alias = 'SILVER_ORDER_ITEMS'
) }}

-- Extract and clean order line item data from Bronze
--
-- silver_orders.sql aggregates order items up to the order
-- header grain, so it cannot answer product-level questions.
-- This model keeps the item-level grain (order_id + item_number)
-- and is the feeder model for silver_inventory (product/store
-- level sold quantity).

with order_source as (

    select

        order_id,

        nullif(trim(raw_payload:customer_id::string), '') as customer_id,
        nullif(trim(raw_payload:store_id::string), '') as store_id,

        try_to_date(
            nullif(trim(raw_payload:order_date::string), '')
        ) as order_date,

        initcap(trim(raw_payload:order_status::string)) as order_status,

        raw_payload:order_items as order_items,

        to_date(
            regexp_substr(
                _source_file,
                '[0-9]{4}-[0-9]{2}-[0-9]{2}'
            )
        ) as source_snapshot_date,

        _source_file,
        _loaded_at,
        _batch_id

    from {{ ref('br_orders') }}

    where order_id is not null

),

-- Flatten order items
--
-- flatten.index gives the item's position within the
-- order_items array. It is converted to a 1-based item_number
-- so the item grain (order_id + item_number) is stable.

flattened_items as (

    select

        o.order_id,
        o.customer_id,
        o.store_id,
        o.order_date,
        o.order_status,

        o.source_snapshot_date,
        o._source_file,
        o._loaded_at,
        o._batch_id,

        item.index + 1 as item_number,
        item.value as item_data

    from order_source o,
         lateral flatten(
             input => o.order_items
         ) item

),

-- Clean order item values

cleaned as (

    select

        {{ dbt_utils.generate_surrogate_key([
            'order_id',
            'item_number'
        ]) }} as order_item_key,

        order_id,
        item_number,

        order_date,
        order_status,
        customer_id,
        store_id,

        nullif(
            trim(item_data:product_id::string),
            ''
        ) as product_id,

        coalesce(
            try_to_number(
                nullif(
                    trim(item_data:quantity::string),
                    ''
                )
            ),
            0
        ) as quantity,

        coalesce(
            try_to_decimal(
                nullif(
                    regexp_replace(
                        trim(item_data:unit_price::string),
                        '[$,]',
                        ''
                    ),
                    ''
                ),
                18,
                2
            ),
            0.00
        ) as unit_price,

        coalesce(
            try_to_decimal(
                nullif(
                    regexp_replace(
                        trim(item_data:cost_price::string),
                        '[$,]',
                        ''
                    ),
                    ''
                ),
                18,
                2
            ),
            0.00
        ) as cost_price,

        -- Item discount is a percentage, so convert it to a fraction

        coalesce(
            try_to_decimal(
                nullif(
                    trim(item_data:discount_amount::string),
                    ''
                ),
                18,
                6
            ) / 100,
            0
        ) as item_discount_rate,

        source_snapshot_date,
        _source_file,
        _loaded_at,
        _batch_id

    from flattened_items

),

-- Remove duplicate order item records
--
-- Keeps the most recently loaded version of each order_id +
-- item_number combination.

deduplicated as (

    select *

    from cleaned

    qualify row_number() over (
        partition by order_id, item_number
        order by
            source_snapshot_date desc nulls last,
            _loaded_at desc,
            _source_file desc
    ) = 1

)

-- Final Silver Order Items table

select

    order_item_key,

    order_id,
    item_number,

    order_date,
    order_status,
    customer_id,
    store_id,

    product_id,
    quantity,

    unit_price,
    cost_price,
    item_discount_rate,

    source_snapshot_date,

    _source_file,
    _loaded_at,
    _batch_id

from deduplicated
