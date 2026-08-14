{{ config(
    materialized = 'table',
    schema = 'SILVER',
    alias = 'SILVER_INVENTORY'
) }}

-- Get inventory data from Bronze

with inventory_source as (

    select
        product_id,

        to_date(
            regexp_substr(
                _source_file,
                '[0-9]{4}-[0-9]{2}-[0-9]{2}'
            )
        ) as source_snapshot_date,

        raw_payload:stock_quantity::number as stock_quantity,
        raw_payload:reorder_level::number as reorder_level,

        _loaded_at,
        _source_file

    from {{ ref('br_products') }}

    where product_id is not null

),

-- Keep one record per product and snapshot date

inventory_daily as (

    select
        product_id,
        source_snapshot_date,
        stock_quantity,
        reorder_level,
        _loaded_at,
        _source_file

    from inventory_source

    where source_snapshot_date is not null

    qualify row_number() over (
        partition by product_id, source_snapshot_date
        order by _loaded_at desc, _source_file desc
    ) = 1

),

-- Calculate beginning stock from the previous snapshot

stock_positions as (

    select
        product_id,
        source_snapshot_date as snapshot_date,

        lag(stock_quantity) over (
            partition by product_id
            order by source_snapshot_date
        ) as beginning_stock,

        stock_quantity as ending_stock,

        reorder_level

    from inventory_daily

),

-- Calculate the quantity sold for each product on each day

-- Calculate the quantity sold for each product on each day

daily_sold as (

    select
        oi_data.value:product_id::string as product_id,

        o.raw_payload:order_date::date as stock_date,

        sum(
            oi_data.value:quantity::number
        ) as sold_quantity

    from {{ ref('br_orders') }} o

    cross join lateral flatten(
        input => o.raw_payload:order_items
    ) oi_data

    where o.raw_payload:order_date is not null
      and lower(o.raw_payload:order_status::string) = 'completed'
      and oi_data.value:product_id::string is not null

    group by
        oi_data.value:product_id::string,
        o.raw_payload:order_date::date

),

-- Calculate stock movement

inventory_calculation as (

    select
        sp.product_id,
        sp.snapshot_date,

        sp.beginning_stock,
        sp.ending_stock,

        coalesce(
            ds.sold_quantity,
            0
        ) as sold_quantity,

        sp.reorder_level,

        case
            when sp.beginning_stock is not null
             and sp.ending_stock is not null
            then greatest(
                sp.ending_stock
                - sp.beginning_stock
                + coalesce(ds.sold_quantity, 0),
                0
            )
            else null
        end as purchased_quantity,

        case
            when sp.beginning_stock is not null
             and sp.ending_stock is not null
            then
                sp.ending_stock
                - sp.beginning_stock
                + coalesce(ds.sold_quantity, 0)
            else null
        end as stock_adjustment_quantity

    from stock_positions sp

    left join daily_sold ds
        on sp.product_id = ds.product_id
       and sp.snapshot_date = ds.stock_date

),

-- Create the final inventory indicators

final_data as (

    select
        product_id,
        snapshot_date,

        beginning_stock,
        ending_stock,
        sold_quantity,
        purchased_quantity,
        reorder_level,

        case
            when ending_stock is not null
             and reorder_level is not null
             and ending_stock < reorder_level
            then true
            else false
        end as low_stock_flag,

        case
            when stock_adjustment_quantity < 0
            then true
            else false
        end as negative_balance_flag

    from inventory_calculation

)

select *
from final_data