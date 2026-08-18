{{ config(
    materialized = 'table',
    schema = 'SILVER',
    alias = 'SILVER_ORDERS'
) }}

-- Extract and clean order header data from Bronze

with order_source as (

    select

        order_id,

        nullif(trim(raw_payload:customer_id::string), '') as customer_id,
        nullif(trim(raw_payload:store_id::string), '') as store_id,
        nullif(trim(raw_payload:employee_id::string), '') as employee_id,
        nullif(trim(raw_payload:campaign_id::string), '') as campaign_id,

        try_to_timestamp_tz(
            nullif(trim(raw_payload:order_date::string), '')
        ) as order_datetime,

        try_to_date(
            nullif(trim(raw_payload:order_date::string), '')
        ) as order_date,

        try_to_date(
            nullif(trim(raw_payload:shipping_date::string), '')
        ) as shipping_date,

        try_to_date(
            nullif(trim(raw_payload:delivery_date::string), '')
        ) as delivery_date,

        try_to_date(
            nullif(trim(raw_payload:estimated_delivery_date::string), '')
        ) as estimated_delivery_date,

        -- Source discount is a percentage, so convert it to a fraction

        coalesce(
            try_to_decimal(
                nullif(trim(raw_payload:discount_amount::string), ''),
                18,
                6
            ) / 100,
            0
        ) as order_discount_rate,

        -- Parse monetary values into numeric decimals

        coalesce(
            try_to_decimal(
                nullif(
                    regexp_replace(
                        trim(raw_payload:shipping_cost::string),
                        '[$,]',
                        ''
                    ),
                    ''
                ),
                18,
                2
            ),
            0.00
        ) as shipping_cost,

        coalesce(
            try_to_decimal(
                nullif(
                    regexp_replace(
                        trim(raw_payload:tax_amount::string),
                        '[$,]',
                        ''
                    ),
                    ''
                ),
                18,
                2
            ),
            0.00
        ) as tax_amount,

        -- Standardize descriptive fields

        initcap(trim(raw_payload:order_source::string)) as order_source,
        initcap(trim(raw_payload:order_status::string)) as order_status,
        initcap(trim(raw_payload:payment_method::string)) as payment_method,
        initcap(trim(raw_payload:shipping_method::string)) as shipping_method,

        -- Standardize billing address fields

        initcap(trim(raw_payload:billing_address:street::string)) as billing_street,
        initcap(trim(raw_payload:billing_address:city::string)) as billing_city,
        upper(trim(raw_payload:billing_address:state::string)) as billing_state,
        trim(raw_payload:billing_address:zip_code::string) as billing_zip_code,

        -- Standardize shipping address fields

        initcap(trim(raw_payload:shipping_address:street::string)) as shipping_street,
        initcap(trim(raw_payload:shipping_address:city::string)) as shipping_city,
        upper(trim(raw_payload:shipping_address:state::string)) as shipping_state,
        trim(raw_payload:shipping_address:zip_code::string) as shipping_zip_code,

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

flattened_items as (

    select

        o.order_id,

        item.value as item_data

    from order_source o,
         lateral flatten(
             input => o.order_items
         ) item

),

-- Clean order item values

cleaned_items as (

    select

        order_id,

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
        ) as item_discount_rate

    from flattened_items

),

-- Aggregate order items to order grain

order_item_aggregates as (

    select

        order_id,

        count(product_id) as total_items,

        sum(quantity) as total_quantity,

        sum(
            quantity * unit_price
        ) as total_amount,

        sum(
            quantity * cost_price
        ) as total_cost,

        sum(
            item_discount_rate
        ) as total_discount,

        sum(
            quantity
            * unit_price
            * (1 - item_discount_rate)
        ) as line_revenue,

        sum(
            quantity * cost_price
        ) as line_cost

    from cleaned_items

    group by order_id

),

-- Combine order header with item aggregates

combined as (

    select

        o._source_file,
        o._loaded_at,
        o._batch_id,
        o.source_snapshot_date,

        o.order_id,
        o.customer_id,
        o.store_id,
        o.employee_id,
        o.campaign_id,

        o.order_datetime,
        o.order_date,
        o.shipping_date,
        o.delivery_date,
        o.estimated_delivery_date,

        o.order_source,
        o.order_status,
        o.payment_method,
        o.shipping_method,

        o.order_discount_rate,

        o.shipping_cost,
        o.tax_amount,

        o.billing_street,
        o.billing_city,
        o.billing_state,
        o.billing_zip_code,

        o.shipping_street,
        o.shipping_city,
        o.shipping_state,
        o.shipping_zip_code,

        coalesce(i.total_items, 0) as total_items,
        coalesce(i.total_quantity, 0) as total_quantity,
        coalesce(i.total_amount, 0.00)::decimal(18,2) as total_amount,
        coalesce(i.total_cost, 0.00)::decimal(18,2) as total_cost,
        coalesce(i.total_discount, 0.00)::decimal(18,6) as total_discount,

        coalesce(i.line_revenue, 0.00)::decimal(18,2) as line_revenue,
        coalesce(i.line_cost, 0.00)::decimal(18,2) as line_cost

    from order_source o

    left join order_item_aggregates i
        on o.order_id = i.order_id

),

-- Calculate order profitability and delivery metrics

derived as (

    select

        c.*,

        extract(
            hour from c.order_datetime
        ) as order_hour,

        case
            when extract(hour from c.order_datetime) >= 5
                 and extract(hour from c.order_datetime) < 12
                then 'Morning'

            when extract(hour from c.order_datetime) >= 12
                 and extract(hour from c.order_datetime) < 17
                then 'Afternoon'

            when extract(hour from c.order_datetime) >= 17
                 and extract(hour from c.order_datetime) < 22
                then 'Evening'

            else 'Night'
        end as order_time_of_day,

        week(c.order_date) as order_week,
        month(c.order_date) as order_month,
        quarter(c.order_date) as order_quarter,
        year(c.order_date) as order_year,

        -- Calculate profit after order discount, cost, shipping and tax

        (
            c.line_revenue * (1 - c.order_discount_rate)
        )
        - c.line_cost
        - c.shipping_cost
        - c.tax_amount
        as profit_amount,

        -- Calculate profit margin with divide-by-zero protection

        case
            when c.line_revenue > 0
                then (
                    (
                        c.line_revenue * (1 - c.order_discount_rate)
                    )
                    - c.line_cost
                    - c.shipping_cost
                    - c.tax_amount
                ) / c.line_revenue * 100
            else null
        end as profit_margin_percentage,

        -- Calculate order processing time

        datediff(
            day,
            c.order_date,
            c.shipping_date
        ) as processing_days,

        -- Calculate shipping time

        datediff(
            day,
            c.shipping_date,
            c.delivery_date
        ) as shipping_days,

        -- Determine delivery status

        case
            when c.delivery_date is not null
                 and c.delivery_date <= c.estimated_delivery_date
                then 'On Time'

            when c.delivery_date is not null
                 and c.delivery_date > c.estimated_delivery_date
                then 'Delayed'

            when c.delivery_date is null
                 and current_date() > c.estimated_delivery_date
                then 'Potentially Delayed'

            else 'In Transit'
        end as delivery_status

    from combined c

),

-- Remove duplicate order records

deduplicated as (

    select *

    from derived

    qualify row_number() over (
        partition by order_id
        order by
            source_snapshot_date desc nulls last,
            _loaded_at desc,
            _source_file desc
    ) = 1

)

-- Final Silver Orders table

select

    order_id,
    customer_id,
    store_id,
    employee_id,
    campaign_id,

    order_datetime,
    order_date,
    shipping_date,
    delivery_date,
    estimated_delivery_date,

    order_source,
    order_status,
    payment_method,
    shipping_method,

    order_discount_rate,

    total_items,
    total_quantity,
    total_amount,
    total_cost,
    total_discount,

    line_revenue,
    line_cost,
    shipping_cost,
    tax_amount,

    profit_amount,
    profit_margin_percentage,

    order_hour,
    order_time_of_day,

    order_week,
    order_month,
    order_quarter,
    order_year,

    processing_days,
    shipping_days,
    delivery_status,

    billing_street,
    billing_city,
    billing_state,
    billing_zip_code,

    shipping_street,
    shipping_city,
    shipping_state,
    shipping_zip_code,

    source_snapshot_date,

    _source_file,
    _loaded_at,
    _batch_id

from deduplicated