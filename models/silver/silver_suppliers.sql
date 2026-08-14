{{ config(
    materialized = 'table',
    schema = 'SILVER',
    alias = 'SILVER_SUPPLIERS'
) }}

-- Extract and clean supplier data from Bronze

with supplier_source as (

    select

        supplier_id,

        initcap(
            trim(raw_payload:supplier_name::string)
        ) as supplier_name,

        initcap(
            trim(raw_payload:supplier_type::string)
        ) as supplier_type,

        initcap(
            trim(raw_payload:credit_rating::string)
        ) as credit_rating,

        raw_payload:is_active::boolean as is_active,

        try_to_date(
            nullif(
                trim(raw_payload:last_modified_date::string),
                ''
            )
        ) as last_modified_date,

        try_to_date(
            nullif(
                trim(raw_payload:last_order_date::string),
                ''
            )
        ) as last_order_date,

        try_to_number(
            nullif(
                trim(raw_payload:lead_time_days::string),
                ''
            )
        ) as lead_time_days,

        try_to_number(
            nullif(
                trim(raw_payload:minimum_order_quantity::string),
                ''
            )
        ) as minimum_order_quantity,

        initcap(
            trim(raw_payload:payment_terms::string)
        ) as payment_terms,

        initcap(
            trim(raw_payload:preferred_carrier::string)
        ) as preferred_carrier,

        trim(raw_payload:tax_id::string) as tax_id,

        lower(
            trim(raw_payload:website::string)
        ) as website,

        try_to_number(
            nullif(
                trim(raw_payload:year_established::string),
                ''
            )
        ) as year_established,

        -- Supplier categories

        raw_payload:categories_supplied as categories_supplied,

        -- Contact information

        initcap(
            trim(raw_payload:contact_information:contact_person::string)
        ) as contact_person,

        case
            when regexp_like(
                trim(raw_payload:contact_information:email::string),
                '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
            )
            then lower(
                trim(raw_payload:contact_information:email::string)
            )
            else null
        end as email,

        case
            when length(
                regexp_replace(
                    raw_payload:contact_information:phone::string,
                    '[^0-9]',
                    ''
                )
            ) >= 10
            then regexp_replace(
                raw_payload:contact_information:phone::string,
                '[^0-9]',
                ''
            )
            else null
        end as phone,

        initcap(
            trim(raw_payload:contact_information:address::string)
        ) as address,

        -- Contract details

        trim(
            raw_payload:contract_details:contract_id::string
        ) as contract_id,

        try_to_date(
            nullif(
                trim(raw_payload:contract_details:start_date::string),
                ''
            )
        ) as contract_start_date,

        try_to_date(
            nullif(
                trim(raw_payload:contract_details:end_date::string),
                ''
            )
        ) as contract_end_date,

        raw_payload:contract_details:exclusivity::boolean
            as contract_exclusivity,

        raw_payload:contract_details:renewal_option::boolean
            as contract_renewal_option,

        -- Performance metrics

        try_to_decimal(
            nullif(
                trim(
                    raw_payload:performance_metrics:average_delay_days::string
                ),
                ''
            ),
            10,
            2
        ) as average_delay_days,

        try_to_decimal(
            nullif(
                trim(
                    raw_payload:performance_metrics:defect_rate::string
                ),
                ''
            ),
            10,
            2
        ) as defect_rate,

        try_to_decimal(
            nullif(
                trim(
                    raw_payload:performance_metrics:on_time_delivery_rate::string
                ),
                ''
            ),
            10,
            2
        ) as on_time_delivery_rate,

        initcap(
            trim(
                raw_payload:performance_metrics:quality_rating::string
            )
        ) as quality_rating,

        try_to_decimal(
            nullif(
                trim(
                    raw_payload:performance_metrics:response_time_hours::string
                ),
                ''
            ),
            10,
            2
        ) as response_time_hours,

        try_to_decimal(
            nullif(
                trim(
                    raw_payload:performance_metrics:returns_percentage::string
                ),
                ''
            ),
            10,
            2
        ) as returns_percentage,

        _source_file,
        _loaded_at,
        _batch_id

    from {{ ref('br_suppliers') }}

    where supplier_id is not null

),

-- Remove duplicate supplier records

deduplicated as (

    select *

    from supplier_source

    qualify row_number() over (
        partition by supplier_id
        order by
            _loaded_at desc,
            _source_file desc
    ) = 1

)

-- Final Silver Suppliers table

select

    supplier_id,

    supplier_name,
    supplier_type,
    credit_rating,
    is_active,

    last_modified_date,
    last_order_date,

    lead_time_days,
    minimum_order_quantity,
    payment_terms,
    preferred_carrier,

    tax_id,
    website,
    year_established,

    categories_supplied,

    contact_person,
    email,
    phone,
    address,

    contract_id,
    contract_start_date,
    contract_end_date,
    contract_exclusivity,
    contract_renewal_option,

    average_delay_days,
    defect_rate,
    on_time_delivery_rate,
    quality_rating,
    response_time_hours,
    returns_percentage,

    _source_file,
    _loaded_at,
    _batch_id

from deduplicated