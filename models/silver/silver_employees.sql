{{ config(
    materialized = 'table',
    schema = 'SILVER',
    alias = 'SILVER_EMPLOYEES'
) }}

-- Extract and clean employee data from Bronze

with employee_source as (

    select

        employee_id,

        nullif(
            trim(raw_payload:first_name::string),
            ''
        ) as first_name,

        nullif(
            trim(raw_payload:last_name::string),
            ''
        ) as last_name,

        nullif(
            trim(raw_payload:email::string),
            ''
        ) as email,

        nullif(
            trim(raw_payload:phone::string),
            ''
        ) as phone,

        try_to_date(
            nullif(
                trim(raw_payload:date_of_birth::string),
                ''
            )
        ) as date_of_birth,

        initcap(
            trim(raw_payload:department::string)
        ) as department,

        initcap(
            trim(raw_payload:role::string)
        ) as role,

        initcap(
            trim(raw_payload:education::string)
        ) as education,

        initcap(
            trim(raw_payload:employment_status::string)
        ) as employment_status,

        try_to_date(
            nullif(
                trim(raw_payload:hire_date::string),
                ''
            )
        ) as hire_date,

        try_to_date(
            nullif(
                trim(raw_payload:last_modified_date::string),
                ''
            )
        ) as last_modified_date,

        nullif(
            trim(raw_payload:manager_id::string),
            ''
        ) as manager_id,

        nullif(
            trim(raw_payload:work_location::string),
            ''
        ) as work_location,

        -- Parse monetary values into numeric decimals

        coalesce(
            try_to_decimal(
                nullif(
                    regexp_replace(
                        trim(raw_payload:salary::string),
                        '[$,]',
                        ''
                    ),
                    ''
                ),
                18,
                2
            ),
            0.00
        ) as salary,

        coalesce(
            try_to_decimal(
                nullif(
                    regexp_replace(
                        trim(raw_payload:current_sales::string),
                        '[$,]',
                        ''
                    ),
                    ''
                ),
                18,
                2
            ),
            0.00
        ) as current_sales,

        coalesce(
            try_to_decimal(
                nullif(
                    regexp_replace(
                        trim(raw_payload:sales_target::string),
                        '[$,]',
                        ''
                    ),
                    ''
                ),
                18,
                2
            ),
            0.00
        ) as sales_target,

        try_to_decimal(
            nullif(
                trim(raw_payload:performance_rating::string),
                ''
            ),
            5,
            2
        ) as performance_rating,

        -- Keep certifications as an array

        raw_payload:certifications as certifications,

        -- Standardize address fields

        initcap(
            trim(raw_payload:address:street::string)
        ) as address_street,

        initcap(
            trim(raw_payload:address:city::string)
        ) as address_city,

        upper(
            trim(raw_payload:address:state::string)
        ) as address_state,

        trim(
            raw_payload:address:zip_code::string
        ) as address_zip_code,

        -- Validate email format

        case
            when regexp_like(
                trim(raw_payload:email::string),
                '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
            )
            then lower(trim(raw_payload:email::string))
            else null
        end as validated_email,

        -- Normalize phone number to digits only

        case
            when length(
                regexp_replace(
                    raw_payload:phone::string,
                    '[^0-9]',
                    ''
                )
            ) >= 10
            then regexp_replace(
                raw_payload:phone::string,
                '[^0-9]',
                ''
            )
            else null
        end as normalized_phone,

        -- Derive source snapshot date from the source file

        to_date(
            regexp_substr(
                _source_file,
                '[0-9]{4}-[0-9]{2}-[0-9]{2}'
            )
        ) as source_snapshot_date,

        _source_file,
        _loaded_at,
        _batch_id

    from {{ ref('br_employees') }}

    where employee_id is not null

),

-- Remove duplicate employee records for each source snapshot

deduplicated as (

    select *

    from employee_source

    qualify row_number() over (
        partition by employee_id, source_snapshot_date
        order by
            _loaded_at desc,
            _source_file desc
    ) = 1

)

-- Final Silver Employees table

select

    employee_id,

    first_name,
    last_name,

    concat(
        first_name,
        ' ',
        last_name
    ) as full_name,

    validated_email as email,
    normalized_phone as phone,

    date_of_birth,
    department,
    role,
    education,
    employment_status,

    hire_date,
    last_modified_date,

    manager_id,
    work_location,

    salary,
    current_sales,
    sales_target,
    performance_rating,

    certifications,

    address_street,
    address_city,
    address_state,
    address_zip_code,

    source_snapshot_date,

    _source_file,
    _loaded_at,
    _batch_id

from deduplicated