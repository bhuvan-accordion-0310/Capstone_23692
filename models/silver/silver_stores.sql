{{ config(
    materialized = 'table',
    schema = 'SILVER',
    alias = 'SILVER_STORES'
) }}

WITH bronze_data AS (

    SELECT
        store_id,
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

    FROM {{ ref('br_stores') }}

),

cleaned AS (

    SELECT

        -- Store key

        NULLIF(
            TRIM(store_id),
            ''
        ) AS store_id,


        -- Store details

        NULLIF(
            REGEXP_REPLACE(
                INITCAP(
                    TRIM(raw_payload:store_name::STRING)
                ),
                '[^A-Za-z0-9'' -]',
                ''
            ),
            ''
        ) AS store_name,


        -- Store size

        TRY_TO_NUMBER(
            NULLIF(
                TRIM(raw_payload:size_sq_ft::STRING),
                ''
            )
        ) AS size_sq_ft,


        -- Opening date

        TRY_TO_DATE(
            TRIM(raw_payload:opening_date::STRING)
        ) AS opening_date,


        -- Address

        NULLIF(
            INITCAP(
                REGEXP_REPLACE(
                    TRIM(raw_payload:address:street::STRING),
                    '[^A-Za-z0-9'' -]',
                    ''
                )
            ),
            ''
        ) AS address_street,

        NULLIF(
            INITCAP(
                TRIM(raw_payload:address:city::STRING)
            ),
            ''
        ) AS address_city,

        NULLIF(
            UPPER(
                TRIM(raw_payload:address:state::STRING)
            ),
            ''
        ) AS address_state,

        NULLIF(
            TRIM(raw_payload:address:zip_code::STRING),
            ''
        ) AS address_zip_code,

        NULLIF(
            INITCAP(
                TRIM(raw_payload:address:country::STRING)
            ),
            ''
        ) AS address_country,


        -- Sales values

        TRY_TO_DECIMAL(
            REGEXP_REPLACE(
                TRIM(raw_payload:sales_target::STRING),
                '[$,]',
                ''
            ),
            18,
            2
        ) AS sales_target,

        TRY_TO_DECIMAL(
            REGEXP_REPLACE(
                TRIM(raw_payload:current_sales::STRING),
                '[$,]',
                ''
            ),
            18,
            2
        ) AS current_sales,


        -- Employee count

        TRY_TO_NUMBER(
            NULLIF(
                TRIM(raw_payload:employee_count::STRING),
                ''
            )
        ) AS employee_count,


        -- Bronze metadata

        _source_file,
        _loaded_at,
        _batch_id,
        source_snapshot_date

    FROM bronze_data

),

transformed AS (

    SELECT

        store_id,
        store_name,
        size_sq_ft,
        opening_date,

        -- Store size category

        CASE
            WHEN size_sq_ft < 5000
                THEN 'Small'

            WHEN size_sq_ft >= 5000
             AND size_sq_ft <= 10000
                THEN 'Medium'

            WHEN size_sq_ft > 10000
                THEN 'Large'

            ELSE NULL
        END AS store_size_category,


        -- Address

        address_street,
        address_city,
        address_state,
        address_zip_code,
        address_country,

        NULLIF(
            CONCAT_WS(
                ', ',
                address_street,
                address_city,
                address_state,
                address_zip_code,
                address_country
            ),
            ''
        ) AS standardized_address,


        -- Sales values

        sales_target,
        current_sales,
        employee_count,


        -- Store age

        CASE
            WHEN opening_date IS NOT NULL
             AND opening_date <= CURRENT_DATE()
            THEN
                DATEDIFF(
                    'year',
                    opening_date,
                    CURRENT_DATE()
                )
                -
                CASE
                    WHEN DATEADD(
                        'year',
                        DATEDIFF(
                            'year',
                            opening_date,
                            CURRENT_DATE()
                        ),
                        opening_date
                    ) > CURRENT_DATE()
                    THEN 1
                    ELSE 0
                END
            ELSE NULL
        END AS store_age_years,


        -- Sales target achievement

        CAST(
            CASE
                WHEN sales_target > 0
                THEN (
                    current_sales / sales_target
                ) * 100
                ELSE NULL
            END
            AS DECIMAL(18, 2)
        ) AS sales_target_achievement_percentage,


        -- Revenue per square foot

        CAST(
            CASE
                WHEN size_sq_ft > 0
                THEN current_sales / size_sq_ft
                ELSE NULL
            END
            AS DECIMAL(18, 2)
        ) AS revenue_per_sq_ft,


        -- Employee efficiency

        CAST(
            CASE
                WHEN employee_count > 0
                THEN current_sales / employee_count
                ELSE NULL
            END
            AS DECIMAL(18, 2)
        ) AS employee_efficiency,


        -- Source metadata used for deduplication

        _source_file,
        _loaded_at,
        _batch_id,
        source_snapshot_date

    FROM cleaned

),

final_data AS (

    SELECT

        *,
        
        -- Performance issue flag

        CASE
            WHEN sales_target_achievement_percentage < 90
                THEN TRUE
            ELSE FALSE
        END AS performance_issue_flag

    FROM transformed

),

deduplicated AS (

    SELECT *

    FROM final_data

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY store_id
        ORDER BY
            source_snapshot_date DESC NULLS LAST,
            _loaded_at DESC
    ) = 1

)

SELECT

    store_id,
    store_name,
    size_sq_ft,
    store_size_category,

    opening_date,
    store_age_years,

    address_street,
    address_city,
    address_state,
    address_zip_code,
    address_country,
    standardized_address,

    sales_target,
    current_sales,
    employee_count,

    sales_target_achievement_percentage,
    revenue_per_sq_ft,
    employee_efficiency,

    performance_issue_flag

FROM deduplicated