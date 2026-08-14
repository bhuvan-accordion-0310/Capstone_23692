{{ config(
    materialized = 'table',
    schema = 'SILVER',
    alias = 'SILVER_CUSTOMERS'
) }}

WITH bronze_data AS (

    SELECT
        customer_id,
        raw_payload,
        _source_file,
        _loaded_at,
        _batch_id,

        /*
         * Source snapshot date is present in the source filename.
         * This is used only for Silver deduplication.
         */
        TRY_TO_DATE(
            REGEXP_SUBSTR(
                _source_file,
                '[0-9]{4}-[0-9]{2}-[0-9]{2}'
            )
        ) AS source_snapshot_date

    FROM {{ ref('br_customers') }}

),

cleaned AS (

    SELECT

        /* =========================================================
           CUSTOMER KEY
           ========================================================= */

        NULLIF(
            TRIM(customer_id),
            ''
        ) AS customer_id,


        /* =========================================================
           NAME
           Generic rules:
           - trim whitespace
           - remove unwanted characters
           - standardize capitalization
           ========================================================= */

        NULLIF(
            INITCAP(
                REGEXP_REPLACE(
                    TRIM(raw_payload:first_name::STRING),
                    '[^A-Za-z'' -]',
                    ''
                )
            ),
            ''
        ) AS first_name,

        NULLIF(
            INITCAP(
                REGEXP_REPLACE(
                    TRIM(raw_payload:last_name::STRING),
                    '[^A-Za-z'' -]',
                    ''
                )
            ),
            ''
        ) AS last_name,


        /* =========================================================
           CUSTOMER-SPECIFIC TRANSFORMATION
           full_name = FirstName || ' ' || LastName
           ========================================================= */

        NULLIF(
            TRIM(
                CONCAT(
                    COALESCE(
                        INITCAP(
                            REGEXP_REPLACE(
                                TRIM(raw_payload:first_name::STRING),
                                '[^A-Za-z'' -]',
                                ''
                            )
                        ),
                        ''
                    ),
                    ' ',
                    COALESCE(
                        INITCAP(
                            REGEXP_REPLACE(
                                TRIM(raw_payload:last_name::STRING),
                                '[^A-Za-z'' -]',
                                ''
                            )
                        ),
                        ''
                    )
                )
            ),
            ''
        ) AS full_name,


        /* =========================================================
           EMAIL
           Generic rule:
           validate email and flag invalid values
           ========================================================= */

        LOWER(
            NULLIF(
                TRIM(raw_payload:email::STRING),
                ''
            )
        ) AS email_raw,


        /* =========================================================
           PHONE
           Generic rule:
           remove unwanted characters and normalize
           ========================================================= */

        NULLIF(
            REGEXP_REPLACE(
                TRIM(raw_payload:phone::STRING),
                '[^0-9]',
                ''
            ),
            ''
        ) AS phone_normalized,


        /* =========================================================
           DATES
           Canonical DATE format
           ========================================================= */

        TRY_TO_DATE(
            TRIM(raw_payload:birth_date::STRING)
        ) AS birth_date,

        TRY_TO_DATE(
            TRIM(raw_payload:registration_date::STRING)
        ) AS registration_date,

        TRY_TO_DATE(
            TRIM(raw_payload:last_purchase_date::STRING)
        ) AS last_purchase_date,

        TRY_TO_DATE(
            TRIM(raw_payload:last_modified_date::STRING)
        ) AS last_modified_date,


        /* =========================================================
           CUSTOMER ATTRIBUTES
           ========================================================= */

        UPPER(
            NULLIF(
                TRIM(raw_payload:income_bracket::STRING),
                ''
            )
        ) AS income_bracket,

        NULLIF(
            INITCAP(
                REGEXP_REPLACE(
                    TRIM(raw_payload:occupation::STRING),
                    '[^A-Za-z'' -]',
                    ''
                )
            ),
            ''
        ) AS occupation,

        UPPER(
            NULLIF(
                TRIM(raw_payload:loyalty_tier::STRING),
                ''
            )
        ) AS loyalty_tier,

        TRY_TO_BOOLEAN(
            raw_payload:marketing_opt_in::STRING
        ) AS marketing_opt_in,

        UPPER(
            NULLIF(
                TRIM(raw_payload:preferred_communication::STRING),
                ''
            )
        ) AS preferred_communication,

        INITCAP(
            NULLIF(
                TRIM(raw_payload:preferred_payment_method::STRING),
                ''
            )
        ) AS preferred_payment_method,


        /* =========================================================
           NUMERIC VALUES
           ========================================================= */

        TRY_TO_NUMBER(
            NULLIF(
                TRIM(raw_payload:total_purchases::STRING),
                ''
            )
        ) AS total_purchases,


        /* =========================================================
           MONETARY VALUE
           Generic rule:
           strip $, commas and whitespace
           All amounts are USD
           ========================================================= */

        TRY_TO_DECIMAL(
            REGEXP_REPLACE(
                TRIM(raw_payload:total_spend::STRING),
                '[$,]',
                ''
            ),
            18,
            2
        ) AS total_spend,


        /* =========================================================
           ADDRESS
           Customer-specific:
           standardize address format
           ========================================================= */

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


        /* =========================================================
           BRONZE METADATA
           ========================================================= */

        _source_file,
        _loaded_at,
        _batch_id,
        source_snapshot_date

    FROM bronze_data

),

validated AS (

    SELECT
        *,

        /* =========================================================
           EMAIL VALIDATION
           Invalid email is retained as NULL and flagged.
           ========================================================= */

        CASE
            WHEN email_raw IS NOT NULL
             AND REGEXP_LIKE(
                 email_raw,
                 '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
             )
            THEN email_raw
            ELSE NULL
        END AS email_id,

        CASE
            WHEN email_raw IS NOT NULL
             AND REGEXP_LIKE(
                 email_raw,
                 '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
             )
            THEN TRUE
            ELSE FALSE
        END AS email_valid,


        /* =========================================================
           PHONE VALIDATION
           Canonical format = 10 digits.
           Invalid phone is retained as NULL and flagged.
           ========================================================= */

        CASE
            WHEN phone_normalized IS NOT NULL
             AND LENGTH(phone_normalized) = 10
            THEN phone_normalized
            ELSE NULL
        END AS phn_no,

        CASE
            WHEN phone_normalized IS NOT NULL
             AND LENGTH(phone_normalized) = 10
            THEN TRUE
            ELSE FALSE
        END AS phn_no_valid,


        /* =========================================================
           STANDARDIZED ADDRESS
           ========================================================= */

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
        ) AS standardized_address

    FROM cleaned

),

derived AS (

    SELECT
        *,

        /* =========================================================
           CUSTOMER-SPECIFIC:
           CUSTOMER AGE
           ========================================================= */

        CASE
            WHEN birth_date IS NOT NULL
             AND birth_date <= CURRENT_DATE()
            THEN
                DATEDIFF(
                    'year',
                    birth_date,
                    CURRENT_DATE()
                )
                -
                CASE
                    WHEN DATEADD(
                        'year',
                        DATEDIFF(
                            'year',
                            birth_date,
                            CURRENT_DATE()
                        ),
                        birth_date
                    ) > CURRENT_DATE()
                    THEN 1
                    ELSE 0
                END
            ELSE NULL
        END AS customer_age

    FROM validated

),

segmented AS (

    SELECT
        *,

        /* =========================================================
           CUSTOMER-SPECIFIC:
           NON-OVERLAPPING AGE BANDS

           Young       = 18-35
           Middle-aged = 36-55
           Senior      = 56+
           ========================================================= */

        CASE
            WHEN customer_age BETWEEN 18 AND 35
                THEN 'Young'

            WHEN customer_age BETWEEN 36 AND 55
                THEN 'Middle-aged'

            WHEN customer_age >= 56
                THEN 'Senior'

            ELSE NULL
        END AS customer_segment

    FROM derived

),

deduplicated AS (

    SELECT
        *

    FROM segmented

    /*
     * Silver generic rule:
     * deduplicate using QUALIFY on the natural key.
     *
     * Since Bronze contains multiple source files,
     * retain the latest source version for each customer.
     */

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY customer_id
        ORDER BY
            source_snapshot_date DESC NULLS LAST,
            last_modified_date DESC NULLS LAST,
            _loaded_at DESC,
            _source_file DESC
    ) = 1

)

SELECT
    customer_id,
    first_name,
    last_name,
    full_name,

    email_id,
    email_valid,

    phn_no,
    phn_no_valid,

    birth_date,
    customer_age,
    customer_segment,

    registration_date,
    last_purchase_date,
    last_modified_date,

    income_bracket,
    occupation,
    loyalty_tier,
    marketing_opt_in,
    preferred_communication,
    preferred_payment_method,

    total_purchases,
    total_spend,

    address_street,
    address_city,
    address_state,
    address_zip_code,
    address_country,
    standardized_address,

    _source_file,
    _loaded_at,
    _batch_id,
    source_snapshot_date

FROM deduplicated