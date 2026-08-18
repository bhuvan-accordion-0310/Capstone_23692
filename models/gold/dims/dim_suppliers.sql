{{ config(
    materialized = 'table'
) }}

-- Supplier dimension.

WITH suppliers AS (

    SELECT

        supplier_id,
        supplier_name,
        contact_person,
        email,
        phone,
        address,
        payment_terms,
        supplier_type

    FROM {{ ref('silver_suppliers') }}

),

final AS (

    SELECT

        -- Surrogate key generated from the natural supplier_id

        {{ dbt_utils.generate_surrogate_key([
            'supplier_id'
        ]) }} AS supplier_key,

        supplier_id,

        supplier_name,

        -- Consolidated contact information

        CONCAT_WS(
            ' | ',

            NULLIF(
                TRIM(contact_person),
                ''
            ),

            NULLIF(
                TRIM(email),
                ''
            ),

            NULLIF(
                TRIM(phone),
                ''
            ),

            NULLIF(
                TRIM(address),
                ''
            )

        ) AS contact_information,

        payment_terms,
        supplier_type

    FROM suppliers

)

SELECT *

FROM final
