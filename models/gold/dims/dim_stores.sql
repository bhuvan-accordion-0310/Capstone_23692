{{ config(
    materialized = 'table'
) }}

-- Store dimension.
--
-- NOTE: silver_stores.sql does not have explicit "region" or
-- "store_type" columns. address_state is used as the geographic
-- grouping field, and store_size_category is used as the store
-- classification field.

WITH stores AS (

    SELECT

        store_id,
        store_name,
        standardized_address,
        address_city,
        address_state,
        address_country,
        opening_date,
        store_size_category

    FROM {{ ref('silver_stores') }}

),

final AS (

    SELECT

        -- Surrogate key generated from the natural store ID

        {{ dbt_utils.generate_surrogate_key([
            'store_id'
        ]) }} AS store_key,

        store_id,

        store_name,

        standardized_address AS address,
        address_city,
        address_state,
        address_country,

        opening_date,

        store_size_category AS size_category

    FROM stores

)

SELECT *

FROM final
