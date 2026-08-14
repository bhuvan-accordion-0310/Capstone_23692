{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = ['customer_id', '_source_file'],
    schema = 'BRONZE',
    alias = 'BR_CUSTOMERS'
) }}

SELECT
    value:customer_id::string AS customer_id,
    value AS raw_payload,

    METADATA$FILENAME AS _source_file,
    CURRENT_TIMESTAMP() AS _loaded_at,
    '{{ invocation_id }}' AS _batch_id

FROM {{ source('landing', 'EXT_CUSTOMER_DATA') }}

{% if is_incremental() %}

WHERE NOT EXISTS (
    SELECT 1
    FROM {{ this }} existing
    WHERE existing.customer_id = value:customer_id::string
      AND existing._source_file = METADATA$FILENAME
)

{% endif %}