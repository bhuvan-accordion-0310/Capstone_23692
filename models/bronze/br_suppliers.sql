{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = ['supplier_id', '_source_file'],
    schema = 'BRONZE',
    alias = 'BR_SUPPLIERS'
) }}

SELECT
    value:supplier_id::string AS supplier_id,
    value AS raw_payload,

    METADATA$FILENAME AS _source_file,
    CURRENT_TIMESTAMP() AS _loaded_at,
    '{{ invocation_id }}' AS _batch_id

FROM {{ source('landing', 'EXT_SUPPLIER_DATA') }}

{% if is_incremental() %}

WHERE NOT EXISTS (
    SELECT 1
    FROM {{ this }} existing
    WHERE existing.supplier_id = value:supplier_id::string
      AND existing._source_file = METADATA$FILENAME
)

{% endif %}