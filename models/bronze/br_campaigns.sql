{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = ['campaign_id', '_source_file'],
    schema = 'BRONZE',
    alias = 'BR_CAMPAIGNS'
) }}

SELECT
    value:campaign_id::string AS campaign_id,
    value AS raw_payload,

    METADATA$FILENAME AS _source_file,
    CURRENT_TIMESTAMP() AS _loaded_at,
    '{{ invocation_id }}' AS _batch_id

FROM {{ source('landing', 'EXT_CAMPAIGN_DATA') }}

{% if is_incremental() %}

WHERE NOT EXISTS (
    SELECT 1
    FROM {{ this }} existing
    WHERE existing.campaign_id = value:campaign_id::string
      AND existing._source_file = METADATA$FILENAME
)

{% endif %}