{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = ['employee_id', '_source_file'],
    schema = 'BRONZE',
    alias = 'BR_EMPLOYEES'
) }}

SELECT
    value:employee_id::string AS employee_id,
    value AS raw_payload,

    METADATA$FILENAME AS _source_file,
    CURRENT_TIMESTAMP() AS _loaded_at,
    '{{ invocation_id }}' AS _batch_id

FROM {{ source('landing', 'EXT_EMPLOYEE_DATA') }}

{% if is_incremental() %}

WHERE NOT EXISTS (
    SELECT 1
    FROM {{ this }} existing
    WHERE existing.employee_id = value:employee_id::string
      AND existing._source_file = METADATA$FILENAME
)

{% endif %}