{% snapshot snp_br_suppliers %}

{{
    config(
        target_schema = 'SNAPSHOTS',
        unique_key = 'supplier_id',
        strategy = 'timestamp',
        updated_at = 'source_snapshot_ts'
    )
}}

WITH bronze_records AS (

    SELECT
        supplier_id,
        raw_payload,
        _source_file,
        _loaded_at,
        _batch_id,

        TRY_TO_TIMESTAMP_NTZ(
            REGEXP_SUBSTR(
                _source_file,
                '[0-9]{4}-[0-9]{2}-[0-2]{2}'
            )
        ) AS source_snapshot_ts

    FROM {{ ref('br_suppliers') }}

),

deduplicated AS (

    SELECT
        supplier_id,
        raw_payload,
        _source_file,
        _loaded_at,
        _batch_id,
        source_snapshot_ts

    FROM bronze_records

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY supplier_id
        ORDER BY
            source_snapshot_ts DESC,
            _loaded_at DESC,
            _source_file DESC
    ) = 1

)

SELECT
    supplier_id,
    raw_payload,
    _source_file,
    _loaded_at,
    _batch_id,
    source_snapshot_ts

FROM deduplicated

{% endsnapshot %}