{% snapshot snp_br_products %}

{{
    config(
        target_schema='SNAPSHOTS',
        unique_key="product_id || '|' || source_snapshot_date",
        strategy='timestamp',
        updated_at='source_snapshot_date'
    )
}}

WITH product_data AS (

    SELECT

        product_id,
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

    FROM {{ ref('br_products') }}

)

SELECT

    product_id,
    raw_payload,
    _source_file,
    _loaded_at,
    _batch_id,
    source_snapshot_date

FROM product_data

{% endsnapshot %}