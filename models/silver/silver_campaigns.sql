{{ config(
    materialized = 'table',
    schema = 'SILVER',
    alias = 'SILVER_CAMPAIGNS'
) }}

WITH bronze_data AS (

    SELECT
        campaign_id,
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

    FROM {{ ref('br_campaigns') }}

),

cleaned AS (

    SELECT

        -- Campaign key

        NULLIF(
            TRIM(campaign_id),
            ''
        ) AS campaign_id,


        -- Campaign details

        NULLIF(
            TRIM(raw_payload:campaign_name::STRING),
            ''
        ) AS campaign_name,


        -- Campaign dates

        TRY_TO_DATE(
            TRIM(raw_payload:start_date::STRING)
        ) AS start_date,

        TRY_TO_DATE(
            TRIM(raw_payload:end_date::STRING)
        ) AS end_date,


        -- Campaign financial values

        TRY_TO_DECIMAL(
            REGEXP_REPLACE(
                TRIM(raw_payload:budget::STRING),
                '[$,]',
                ''
            ),
            18,
            2
        ) AS budget,

        TRY_TO_DECIMAL(
            REGEXP_REPLACE(
                TRIM(raw_payload:total_cost::STRING),
                '[$,]',
                ''
            ),
            18,
            2
        ) AS total_cost,

        TRY_TO_DECIMAL(
            REGEXP_REPLACE(
                TRIM(raw_payload:total_revenue::STRING),
                '[$,]',
                ''
            ),
            18,
            2
        ) AS total_revenue,


        -- ROI value supplied by the source

        TRY_TO_DECIMAL(
            TRIM(raw_payload:roi_calculation::STRING),
            18,
            2
        ) AS roi_calculation,


        -- Campaign demographics

        raw_payload:demographics AS demographics,


        -- Source metadata used for deduplication

        _source_file,
        _loaded_at,
        _batch_id,
        source_snapshot_date

    FROM bronze_data

),

transformed AS (

    SELECT

        campaign_id,
        campaign_name,

        start_date,
        end_date,

        -- Campaign duration

        CASE
            WHEN start_date IS NOT NULL
             AND end_date IS NOT NULL
             AND end_date >= start_date
            THEN DATEDIFF(
                'day',
                start_date,
                end_date
            )
            ELSE NULL
        END AS campaign_duration_days,


        budget,
        total_cost,
        total_revenue,

        /*
         * ROI is only normalized here.
         * ROI validation/calculation is performed in Gold
         * after attributed sales revenue becomes available.
         */

        roi_calculation,

        demographics,


        -- Source metadata

        _source_file,
        _loaded_at,
        _batch_id,
        source_snapshot_date

    FROM cleaned

),

deduplicated AS (

    SELECT *

    FROM transformed

    -- Remove duplicate campaign records

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY campaign_id
        ORDER BY
            source_snapshot_date DESC NULLS LAST,
            _loaded_at DESC
    ) = 1

)

SELECT

    campaign_id,
    campaign_name,

    start_date,
    end_date,
    campaign_duration_days,

    budget,
    total_cost,
    total_revenue,
    roi_calculation,

    demographics

FROM deduplicated