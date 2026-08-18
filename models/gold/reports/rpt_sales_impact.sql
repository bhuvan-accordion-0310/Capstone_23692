{{ config(
    materialized = 'view'
) }}

SELECT

    fmp.campaign_key,
    dmc.campaign_id,
    dmc.target_audience_segment,

    fmp.date_key,
    dd.full_date,

    fmp.total_sales_influenced,
    fmp.total_campaign_cost,
    fmp.roi

FROM {{ ref('fact_marketing_performance') }} fmp

LEFT JOIN {{ ref('dim_marketing_campaign') }} dmc
    ON fmp.campaign_key = dmc.campaign_key

LEFT JOIN {{ ref('dim_date') }} dd
    ON fmp.date_key = dd.date_key
