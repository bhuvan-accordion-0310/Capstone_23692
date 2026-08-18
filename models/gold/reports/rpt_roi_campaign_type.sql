{{ config(
    materialized = 'view'
) }}

-- NOTE: this project's source data has no explicit "campaign_type"
-- column - dim_marketing_campaign.target_audience_segment
-- (sourced from the campaign's demographics field) is used as the
-- grouping dimension instead.

SELECT

    dmc.target_audience_segment,

    COUNT(DISTINCT fmp.campaign_key) AS campaign_count,

    SUM(fmp.total_sales_influenced) AS total_sales_influenced,
    SUM(fmp.total_campaign_cost) AS total_campaign_cost,

    AVG(fmp.roi) AS average_roi,

    CASE

        WHEN SUM(fmp.total_campaign_cost) > 0

        THEN
            (
                SUM(fmp.total_sales_influenced)
                - SUM(fmp.total_campaign_cost)
            )
            / SUM(fmp.total_campaign_cost)
            * 100

        ELSE NULL

    END AS calculated_roi

FROM {{ ref('fact_marketing_performance') }} fmp

LEFT JOIN {{ ref('dim_marketing_campaign') }} dmc
    ON fmp.campaign_key = dmc.campaign_key

GROUP BY

    dmc.target_audience_segment
