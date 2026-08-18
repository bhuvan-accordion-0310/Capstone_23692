{{ config(
    materialized = 'table'
) }}

-- Marketing campaign dimension.
--
-- NOTE: silver_campaigns.sql has no separate "campaign_type" or
-- "target_audience_segmentation" column - it has a single
-- "demographics" field. That field is used here as the audience
-- segment. The final attributed-sales ROI is calculated in
-- fact_marketing_performance; roi_calculation from Silver is kept
-- as the source ROI for reference.

WITH campaigns AS (

    SELECT

        campaign_id,
        campaign_name,
        demographics,
        budget,
        total_cost,
        total_revenue,
        campaign_duration_days,
        roi_calculation,
        start_date,
        end_date

    FROM {{ ref('silver_campaigns') }}

),

final AS (

    SELECT

        -- Surrogate key generated from the natural Campaign ID

        {{ dbt_utils.generate_surrogate_key([
            'campaign_id'
        ]) }} AS campaign_key,

        campaign_id,

        campaign_name,

        demographics AS target_audience_segment,

        budget,
        total_cost,
        total_revenue,

        campaign_duration_days AS duration,

        roi_calculation AS roi,

        start_date,
        end_date

    FROM campaigns

)

SELECT *

FROM final
