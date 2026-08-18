{{ config(
    materialized = 'table'
) }}

-- Marketing performance fact.
-- Grain: one row per campaign per date.
--
-- There is no separate fact_sales gold model in this project, so
-- sales are sourced directly from silver_orders, which is already
-- aggregated to the order grain with a total_amount column.

WITH campaigns AS (

    SELECT

        campaign_key,
        campaign_id,
        budget,
        start_date,
        end_date

    FROM {{ ref('dim_marketing_campaign') }}

),

dates AS (

    SELECT

        date_key,
        full_date

    FROM {{ ref('dim_date') }}

),

-- 1. SALES BASE

sales_base AS (

    SELECT

        order_id,

        {{ dbt_utils.generate_surrogate_key([
            'campaign_id'
        ]) }} AS campaign_key,

        TO_NUMBER(
            TO_CHAR(
                order_date,
                'YYYYMMDD'
            )
        ) AS date_key,

        {{ dbt_utils.generate_surrogate_key([
            'customer_id'
        ]) }} AS customer_key,

        customer_id,
        campaign_id,
        order_date,

        COALESCE(
            total_amount,
            0.00
        ) AS total_sales_amount

    FROM {{ ref('silver_orders') }}

    WHERE order_id IS NOT NULL
      AND campaign_id IS NOT NULL

),

-- 2. CUSTOMER PURCHASE HISTORY
-- Determine the customer's first-ever purchase date, needed to
-- identify first vs repeat purchases.

customer_purchase_history AS (

    SELECT

        customer_id,

        MIN(order_date) AS first_purchase_date

    FROM sales_base

    WHERE customer_id IS NOT NULL

    GROUP BY

        customer_id

),

-- 3. SALES WITH PURCHASE FLAGS

fact_sales AS (

    SELECT

        s.order_id,

        s.campaign_key,
        s.date_key,
        s.customer_key,

        s.customer_id,
        s.campaign_id,
        s.order_date,

        s.total_sales_amount,

        CASE

            WHEN s.order_date = h.first_purchase_date

            THEN TRUE

            ELSE FALSE

        END AS is_first_purchase,

        CASE

            WHEN s.order_date > h.first_purchase_date

            THEN TRUE

            ELSE FALSE

        END AS is_repeat_purchase

    FROM sales_base s

    LEFT JOIN customer_purchase_history h
        ON s.customer_id = h.customer_id

),

-- 4. CAMPAIGN CUSTOMER POPULATION
-- Grain: one row per campaign per customer

campaign_customers AS (

    SELECT DISTINCT

        c.campaign_key,
        c.campaign_id,

        fs.customer_key,
        fs.customer_id,

        c.start_date,
        c.end_date

    FROM campaigns c

    INNER JOIN fact_sales fs
        ON fs.campaign_key = c.campaign_key
       AND fs.order_date BETWEEN c.start_date AND c.end_date

    WHERE fs.customer_key IS NOT NULL

),

-- 5. CUSTOMER-LEVEL CAMPAIGN PURCHASE STATUS
-- Grain: one row per campaign per customer

campaign_customer_status AS (

    SELECT

        cc.campaign_key,
        cc.customer_key,

        MAX(
            CASE
                WHEN fs.is_first_purchase = TRUE THEN 1
                ELSE 0
            END
        ) AS is_first_purchase,

        MAX(
            CASE
                WHEN fs.is_repeat_purchase = TRUE THEN 1
                ELSE 0
            END
        ) AS is_repeat_purchase

    FROM campaign_customers cc

    INNER JOIN fact_sales fs
        ON fs.campaign_key = cc.campaign_key
       AND fs.customer_key = cc.customer_key
       AND fs.order_date BETWEEN cc.start_date AND cc.end_date

    GROUP BY

        cc.campaign_key,
        cc.customer_key

),

-- 6. CAMPAIGN CUSTOMER METRICS

campaign_customer_metrics AS (

    SELECT

        campaign_key,

        COUNT(DISTINCT customer_key) AS total_campaign_customers,

        COUNT(
            DISTINCT CASE
                WHEN is_first_purchase = 1 THEN customer_key
                ELSE NULL
            END
        ) AS first_purchase_customers,

        COUNT(
            DISTINCT CASE
                WHEN is_repeat_purchase = 1 THEN customer_key
                ELSE NULL
            END
        ) AS repeat_purchase_customers

    FROM campaign_customer_status

    GROUP BY

        campaign_key

),

-- 7. CAMPAIGN / DATE GRAIN

campaign_dates AS (

    SELECT

        c.campaign_key,
        c.campaign_id,

        c.budget,

        c.start_date,
        c.end_date,

        d.date_key,
        d.full_date

    FROM campaigns c

    INNER JOIN dates d
        ON d.full_date BETWEEN c.start_date AND c.end_date

),

-- 8. TOTAL SALES INFLUENCED BY CAMPAIGN

sales_influenced AS (

    SELECT

        cd.campaign_key,
        cd.date_key,

        COALESCE(
            SUM(fs.total_sales_amount),
            0.00
        ) AS total_sales_influenced

    FROM campaign_dates cd

    LEFT JOIN fact_sales fs
        ON fs.campaign_key = cd.campaign_key
       AND fs.date_key = cd.date_key
       AND fs.order_date BETWEEN cd.start_date AND cd.end_date

    GROUP BY

        cd.campaign_key,
        cd.date_key

),

-- 9. NEW CUSTOMERS ACQUIRED

new_customers AS (

    SELECT

        cd.campaign_key,
        cd.date_key,

        COUNT(
            DISTINCT CASE

                WHEN fs.customer_key IS NOT NULL
                 AND fs.order_date BETWEEN cd.start_date AND cd.end_date
                 AND NOT EXISTS (

                     SELECT 1

                     FROM fact_sales prior

                     WHERE prior.customer_key = fs.customer_key
                       AND prior.order_date < cd.start_date

                 )

                THEN fs.customer_key

                ELSE NULL

            END
        ) AS new_customers_acquired

    FROM campaign_dates cd

    LEFT JOIN fact_sales fs
        ON fs.campaign_key = cd.campaign_key
       AND fs.date_key = cd.date_key

    GROUP BY

        cd.campaign_key,
        cd.date_key

),

-- 10. FINAL METRIC ASSEMBLY

metrics AS (

    SELECT

        cd.campaign_key,
        cd.campaign_id,

        cd.date_key,
        cd.full_date,

        cd.budget,

        COALESCE(si.total_sales_influenced, 0.00) AS total_sales_influenced,
        COALESCE(nc.new_customers_acquired, 0) AS new_customers_acquired,
        COALESCE(ccm.total_campaign_customers, 0) AS total_campaign_customers,
        COALESCE(ccm.first_purchase_customers, 0) AS first_purchase_customers,
        COALESCE(ccm.repeat_purchase_customers, 0) AS repeat_purchase_customers

    FROM campaign_dates cd

    LEFT JOIN sales_influenced si
        ON cd.campaign_key = si.campaign_key
       AND cd.date_key = si.date_key

    LEFT JOIN new_customers nc
        ON cd.campaign_key = nc.campaign_key
       AND cd.date_key = nc.date_key

    LEFT JOIN campaign_customer_metrics ccm
        ON cd.campaign_key = ccm.campaign_key

),

-- 11. FINAL FACT
-- Grain: one row per campaign per date

final AS (

    SELECT

        {{ dbt_utils.generate_surrogate_key([
            'campaign_key',
            'date_key'
        ]) }} AS marketing_performance_key,

        campaign_key,
        date_key,

        campaign_id,
        full_date,

        total_sales_influenced,
        new_customers_acquired,

        -- Repeat purchase rate: share of campaign-influenced
        -- customers who made a repeat purchase

        CASE

            WHEN total_campaign_customers > 0

            THEN
                100.0
                * repeat_purchase_customers
                / NULLIF(total_campaign_customers, 0)

            ELSE NULL

        END AS repeat_purchase_rate,

        budget AS total_campaign_cost,

        -- ROI

        CASE

            WHEN budget > 0

            THEN
                (total_sales_influenced - budget)
                / budget * 100

            ELSE NULL

        END AS roi

    FROM metrics

)

SELECT

    marketing_performance_key,

    campaign_key,
    date_key,

    campaign_id,
    full_date,

    total_sales_influenced,
    new_customers_acquired,

    repeat_purchase_rate,

    total_campaign_cost,
    roi

FROM final
