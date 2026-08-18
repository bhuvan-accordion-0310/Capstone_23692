{{ config(
    materialized = 'table'
) }}

-- Complete, gap-free calendar date dimension.
--
-- Date range mirrors the operational window of the source data
-- (2024-04-01 through 2024-09-27). dbt_utils.date_spine uses an
-- exclusive end date, so 2024-09-28 is supplied as the end bound.

WITH date_spine AS (

    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2024-04-01' as date)",
        end_date="cast('2024-09-28' as date)"
    ) }}

),

date_attributes AS (

    SELECT

        -- Date key
        -- YYYYMMDD format, e.g. 2024-04-01 -> 20240401

        TO_NUMBER(
            TO_CHAR(
                date_day,
                'YYYYMMDD'
            )
        ) AS date_key,

        date_day AS full_date,

        YEAR(date_day) AS year,
        QUARTER(date_day) AS quarter,
        MONTH(date_day) AS month,
        WEEK(date_day) AS week,

        DAYOFWEEK(date_day) AS day_of_week,
        DAYNAME(date_day) AS day_name,

        -- US holiday flag
        -- Holidays occurring inside the source data's date window in 2024:
        -- Memorial Day (05-27), Juneteenth (06-19),
        -- Independence Day (07-04), Labor Day (09-02)

        CASE

            WHEN date_day IN (
                DATE '2024-05-27',
                DATE '2024-06-19',
                DATE '2024-07-04',
                DATE '2024-09-02'
            )

            THEN TRUE

            ELSE FALSE

        END AS holiday_flag,

        -- Season (northern hemisphere)

        CASE

            WHEN MONTH(date_day) IN (12, 1, 2)
                THEN 'Winter'

            WHEN MONTH(date_day) IN (3, 4, 5)
                THEN 'Spring'

            WHEN MONTH(date_day) IN (6, 7, 8)
                THEN 'Summer'

            WHEN MONTH(date_day) IN (9, 10, 11)
                THEN 'Fall'

        END AS season

    FROM date_spine

)

SELECT

    date_key,
    full_date,
    year,
    quarter,
    month,
    week,
    day_of_week,
    day_name,
    holiday_flag,
    season

FROM date_attributes
