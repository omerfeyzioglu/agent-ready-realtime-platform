{{ config(location=staging_root() ~ '/holidays/data.parquet') }}

-- Grain: one row per calendar date that counts as a holiday.
-- A holiday on a weekend is observed on a weekday; both dates behave
-- unlike a normal day, so both are listed.

with src as (
    select "date"::date as actual_date, observed_date::date as observed_date, name
    from {{ source('source_files', 'us_federal_holidays') }}
),

dates as (
    select actual_date as holiday_date, name as holiday_name, false as is_observed_only from src
    union all
    select observed_date, name, true from src where observed_date <> actual_date
)

-- New Year's Day 2023 is observed on Jan 2; keep one row per date
select holiday_date, any_value(holiday_name) as holiday_name, bool_and(is_observed_only) as is_observed_only
from dates
group by holiday_date
