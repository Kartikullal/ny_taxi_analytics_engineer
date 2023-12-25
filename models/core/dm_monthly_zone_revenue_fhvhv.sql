{{ config(materialized='table') }}

with trips_data as (
    select * from {{ ref('fact_trips_fhvhv') }}
)
    select 
    -- Reveneue grouping 
    pickup_zone as revenue_zone,
    date_trunc(pickup_datetime, month) as revenue_month,  

    company_name, 

    -- Revenue calculation 
    sum(passenger_fare_amount) as revenue_fhvhv_passenger_monthly_fare,
    sum(bcf) as revenue_monthly_bcf,
    sum(sales_tax) as revenue_monthly_fhvhv_sales_tax,
    sum(tip_amount) as revenue_monthly_fhvhv_tip_amount,
    sum(tolls_amount) as revenue_monthly_fhvhv_tolls_amount,
    sum(airport_fee) as revenue_monthly_fhvhv_airport_fee,
    sum(driver_pay) as revenue_monthly_fhvhv_driver_pay,
    sum(total_amount) as revenue_monthly_fhvhv_total_amount,
    sum(congestion_surcharge) as revenue_monthly_fhvhv_congestion_surcharge,
    sum(company_pay) as revenue_monthly_fhvhv_company_pay,

    -- Additional calculations
    count(tripid) as total_monthly_trips,
    avg(trip_distance) as avg_montly_trip_distance

    from trips_data
    group by 1,2,3