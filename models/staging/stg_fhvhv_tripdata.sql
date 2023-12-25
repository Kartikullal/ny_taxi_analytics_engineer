with tripdata as 
(
  select *,
    row_number() over(partition by hvfhs_license_num, dispatching_base_num,request_datetime,pickup_datetime) as rn
  from `bubbly-domain-408520`.`ny_taxi`.`fhvhv_taxi_rides`
  where hvfhs_license_num is not null
)
select
    -- identifiers
    to_hex(md5(cast(coalesce(cast(hvfhs_license_num as 
    string
), '') || '-' || coalesce(cast(dispatching_base_num as 
    string
), '') || '-' || coalesce(cast(request_datetime as 
    string
), '') || '-' || coalesce(cast(pickup_datetime as 
    string
), '') as 
    string
))) as tripid,
    cast(hvfhs_license_num as string) as hvfhs_license_num,
    case hvfhs_license_num
        when 'HV0002' then 'Juno'
        when 'HV0003' then 'Uber'
        when 'HV0004' then 'Via'
        when 'HV0005' then 'Lyft'
    end as company_name, 
    cast(dispatching_base_num as string) as dispatching_base_num,
    cast(originating_base_num as string) as originating_base_num,
    cast(PULocationID as integer) as  pickup_locationid,
    cast(DOLocationID as integer) as dropoff_locationid,
    
    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,
    cast(request_datetime as timestamp) as request_datetime,
    cast(on_scene_datetime as timestamp) as on_scene_datetime,
    
    -- trip info
    cast(trip_miles as numeric) as trip_distance,
    cast(trip_time as integer) as trip_time, --in seconds
    shared_request_flag, -- shared trip requested or not
    shared_match_flag,  -- shared trip matched or not
    access_a_ride_flag,  -- Was the trip administered on behalf of the (MTA)
    wav_request_flag,  -- Did the passenger request a wheelchair-accessible vehicle (WAV)
    wav_match_flag, -- Did the trip occur in a wheelchair-accessible vehicle (WAV)? 

    -- payment info
    cast(base_passenger_fare as numeric) as passenger_fare_amount,
    cast(bcf as numeric) as bcf, -- total amount collected in trip for Black Car Fund
    cast(sales_tax as numeric) as sales_tax,
    cast(tips as numeric) as tip_amount,
    cast(tolls as numeric) as tolls_amount,
    cast(congestion_surcharge as numeric) as congestion_surcharge,
    cast(COALESCE(airport_fee,0) as numeric) as airport_fee,
    cast((base_passenger_fare + bcf + sales_tax + tips + tolls + congestion_surcharge + COALESCE(airport_fee,0) ) as numeric) as total_amount,
    cast(driver_pay as numeric) as driver_pay,
    cast((base_passenger_fare - driver_pay) as numeric) as company_pay
from tripdata
where rn = 1



-- dbt build --m <model.sql> --var 'is_test_run: false'


  limit 100