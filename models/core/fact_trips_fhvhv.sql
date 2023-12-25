{{ config(materialized='table') }}


with dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select
    trips.tripid,
    trips.hvfhs_license_num,
    trips.company_name, 
    trips.dispatching_base_num,
    trips.originating_base_num,
    trips.pickup_locationid,
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    trips.dropoff_locationid,
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,  
    trips.pickup_datetime,
    trips.dropoff_datetime,
    trips.request_datetime,
    trips.on_scene_datetime,
    trips.trip_distance,
    trips.trip_time, --in seconds
    trips.shared_request_flag, -- shared trip requested or not
    trips.shared_match_flag,  -- shared trip matched or not
    trips.access_a_ride_flag,  -- Was the trip administered on behalf of the (MTA)
    trips.wav_request_flag,  -- Did the passenger request a wheelchair-accessible vehicle (WAV)
    trips.wav_match_flag, -- Did the trip occur in a wheelchair-accessible vehicle (WAV)? 
    trips.passenger_fare_amount,
    trips.bcf, -- total amount collected in trip for Black Car Fund
    trips.sales_tax,
    trips.tip_amount,
    trips.tolls_amount,
    trips.congestion_surcharge,
    trips.airport_fee,
    trips.total_amount,
    trips.driver_pay,
    trips.company_pay
from {{ ref('stg_fhvhv_tripdata') }} as trips
inner join dim_zones as pickup_zone
on trips.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on trips.dropoff_locationid = dropoff_zone.locationid