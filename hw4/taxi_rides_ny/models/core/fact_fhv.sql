{{
    config(
        materialized='table'
    )
}}

with fhv_tripdata as (
    select * from {{ ref('stg_external_fhv') }}
), 
dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown' or borough != 'N/A'
)
select
    fhv_tripdata.pickup_locationid as pickup_locationid,
    fhv_tripdata.dropoff_locationid as dropoff_locationid,
    fhv_tripdata.pickup_datetime,
    fhv_tripdata.dropoff_datetime,
    pickup_zone.borough as pickup_borough,
    pickup_zone.zone as pickup_zone, 
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone
from fhv_tripdata
inner join dim_zones as pickup_zone
on fhv_tripdata.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on fhv_tripdata.dropoff_locationid = dropoff_zone.locationid