{{
    config(
        materialized='view'
    )
}}

with tripdata as 
(
  select *,
    -- row_number() over(partition by vendorid, lpep_pickup_datetime) as rn
  from {{ source('staging','external_fhv') }}
  where date(pickup_datetime)  BETWEEN '2019-01-01' AND '2019-12-31'
--   where vendorid is not null 
)
select
    -- identifiers
    {{ dbt.safe_cast("PUlocationID", api.Column.translate_type("integer")) }} as pickup_locationid,
    {{ dbt.safe_cast("DOlocationID", api.Column.translate_type("integer")) }} as dropoff_locationid,
    
    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp) as dropoff_datetime,

from tripdata
-- where rn = 1


-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
-- {% if var('is_test_run', default=true) %}

--   limit 100

-- {% endif %}