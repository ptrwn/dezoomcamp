-- Creating external table referring to gcs path, Q2

CREATE OR REPLACE EXTERNAL TABLE `taxi_rides.external_green_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://mage-zoomcamp-myhwdisk/*.parquet']
);

-- Q1

SELECT count(1) FROM taxi_rides.external_green_tripdata;

-- Check data

SELECT * FROM taxi_rides.external_green_tripdata limit 10;

-- Materialize, Q2

CREATE OR REPLACE TABLE taxi_rides.green_tripdata_nonpart AS
SELECT * FROM taxi_rides.external_green_tripdata;

-- Create a partitioned table from external table

CREATE OR REPLACE TABLE taxi_rides.green_tripdata_part
PARTITION BY
  DATE(lpep_pickup_datetime) AS
SELECT * FROM taxi_rides.external_green_tripdata;


SELECT DISTINCT(PULocationID)
FROM taxi_rides.external_green_tripdata;

-- Q3

SELECT count(1)
FROM taxi_rides.green_tripdata_nonpart
where fare_amount=0;


-- Impact of partition

-- Scanning 1.6GB of data

SELECT DISTINCT(PULocationID)
FROM taxi_rides.green_tripdata_nonpart
-- WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2019-06-30';
;

SELECT DISTINCT(PULocationID)
FROM taxi_rides.green_tripdata_part;


-- Check the partitons

SELECT table_name, partition_id, total_rows
FROM `taxi_rides.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'green_tripdata_part'
ORDER BY total_rows DESC;



-- Create a partitioned and clustered table

CREATE OR REPLACE TABLE taxi_rides.green_tripdata_part_clust
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PUlocationID AS
SELECT * FROM taxi_rides.external_green_tripdata;

---- Q5

-- 12.82

SELECT DISTINCT(PULocationID)
FROM taxi_rides.green_tripdata_nonpart
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';

-- 1.12

SELECT DISTINCT(PULocationID)
FROM taxi_rides.green_tripdata_part_clust
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';

