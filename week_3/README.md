SQL code for homework

#1
SELECT COUNT(*) FROM `rides_dataset.yellow_taxi_data`;

#2
SELECT COUNT(DISTINCT pu_location_id) FROM `rides_dataset.yellow_taxi_data`;
SELECT COUNT(DISTINCT pu_location_id) FROM `rides_dataset.external_yellow_taxi`;

#3
SELECT pu_location_id FROM `rides_dataset.yellow_taxi_data`;
SELECT pu_location_id, do_location_id FROM `rides_dataset.yellow_taxi_data`;

#4
SELECT COUNT(*)
FROM `rides_dataset.yellow_taxi_data`
WHERE fare_amount = 0;

#5
CREATE OR REPLACE TABLE `rides_dataset.yellow_taxi_partitioned`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY vendor_id AS
SELECT * FROM `rides_dataset.external_yellow_taxi`;

#6
SELECT DISTINCT (vendor_id), tpep_dropoff_datetime
FROM `rides_dataset.yellow_taxi_data`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';

SELECT DISTINCT (vendor_id), tpep_dropoff_datetime
FROM `rides_dataset.yellow_taxi_partitioned`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';

#9
SELECT COUNT(*) FROM `rides_dataset.yellow_taxi_data`;