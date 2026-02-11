# Data Engineering Zoomcamp 2026 - Module 4 Homework

This repository contains the dbt models and SQL queries developed for the Module 4 Analytics Engineering assignment. The project focuses on NYC Taxi (Green and Yellow) and For-Hire Vehicle (FHV) trip data.

---

### Question 1: What is the behavior of `dbt build` when a model fails?
> **Answer:** int_trips_unioned only

### Question 2: What happens when you provide a variable via the command line like `dbt run --vars '{"is_test_run": false}'`?
> **Answer:** dbt fails the test with non-zero exit code.

### Question 3: Total Record Count
> **Query:** Verify the total number of records in the consolidated fact table.
> > **Answer:** 12,184

```sql
SELECT 
    COUNT(*) AS total_records 
FROM {{ ref('fct_monthly_zone_revenue') }};
```

### Question 4: Highest Revenue Zone (Green Taxis 2020)
> **Query:** Identify the zone with the highest total revenue for Green taxis in the year 2020.
> > **Answer:** East Harlem North
```sql
WITH monthly_revenue AS (
    SELECT 
        pickup_zone,
        revenue_monthly_total_amount,
        revenue_month
    FROM {{ ref('fct_monthly_zone_revenue') }}
    WHERE service_type = 'Green'
      AND EXTRACT(YEAR FROM revenue_month) = 2020 
)
SELECT 
    pickup_zone, 
    revenue_monthly_total_amount AS total_rev
FROM monthly_revenue
GROUP BY 1
ORDER BY total_rev DESC
LIMIT 1;
```


### Question 5: Trip Volume (Green Taxis Oct 2019)
> **Query:** Total number of trips for Green taxis in October 2019.
> > **Answer:** 384,624
```sql
SELECT 
    SUM(total_monthly_trips) AS total_trips
FROM {{ ref('fct_monthly_zone_revenue') }}
WHERE 
    service_type = 'Green'
    AND EXTRACT(MONTH FROM revenue_month) = 10
    AND EXTRACT(YEAR FROM revenue_month) = 2019;
```

### Question 6: FHV Staging Model
> **Requirements:** Create a staging model for 2019 FHV data, ensuring dispatching_base_num is not null and counting total records.
>> **Answer:** 43,244,693
```sql
{{ config(materialized='table') }}

SELECT
    -- identifiers
    dispatching_base_num,
    CAST(pulocationid AS integer) AS pickup_location_id,
    CAST(dolocationid AS integer) AS dropoff_location_id,
    
    -- timestamps
    CAST(pickup_datetime AS timestamp) AS pickup_datetime,
    CAST(dropoff_datetime AS timestamp) AS dropoff_datetime,
    
    -- trip info
    sr_flag,
    affiliated_base_number
FROM read_csv_auto([
    '[https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-01.csv.gz](https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-01.csv.gz)',
    '[https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-02.csv.gz](https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-02.csv.gz)',
    '[https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-03.csv.gz](https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-03.csv.gz)',
    '[https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-04.csv.gz](https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-04.csv.gz)',
    '[https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-05.csv.gz](https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-05.csv.gz)',
    '[https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-06.csv.gz](https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-06.csv.gz)',
    '[https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-07.csv.gz](https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-07.csv.gz)',
    '[https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-08.csv.gz](https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-08.csv.gz)',
    '[https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-09.csv.gz](https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-09.csv.gz)',
    '[https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-10.csv.gz](https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-10.csv.gz)',
    '[https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-11.csv.gz](https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-11.csv.gz)',
    '[https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-12.csv.gz](https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-12.csv.gz)'
])
WHERE dispatching_base_num IS NOT NULL;
```
#### Final record count verification
```sql
SELECT COUNT(*) FROM {{ ref('stg_fhv_tripdata') }};
```
