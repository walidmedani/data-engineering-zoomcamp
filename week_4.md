# Data Engineering Zoomcamp 2026 - Module 4 Homework

This repository contains the dbt models and SQL queries developed for the Module 4 Analytics Engineering assignment. The project focuses on NYC Taxi (Green and Yellow) and For-Hire Vehicle (FHV) trip data.

---

### Question 1: If you run dbt run --select int_trips_unioned, what models will be built?
> **Answer:** int_trips_unioned only
```sql
models/
├── staging/
│   ├── stg_green_tripdata.sql
│   └── stg_yellow_tripdata.sql
└── intermediate/
    └── int_trips_unioned.sql (depends on stg_green_tripdata & stg_yellow_tripdata)
```

### Question 2: You've configured a generic test like this in your schema.yml:
```sql
columns:
  - name: payment_type
    data_tests:
      - accepted_values:
          arguments:
            values: [1, 2, 3, 4, 5]
            quote: false
```
Your model fct_trips has been running successfully for months. A new value 6 now appears in the source data. What happens when you run dbt test --select fct_trips?
> **Answer:** dbt fails the test with non-zero exit code.

### Question 3: After running your dbt project, query the fct_monthly_zone_revenue model. What is the count of records in the fct_monthly_zone_revenue model?
> **Answer:** 12,184

```sql
SELECT 
    COUNT(*) AS total_records 
FROM {{ ref('fct_monthly_zone_revenue') }};
```

### Question 4: Using the fct_monthly_zone_revenue table, find the pickup zone with the highest total revenue (revenue_monthly_total_amount) for Green taxi trips in 2020. Which zone had the highest revenue?
> **Answer:** East Harlem North
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


### Question 5: Using the fct_monthly_zone_revenue table, what is the total number of trips (total_monthly_trips) for Green taxis in October 2019?
>  **Answer:** 384,624
```sql
SELECT 
    SUM(total_monthly_trips) AS total_trips
FROM {{ ref('fct_monthly_zone_revenue') }}
WHERE 
    service_type = 'Green'
    AND EXTRACT(MONTH FROM revenue_month) = 10
    AND EXTRACT(YEAR FROM revenue_month) = 2019;
```

### Question 6: Create a staging model for 2019 FHV data, ensuring dispatching_base_num is not null and counting total records. What is the count of records in stg_fhv_tripdata?
> **Answer:** 43,244,693
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
