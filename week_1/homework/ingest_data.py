import pandas as pd
from sqlalchemy import create_engine

engine = create_engine("postgresql://root:root@localhost:5432/ny_taxi")


print("Reading parquet file...")
df = pd.read_parquet("green_tripdata_2025-11.parquet")


print("Inserting data into Postgres...")
df.to_sql(name="green_taxi_trips", con=engine, if_exists="replace", index=False)


df_zones = pd.read_csv("taxi_zone_lookup.csv")
df_zones.to_sql(name="zones", con=engine, if_exists="replace", index=False)

print("Finished!")
