#!/usr/bin/env python
# coding: utf-8

from sqlalchemy import create_engine
import pandas as pd
from tqdm.auto import tqdm
import click

# Keep your strict Yellow Taxi schema here
YELLOW_TAXI_DTYPES = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64",
}

YELLOW_TAXI_PARSE_DATES = ["tpep_pickup_datetime", "tpep_dropoff_datetime"]


@click.command()
@click.option("--pg-user", default="root")
@click.option("--pg-pass", default="root")
@click.option("--pg-host", default="localhost")
@click.option("--pg-port", default=5432, type=int)
@click.option("--pg-db", default="ny_taxi")
@click.option("--target-table", required=True)
@click.option("--url", required=True)
@click.option("--chunksize", default=100000, type=int)
def run(pg_user, pg_pass, pg_host, pg_port, pg_db, target_table, url, chunksize):
    engine = create_engine(
        f"postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}"
    )

    # --- LOGIC TO SELECT DTYPES ---
    # We only apply the strict types if we are targeting the yellow taxi table
    if target_table == "yellow_taxi_data":
        print("Applying strict schema for Yellow Taxi data...")
        specific_dtypes = YELLOW_TAXI_DTYPES
        specific_parse_dates = YELLOW_TAXI_PARSE_DATES
    else:
        print(
            f"Generic table '{target_table}' detected. Using Pandas auto-inference..."
        )
        specific_dtypes = None
        specific_parse_dates = None
    # ------------------------------

    df_iter = pd.read_csv(
        url,
        dtype=specific_dtypes,
        parse_dates=specific_parse_dates,
        iterator=True,
        chunksize=chunksize,
        low_memory=False,
    )

    first = True

    for df_chunk in tqdm(df_iter):
        if first:
            # Create the table schema
            df_chunk.head(0).to_sql(name=target_table, con=engine, if_exists="replace")
            first = False

        df_chunk.to_sql(name=target_table, con=engine, if_exists="append")

    print(f"Finished ingesting data into table: {target_table}")


if __name__ == "__main__":
    run()
