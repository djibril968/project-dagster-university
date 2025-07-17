#we start by importing our library

import numpy as np
import pandas as pd
import requests
from dagster_essentials.assets import constants
import dagster as dg
#import duckdb
from dagster_duckdb import DuckDBResource
import os
#from dagster._utils.backoff import backoff
from dagster_essentials.partitions import monthly_partitions, weekly_partitions  

#now we define our first function with no input and returns nothing
@dg.asset(
    partitions_def=monthly_partitions,
)
def taxi_trips_file(context: dg.AssetExecutionContext) -> None:
    """
        The raw parquet files for our taxi trip dataset. Sourced from the NYC open data portal.
    """
    partition_date_str = context.partition_key
    month_to_fetch = partition_date_str[:-3]
    #month_to_fetch = monthly_partitions.get_partition_key_from_datetime(
    #    pd.Timestamp.now(tz="America/New_York"))
    raw_trips = requests.get (
        f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month_to_fetch}.parquet"

    ) #this line retrieves the data from the source

    with open(constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch), "wb") as output_file:
        output_file.write(raw_trips.content)



@dg.asset
def taxi_zones_file() -> None:
    """
        The raw parquet files for our taxi zone dataset. Sourced from the NYC open data portal.
    """
    raw_taxi_zones = requests.get (
        "https://community-engineering-artifacts.s3.us-west-2.amazonaws.com/dagster-university/data/taxi_zones.csv"
    )

    #next we use the syntax below to write to file

    with open(constants.TAXI_ZONES_FILE_PATH, 'wb') as output_file: 
        output_file.write(raw_taxi_zones.content)


#here we define our asset that will load the taxi trip data into our database

@dg.asset (
    deps = ["taxi_trips_file"],
    #required_resource_keys={"database"}
)

def taxi_trips(database: DuckDBResource) -> None:
    """
    here we load our taxi trip data into our db, duckdb as in this instance
    """
    query = """
        create or replace table tax_trips as(
            select
                VendorID as vendor_id,
                PULocationID as pickup_zone_id,
                DOLocationID as dropoff_zone_id,
                RatecodeID as rate_code_id,
                payment_type as payment_type,
                tpep_dropoff_datetime as dropoff_datetime,
                tpep_pickup_datetime as pickup_datetime,
                trip_distance as trip_distance,
                passenger_count as passenger_count,
                total_amount as total_amount
            from 'data/raw/taxi_trips_2023-03.parquet'  
            );
    """

    with database.get_connection() as conn: 
         conn.execute(query)

    #conn = backoff(
     #   fn=duckdb.connect,
      #  retry_on=(RuntimeError, duckdb.IOException),
       # kwargs={
        #    "database": os.getenv("DUCKDB_DATABASE"),
        #},
        #max_retries= 10,
    #)
    #conn.execute(query)

# this asset will load the taxi zone data into our db, duckdb as in this instance

@dg.asset(
        deps = ["taxi_zones_file"]
        #required_resource_keys={"database"}
         )    
def zones(database: DuckDBResource) -> None:

        query = """
            create or replace table zones as(
                select
                    LocationID as zone_id,
                    zone as zone,
                    borough as borough,
                    the_geom as geometry
                from 'data/raw/taxi_zones.csv'

            );
        """
        with database.get_connection() as conn:
            conn.execute(query)
        #conn = backoff(
         #   fn = duckdb.connect,
          #  retry_on=(RuntimeError, duckdb.IOException),
           # kwargs={
            #    "database": os.getenv("DUCKDB_DATABASE"),
            #},
            #max_retries= 10,
        #)
        #conn.execute(query)


