#we start by importing our library

import requests
from dagster_essentials.assets import constants
import dagster as dg

import duckdb
import os
from dagster._utils.backoff import backoff

#now we define our first function with no input and returns nothing
@dg.asset
def taxi_trips_file() -> None:
    """
        The raw parquet files for our taxi trip dataset. Sourced from the NYC open data portal.
    """
    month_to_fetch = '2023-03'
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


@dg.asset

def ai_adopt() -> None:

    url = ("https://www.kaggle.com/datasets/dakshbhatnagar08/ai-tools-usage-among-global-high-school-students/ai_adoption_by_country.csv/download")
    response = requests.get(url)

    if response.status_code != 200:
        print (f'error: {response.status_code}')
    else:
        with open('data/raw/ai_data_by_country.csv', "wb") as output_file:
            output_file.write(response.content)


@dg.asset

def ai_adoption() -> None:

    """
    here i write my script to extract data from kaggle website
    """

    url = "https://www.kaggle.com/datasets/dakshbhatnagar08/ai_adoption_by_country.csv/download"
    response = requests.get(url)

    if response.status_code != 200:
        print(f"error, read error code, {response.status_code}")
    else:
        with open(constants.AI_ADOPTION_FILE_PATH, "wb") as output_file:
            output_file.write(response.content)