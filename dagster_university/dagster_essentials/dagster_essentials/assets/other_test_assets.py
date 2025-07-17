

import dagster as dg
import requests
import json
import pandas as pd

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
        #data = response.content
        # we write the data to a file
        #pd.DataFrame(data).to_csv("data/raw/ai_adoption.csv", index=False)
        with open("data/raw/ai_adoption.csv", "wb") as output_file:
            output_file.write(response.content)




@dg.asset

def ev_data () -> None:

    url = "https://data.wa.gov/api/views/f6w7-q2d2/rows.csv?accessType=DOWNLOAD"
    response = requests.get(url)

    if response.status_code != 200:
        print(f"error, {response.status_code}")
    else:
        with open("data/raw/ev_data.csv", "wb") as output_file:
            output_file.write(response.content)

@dg.asset

def ai_adopt() -> None:

    url = ("https://www.kaggle.com/datasets/dakshbhatnagar08/ai-tools-usage-among-global-high-school-students/ai_adoption_by_country.csv/download")
    response = requests.get(url)

    if response.status_code != 200:
        print (f'error: {response.status_code}')
    else:
        with open('data/raw/ai_data_by_country.csv', "wb") as output_file:
            output_file.write(response.content)
