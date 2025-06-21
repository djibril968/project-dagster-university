import dagster as dg
import requests


# the scripts contained here is used to practice how to pull data from various sources into our working directory

@dg.asset

def ai_adopt() -> None:

    url = ("https://www.kaggle.com/datasets/dakshbhatnagar08/ai-tools-usage-among-global-high-school-students/ai_adoption_by_country.csv/download")
    response = requests.get(url)

    if response.status_code != 200:
        print (f'error: {response.status_code}')
    else:
        with open('ai_data_by_country.csv', "wb") as output_file:
            output_file.write(response.content)
  


