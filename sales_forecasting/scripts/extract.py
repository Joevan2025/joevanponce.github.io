import json
import os

def load_credentials(cred_path=r"C:\Users\ADMIN\projects\etl\access_tokens\.kaggle\kaggle.json"):
    with open(cred_path, "r") as f:
        creds = json.load(f)
    os.environ["KAGGLE_USERNAME"] = creds["username"]
    os.environ["KAGGLE_KEY"] = creds["key"]

# Must load credentials BEFORE importing Kaggle
load_credentials()

from kaggle.api.kaggle_api_extended import KaggleApi

def extract(dataset="rohitsahoo/sales-forecasting",
            output_path=r"C:\Users\ADMIN\projects\etl\sales_forecasting\raw_data"):

    api = KaggleApi()
    api.authenticate()

    print(f"Downloading: {dataset}")
    api.dataset_download_files(dataset, path=output_path, unzip=True)

    print(f"Done! Files saved to: {output_path}")

if __name__ == "__main__":
    extract()