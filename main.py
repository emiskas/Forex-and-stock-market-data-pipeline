import json
import os
from datetime import datetime

import requests
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Alpha Vantage API Key
API_KEY = os.getenv("API_KEY")

# Azure Storage Account details
AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
CONTAINER_NAME = os.getenv("CONTAINER_NAME")

# Initialize Azure Blob Storage client
blob_service_client = BlobServiceClient.from_connection_string(
    AZURE_STORAGE_CONNECTION_STRING
)


def fetch_stock_data(symbol="MSFT"):
    """Fetches stock data from Alpha Vantage API for the given symbol."""
    payload = {
        "function": "TIME_SERIES_INTRADAY",
        "symbol": symbol,
        "apikey": API_KEY,
        "interval": "60min",
    }
    url = "https://www.alphavantage.co/query?"
    response = requests.get(url, params=payload)

    if response.status_code == 200:
        return response.json()
    else:
        print("Error fetching data:", response.status_code, response.text)
        return None


def save_data_to_azure(data, symbol):
    """Saves the fetched stock data to Azure Data Lake as JSON."""
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    blob_name = f"{symbol}_stock_data_{timestamp}.json"

    # Convert data to JSON
    json_data = json.dumps(data, indent=4)

    # Upload to Azure Blob Storage
    blob_client = blob_service_client.get_blob_client(
        container=CONTAINER_NAME, blob=blob_name
    )
    blob_client.upload_blob(json_data, overwrite=True)

    print(f"Data successfully uploaded to Azure Data Lake as {blob_name}")


# Fetch stock data and upload to Azure
stock_symbol = "MSFT"
stock_data = fetch_stock_data(stock_symbol)

print(stock_data)

if stock_data:
    save_data_to_azure(stock_data, stock_symbol)
