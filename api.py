import pandas as pd
from fastapi import FastAPI
from pydantic import BaseModel
from typing import List

CUSTOMER_DATA_FILE = "olist_customers_dataset.csv"

try:
    customers_df = pd.read_csv(CUSTOMER_DATA_FILE)
    # Convert DataFrame to a list of dictionaries for Pydantic/JSON serialization
    customer_data = customers_df.to_dict('records')
    print(f"Loaded {len(customer_data)} customer records successfully.")
except FileNotFoundError:
    print(f"Error: {CUSTOMER_DATA_FILE} not found. Ensure the file is present.")
    customer_data = []
except Exception as e:
    print(f"An error occurred during data loading: {e}")
    customer_data = []

app = FastAPI(
    title="Olist Customer Data API",
    description="Serves raw customer data from the Olist dataset.",
    version="1.0.0"
)

class Customer(BaseModel):
    """Schema for a single customer record."""
    customer_id: str
    customer_unique_id: str
    customer_zip_code_prefix: int
    customer_city: str
    customer_state: str

@app.get("/", tags=["Root"])
def read_root():
    """Simple root endpoint check."""
    return {"message": "Welcome to the Olist Customer Data API. Access /customers to get the data."}

@app.get(
    "/customers",
    response_model=List[Customer],
    tags=["Data"]
)
def get_all_customers():
    """
    Returns the entire dataset of customer records as a JSON array.
    This simulates a simple data export/read API endpoint.
    """
    if not customer_data:
        return {"error": "Data could not be loaded."}, 500
    return customer_data
