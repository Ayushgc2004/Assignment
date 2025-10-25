import pandas as pd
from sqlalchemy import create_engine
from prefect import task
import numpy as np

DB_USER = "postgres"
DB_PASSWORD = "1234" 
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "olist_analysis"
DB_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
ENGINE = create_engine(DB_URL)

FILE_CUSTOMERS = "olist_customers_dataset.csv"
FILE_ORDERS = "olist_orders_dataset.csv"
FILE_ITEMS = "olist_order_items_dataset.csv"
FILE_PAYMENTS = "olist_order_payments_dataset.csv"

@task(name="Read and Clean Data Source")
def read_data_task(file_path: str) -> pd.DataFrame:
    """Reads CSV, cleans column names, and converts date columns."""
    print(f"Reading {file_path}...")
    try:
        df = pd.read_csv(file_path)

        # Standardize and clean column names
        df.columns = df.columns.str.lower().str.replace('[^a-zA-Z0-9_]', '', regex=True)

        # Convert timestamps
        for col in df.columns:
            if 'date' in col or 'timestamp' in col:
                df[col] = pd.to_datetime(df[col], errors='coerce')

        print(f"Read {len(df)} rows from {file_path}.")
        return df
    except Exception as e:
        print(f"ERROR reading {file_path}: {e}")
        return pd.DataFrame()

@task(name="Merge All Olist Datasets")
def merge_data_task(df_cust, df_orders, df_items, df_payments) -> pd.DataFrame:
    """Performs left joins to create a wide analytical dataset."""
    if any(df.empty for df in [df_cust, df_orders, df_items, df_payments]):
        raise ValueError("One or more input DataFrames are empty. Cannot merge.")

    print("Starting data merge...")

    # 1. Customers and Orders (on customer_id)
    df_merged = pd.merge(df_orders, df_cust, on='customer_id', how='left')

    # 2. Add Order Items (on order_id) - includes price/freight
    df_merged = pd.merge(df_merged, df_items.drop(columns=['shipping_limit_date']), on='order_id', how='left')

    # 3. Aggregate and Add Payments
    df_payments_agg = df_payments.groupby('order_id').agg(
        total_payment_value=('payment_value', 'sum'),
        payment_installments_max=('payment_installments', 'max'),
        payment_type_most_common=('payment_type', lambda x: x.mode()[0] if not x.mode().empty else np.nan)
    ).reset_index()

    df_merged = pd.merge(df_merged, df_payments_agg, on='order_id', how='left')


    df_merged['total_payment_value'] = df_merged['total_payment_value'].fillna(0)
    df_merged['price'] = df_merged['price'].fillna(0)
    df_merged['freight_value'] = df_merged['freight_value'].fillna(0)

    print(f"Data merge complete. Final size: {len(df_merged)} rows.")
    return df_merged

@task(name="Load DataFrame to PostgreSQL")
def load_to_postgres_task(df: pd.DataFrame, table_name: str):
    """Generic function to load a DataFrame into a PostgreSQL table."""
    if df.empty:
        print(f"WARNING: DataFrame for {table_name} is empty. Skipping load.")
        return

    print(f"Loading {len(df)} rows into table '{table_name}'...")
    try:
        df.to_sql(table_name, ENGINE, if_exists='replace', index=False)
        print(f"Successfully loaded data into '{table_name}'.")
    except Exception as e:
        print(f"ERROR loading data into {table_name}: {e}")

@task(name="Create Sales Summary Table")
def create_sales_summary_task(df_merged: pd.DataFrame) -> pd.DataFrame:
    """Creates the Sales Summary (by customer & region) table."""
    print("Creating Sales Summary...")

    # Use order_id to count unique orders, total_payment_value for spending
    df_sales_summary = df_merged.groupby(['customer_unique_id', 'customer_state', 'customer_city']).agg(
        total_spent=('total_payment_value', 'sum'),
        total_orders=('order_id', 'nunique'),
        avg_payment_installments=('payment_installments_max', 'mean')
    ).reset_index()

    # Calculate Avg Order Value
    df_sales_summary['avg_order_value'] = df_sales_summary['total_spent'] / df_sales_summary['total_orders']

    return df_sales_summary

@task(name="Create Delivery Performance Summary Table")
def create_delivery_summary_task(df_merged: pd.DataFrame) -> pd.DataFrame:
    """Creates the Delivery Performance Summary table."""
    print("Creating Delivery Performance Summary...")

    df_delivered = df_merged[df_merged['order_status'] == 'delivered'].copy()

    # Calculate delivery time in days
    df_delivered['delivery_time_days'] = (
        df_delivered['order_delivered_customer_date'] - df_delivered['order_purchase_timestamp']
    ).dt.total_seconds() / (60 * 60 * 24)

    # Calculate Service Level Agreement (SLA) Miss
    df_delivered['sla_missed'] = (
        df_delivered['order_delivered_customer_date'] > df_delivered['order_estimated_delivery_date']
    ).astype(int)

    # Aggregate by date and region (State)
    df_delivered['purchase_date'] = df_delivered['order_purchase_timestamp'].dt.normalize()

    df_delivery_summary = df_delivered.groupby(['purchase_date', 'customer_state']).agg(
        total_delivered_orders=('order_id', 'nunique'),
        avg_delivery_days=('delivery_time_days', 'mean'),
        orders_missed_sla=('sla_missed', 'sum')
    ).reset_index()

    # Calculate SLA miss rate
    df_delivery_summary['sla_miss_rate'] = (
        df_delivery_summary['orders_missed_sla'] / df_delivery_summary['total_delivered_orders']
    )

    return df_delivery_summary
