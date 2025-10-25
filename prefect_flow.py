from prefect import flow, task
from datetime import timedelta
from etl_tasks import (
    read_data_task, merge_data_task, load_to_postgres_task,
    create_sales_summary_task, create_delivery_summary_task,
    FILE_CUSTOMERS, FILE_ORDERS, FILE_ITEMS, FILE_PAYMENTS
)

@flow(name="Olist Hourly Data Pipeline")
def olist_etl_pipeline():
    """
    Main flow for the Olist data pipeline.
    1. Extract and Clean data from 4 sources.
    2. Merge data into a wide table.
    3. Load merged data into a staging table.
    4. Create and load two analytical tables.
    """
    # 1. Extraction/Cleaning
    df_cust = read_data_task.submit(FILE_CUSTOMERS)
    df_orders = read_data_task.submit(FILE_ORDERS)
    df_items = read_data_task.submit(FILE_ITEMS)
    df_payments = read_data_task.submit(FILE_PAYMENTS)

    # 2. Merging (waits for all reads to complete)
    df_merged = merge_data_task.submit(df_cust, df_orders, df_items, df_payments)

    # 3. Load Staging
    load_to_postgres_task.submit(df_merged, 'staging_olist_data')

    # 4. Create Analytical Tables
    df_sales = create_sales_summary_task.submit(df_merged)
    df_delivery = create_delivery_summary_task.submit(df_merged)

    # 5. Load Analytical Tables
    load_to_postgres_task.submit(df_sales, 'analytics_sales_summary')
    load_to_postgres_task.submit(df_delivery, 'analytics_delivery_performance')

    print("✅ Pipeline run complete. Data loaded into PostgreSQL.")


if __name__ == "__main__":
    olist_etl_pipeline.serve(
        name="hourly-olist-etl",
        schedules=[timedelta(hours=1)], 
        work_pool_name="default-agent-pool"  
    )
