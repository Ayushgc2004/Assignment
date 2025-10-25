Olist Data Pipeline, API, and Analytics Dashboard

This project implements a comprehensive data engineering solution using Python (Pandas, SQLAlchemy), orchestrated by Prefect, served via FastAPI, and visualized in Grafana using a PostgreSQL database.

🎯 System Overview

The system performs the following functions:

Extraction & Transformation (ETL): Reads the four Olist CSV files, cleans them, merges them into a single analytical dataset.

Orchestration: Prefect schedules the ETL process to run every 1 hour.

Data Loading (ELT): Loads the raw merged data into a staging table, and then creates two analytics-ready summary tables in PostgreSQL (analytics_sales_summary and analytics_delivery_performance).

API: A FastAPI service exposes the raw customer data from one CSV file as a JSON API endpoint.

Visualization: Grafana connects to the PostgreSQL analytics tables to display key business metrics.

📦 Project Structure
api.py: FastAPI application. Serves the olist_customers_dataset.csv data.
etl_tasks.py:Contains the modular ETL functions (Read, Clean, Merge, Load).
prefect_flow.py: Defines the Prefect flow, orchestration logic, and hourly schedule.


Setup and execution guide.
Step 1: Prepare Infrastructure & Files
File Placement: Place all Python files (api.py, etl_tasks.py, prefect_flow.py) and all Olist CSV files in a single project directory.
Install Software: Ensure you have PostgreSQL, Grafana, and Prefect Server running.
Create Database: In your PostgreSQL environment, create the target database:
  CREATE DATABASE olist_analysis;
Update Credentials: Crucially, open the etl_tasks.py file and update the DB_PASSWORD placeholder with 1234.

Step 2: Install Python Dependencies
Open your terminal or command prompt and install the required Python libraries:
  pip install pandas sqlalchemy psycopg2-binary fastapi uvicorn prefect

Step 3: Run the Orchestrated Pipeline (Prefect)
This is the core ETL process that populates the database.
Start Prefect Server (If not already running):
  prefect server start-http://localhost:4200)
Start the Prefect Agent: Open a new terminal window and start the execution agent:
  prefect agent start --pool "default-agent-pool"
Deploy the Flow (Hourly Schedule): Open a third terminal window and deploy the flow:
  python prefect_flow.py
Trigger First Run: Go to the Prefect UI, find the deployed flow (hourly-olist-etl), and manually click Run to execute the pipeline immediately and populate your tables.

Step 4: Run the JSON API (FastAPI)
The API provides access to the raw customer data.
Open a fourth terminal window and start the server:
  uvicorn api:app --reload
Verify: Access http://127.0.0.1:8000/customers in your browser to see the JSON output.

Step 5: Setup Grafana Dashboard
Once the Prefect pipeline has successfully populated the PostgreSQL tables, you can set up the visualizations.
  Access Grafana (e.g., http://localhost:3000).
Connect Data Source: Navigate to Connections > Data Sources and configure the PostgreSQL connection using the credentials from Step 1.
Build Dashboard: Follow the detailed SQL and visualization instructions provided in the grafana_instructions.md file to create the four analytical charts.
