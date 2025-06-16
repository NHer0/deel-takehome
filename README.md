# Data System for Financial Balance Alerts

This project implements a monitoring system for financial balances using dbt and Snowflake. It alerts when balances exceed specified thresholds and can send notifications to Slack.

## Features

- Monitor financial balances in Snowflake tables
- Configure custom thresholds for alerts
- Send notifications to Slack channels
- Comprehensive test coverage

## Hypothesis
After analyzing the data contained in the input:
 - `invoice_id` is unique across the invoice data, i.e we don't have different data point for different invoice status of the same id
 - It has been assumed therefore that every invoicing event overwrites the previous in the invoice data source
 - `created_at` refers to to when the row was created in the database, (we assume this is when the event happened approximately) and not to when the invoice was created
 - Similar points for the `organization` dataset and `organization_id`
 - We have an `invoice_amount` and `payment_amount` that are always the same with both exist. What it's different is the currency exchange rate.
 - We assumed that `invoice_fx_rate` is the rate when the service was agreed and `payment_fx_rate` when it was paid. We assume the latest to be the relevant one for balance calculations
 - According to the data there is no clear pattern on when the payments are processed in terms of invoice status, as almost all status have `payment` amount not null/null
 - Some assumptioms were needed therefore to calculate the daily_balance_change_usd:
    - A change in balance is valid financially when the status of the invoice is `paid`
    - We considered that `status = refunded` can impact negatively the balance. We substract the payment_amount from the cumulative balance when an invoice has this status

 ## Data Modelling
 The data model consist of the following layers:
  - Landing: a copy of the input data, ingested into Snowflake using the seed functionality
  - Staging: views on top of the landing layer, carrying out data cleaning and standardisation
  - Fact tables / Dimensions. Layer build on top of the staging views, implementing a star schema modelling approach. In this case we have:
    - fct__organizations_balance
    - dim__organizations

## Monitoring
A function has been created to extract the data from a snowflake table and compare a column value to a threshold, filtering based on a date.
The assumption is that this date will come from a scheduling system (for example Airflow DAG start date), effectively monitoring only new data.
The function is flexible and is design to work in any other table/target_column. It also alerts to a slack_channel if the required slack env variables exist.

Also important, it has been assumed that we are monitoring the changes relative to previous balance data point, in opposition to look into natural changes happening in one natural day.
In case we would like to look to monitor happening in a natural day the function can be easily adapted by using QUERY_ALT in folder `monitoring_queries.py`

## Output
 - Produced fact_tables and dimensions can be found in `output` folder in the form of csv files. For the fact_table, only a sample (balance_date > 2024-01-01) is provided in the repo as the file would be to big for the complete data to be pushed. The  complete data though can be found [here](https://drive.google.com/drive/folders/1fYLebMtyMLMqOAyeiP54HuNmyw6hW74w).
 - [Here](https://drive.google.com/file/d/1pBF78BlPiM5ScjE1N8ebhrXGRqnGbqO9/view?usp=drive_link) you can find a short clip demonstrating the dbt+snowflake building of the data assets (note that the null check for daily_balance_change fails due to some paid invoices not having null payment_amount). [Here](https://drive.google.com/file/d/1RRswXsOkEKiefDbBt8qIrpT98aNjZsyU/view?usp=drive_link) a short demonstration querying the data in Snowflake.
 - [Here](https://github.com/NHer0/deel-takehome/blob/master/monitoring/run_monitoring.py) is the monitoring function and [here](https://drive.google.com/file/d/1wgtB377xa2PPI523pq1pUn1BhnvUnbby/view?usp=drive_link) a small demo of the monitoring usage, including the Slack alerting.

## Prerequisites

- Python 3.12.0
- Snowflake account and credentials
- Slack workspace and bot token (for notifications)
- dbt and Snowflake setup

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd deel-takehome
```

2. Install dependencies using uv:
```bash
uv pip install .
```

## Configuration

### Environment Variables

Create a `.env` file in the project root with the following variables:

```env
# Snowflake Configuration
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_ROLE=your_role
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_SCHEMA=your_schema

# Slack Configuration
SLACK_CHANNEL=#your-channel
SLACK_TOKEN=xoxb-your-token
```

Use following command to export the variables in the file into your terminal.

```
export $(grep -v '^#' .env | xargs)
```

### Monitoring Configuration

The monitoring system can be configured through the `run_monitoring` function parameters:

```python
result = run_monitoring(
    table_name='your_table',
    target_column='amount',
    id_column='id',
    date_column='date',
    threshold=500.0,
    start_date='2024-01-01',
    database='your_database',
    schema='your_schema'
)
```

## Usage

### Running Monitoring

1. Basic monitoring without Slack notifications:
```python
result = run_monitoring(
    table_name='fct__organizations_balance',
    target_column='balance_change_percentage',
    id_column='organization_id',
    date_column='balance_date',
    threshold=50,
    start_date='2024-01-01',
    database='your_database',
    schema='your_schema'
)
```

2. With Slack notifications:
```python
result = run_monitoring(
    table_name='fct__organizations_balance',
    target_column='balance_change_percentage',
    id_column='organization_id',
    date_column='balance_date',
    threshold=50,
    start_date='2024-01-01',
    database='your_database',
    schema='your_schema'
    slack_channel='#monitoring-alerts',
    slack_token='xoxb-your-token'
)
```

### Running Tests

Run the test suite using pytest:
```bash
pytest tests/monitoring/test_run_monitoring.py -v
```

## Project Structure

```
deel-takehome/
├── monitoring/
│   ├── __init__.py
│   ├── create_snowflake_connector.py
│   ├── get_monitoring_results.py
│   ├── monitoring_queries.py
│   ├── run_monitoring.py
│   ├── slack_notifier.py
│   ├── snowflake_reader.py
│   └── utils.py
├── tests/
│   └── monitoring/
│       ├── __init__.py
│       ├── test_run_monitoring.py
│       └── test_slack_notifier.py
├── .env
├── pyproject.toml
└── README.md
```

## Components

- `create_snowflake_connector.py`: Handles Snowflake connection setup
- `monitoring_queries.py`: SQL query templates to extract the data to be monitored
- `run_monitoring.py`: Main monitoring execution
- `slack_notifier.py`: Slack notification functionality
- `snowflake_reader.py`: Snowflake data reading utilities
- `utils.py`: Helper functions

## Testing

The project includes comprehensive tests for:
- Basic monitoring functionality
- Empty results handling
- Connection error handling
- Query error handling
- Slack notification success and failure cases

## Airflow

This project uses Apache Airflow to orchestrate the execution of dbt models. Follow these steps to set up and run Airflow:

### Prerequisites

- Docker and Docker Compose installed on your machine.

### Setup

1. Navigate to the `airflow` directory:
   ```bash
   cd airflow
   ```

2. Build and start the Airflow services:
   ```bash
   make airflow-all
   ```

   This command will:
   - Build the Docker images
   - Start the Airflow services
   - Initialize the Airflow database
   - Create an admin user

3. Access the Airflow web interface at [http://localhost:8080](http://localhost:8080) and log in with:
   - Username: `admin`
   - Password: `admin`

### Running dbt Models

The Airflow DAG (`dbt_dag.py`) is configured to run the following dbt commands in sequence:
- `dbt debug`
- `dbt deps`
- `dbt run`
- `dbt test`

You can trigger the DAG manually from the Airflow web interface or wait for its scheduled run.

### Additional Commands

- To build the Docker images:
  ```bash
  make airflow-build
  ```

- To start the Airflow services:
  ```bash
  make airflow-up
  ```

- To initialize the Airflow database:
  ```bash
  make airflow-init
  ```

- To create an admin user:
  ```bash
  make airflow-create-admin
  ```
