# Data System for Financial Balance Alerts

This project implements a monitoring system for financial balances using dbt and Snowflake. It alerts when balances exceed specified thresholds and can send notifications to Slack.

## Features

- Monitor financial balances in Snowflake tables
- Configure custom thresholds for alerts
- Send notifications to Slack channels
- Comprehensive test coverage
- Environment-based configuration

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
uv pip install -r requirements.txt
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

### Monitoring Configuration

The monitoring system can be configured through the `run_monitoring` function parameters:

```python
result = run_monitoring(
    table_name='your_table',
    target_column='amount',
    id_column='id',
    date_column='date',
    threshold=100.0,
    start_date='2024-01-01',
    database='your_database',
    schema='your_schema'
)
```

## Usage

### Running Monitoring

1. Basic monitoring without Slack notifications:
```python
from monitoring.run_monitoring import run_monitoring

result = run_monitoring(
    table_name='fct__organizations_balance',
    target_column='amount',
    id_column='id',
    date_column='date',
    threshold=100.0,
    start_date='2024-01-01',
    database='your_database',
    schema='your_schema'
)
print(result)
```

2. With Slack notifications:
```python
result = run_monitoring(
    table_name='fct__organizations_balance',
    target_column='amount',
    id_column='id',
    date_column='date',
    threshold=100.0,
    start_date='2024-01-01',
    database='your_database',
    schema='your_schema',
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
│   ├── monitoring_query.py
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
- `get_monitoring_results.py`: Core monitoring logic
- `monitoring_query.py`: SQL query templates
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

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

[Your License Here]

