from typing import Optional
import os
from monitoring.utils import format_monitoring_results, get_connection_params
from monitoring.create_snowflake_connector import create_snowflake_connector
from monitoring.monitoring_query import QUERY
from monitoring.snowflake_reader import read_snowflake_table
from monitoring.slack_notifier import send_monitoring_results_to_slack


def run_monitoring(
    table_name: str,
    target_column: str,
    id_column: str,
    date_column: str,
    threshold: float,
    start_date: str,
    database: str,
    schema: str,
    slack_channel: Optional[str] = None,
    slack_token: Optional[str] = None
) -> str:
    """
    Run monitoring for a specific table and column.
    
    Args:
        table_name (str): Name of the table to monitor
        target_column (str): Name of the column to monitor
        id_column (str): Name of the column containing record identifiers,
        date_column (str): Name of the column containing the date information. It should be a date or datetime column,
        threshold (float): Value to compare against
        start_date (str): Start date to filter records
        database (str): Name of the database
        schema (str): Name of the schema
        slack_channel (str, optional): Slack channel to send results to
        slack_token (str, optional): Slack bot token for authentication
    
    Returns:
        str: Formatted monitoring results
    
    Raises:
        Exception: If there's an error during monitoring
    """
    try:
        # Create Snowflake connection
        conn = create_snowflake_connector(connection_params=get_connection_params())
        
        # Extract monitoring results
        query = QUERY.format(
            table_name=table_name,
            database=database,
            schema=schema,
            target_column=target_column,
            id_column=id_column,
            date_column=date_column,
            threshold=threshold,
            start_date=start_date,
        )
        results_df = read_snowflake_table(
            conn=conn,
            query=query
        )

        # Convert column names to lowercase and then to list of dictionaries
        results_df.columns = results_df.columns.str.lower()
        results = results_df.to_dict('records')

        # Format monitoring results
        formatted_results = format_monitoring_results(results, table_name, target_column)
        
        # Send to Slack if configured
        if slack_channel and slack_token:
            try:
                send_monitoring_results_to_slack(
                    message=formatted_results,
                    channel=slack_channel,
                    token=slack_token
                )
            except Exception as e:
                print(f"Warning: Failed to send results to Slack: {str(e)}")
        
        return formatted_results

    except Exception as e:
        raise Exception(f"Error running monitoring: {str(e)}")

def main():
    """
    Main function to run monitoring for balance table.
    """
    slack_channel = os.getenv("SLACK_CHANNEL")
    slack_token = os.getenv("SLACK_BOT_TOKEN")

    monitoring_config = {
            "table_name": "fct__organizations_balance",
            "target_column": "balance_change_percentage",
            "id_column": "organization_id",
            "date_column": "balance_date",
            "threshold": 50.0,
            "start_date": "2024-01-01",
            "database": "deel_takehome_dev",
            "schema": "ehernani_fact_tables",
            "slack_channel": slack_channel,
            "slack_token": slack_token
    }
    
    results = run_monitoring(**monitoring_config)
    print(results)

if __name__ == "__main__":
    main()
