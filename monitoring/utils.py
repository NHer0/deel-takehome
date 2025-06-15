from typing import List, Dict, Any
import os

def format_monitoring_results(results: List[Dict[str, Any]], table_name: str, target_column: str) -> str:
    """
    Format monitoring results into a readable string.
    
    Args:
        results (List[Dict[str, Any]]): List of monitoring results
        table_name (str): Name of the monitored table
        target_column (str): Name of the monitored column
    
    Returns:
        str: Formatted results string
    """
    if not results:
        return f"No records found exceeding threshold in {table_name}.{target_column}"
    
    output = [f"Monitoring Results for {table_name}.{target_column}:"]
    output.append(f"Found {len(results)} records exceeding threshold")
    output.append("\nTop 5 records:")
    
    for i, record in enumerate(results[:5], 1):
        output.append(f"\n{i}. ID: {record['id']}")
        output.append(f"   Value: {record['value']}")
        output.append(f"   Difference: {record['difference']}")
    
    return "\n".join(output)


def get_connection_params() -> Dict[str, Any]:
    """
    Get Snowflake connection parameters from environment variables.
    
    Returns:
        Dict[str, Any]: Dictionary containing connection parameters
    """
    return {
        "user": os.getenv("SNOWFLAKE_USER"),
        "password": os.getenv("SNOWFLAKE_PASSWORD"),
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "role": os.getenv("SNOWFLAKE_ROLE"),
        "database": os.getenv("SNOWFLAKE_DATABASE"),
        "schema": os.getenv("SNOWFLAKE_SCHEMA"),
    }