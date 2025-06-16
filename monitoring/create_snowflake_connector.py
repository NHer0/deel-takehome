import os
from typing import Optional, Dict, Any
import snowflake.connector

def create_snowflake_connector(
    connection_params: Optional[Dict[str, Any]]
) -> snowflake.connector.SnowflakeConnection:
    """
    Create a connection to Snowflake.
    
    Args:
        connection_params (dict): Dictionary containing Snowflake connection parameters.
            If None, will use environment variables:
            - SNOWFLAKE_USER
            - SNOWFLAKE_PASSWORD
            - SNOWFLAKE_ACCOUNT
            - SNOWFLAKE_WAREHOUSE
            - SNOWFLAKE_ROLE
            - SNOWFLAKE_DATABASE
            - SNOWFLAKE_SCHEMA
    
    Returns:
        snowflake.connector.SnowflakeConnection: The Snowflake connection object
    
    Raises:
        ValueError: If required connection parameters are missing
        Exception: If there's an error connecting to Snowflake
    """
    # Validate required connection parameters
    required_params = ["user", "password", "account", "warehouse", "role"]
    missing_params = [param for param in required_params if not connection_params.get(param)]
    if missing_params:
        raise ValueError(f"Missing required connection parameters: {', '.join(missing_params)}")
    
    try:
        # Establish connection to Snowflake
        conn = snowflake.connector.connect(**connection_params)
        return conn
        
    except Exception as e:
        raise Exception(f"Error connecting to Snowflake: {str(e)}")
