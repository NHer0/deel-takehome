import pandas as pd
import snowflake.connector

def read_snowflake_table(
    conn: snowflake.connector.SnowflakeConnection,
    query: str
) -> pd.DataFrame:
    """
    Read data from a Snowflake table and return it as a pandas DataFrame.
    
    Args:
        query (str): Custom SQL query to execute
        conn (snowflake.connector.SnowflakeConnection): Snowflake connection object
    
    Returns:
        pd.DataFrame: DataFrame containing the query results
    
    Raises:
        Exception: If there's an error executing the query
    """
    try:
        # Create cursor
        cur = conn.cursor()        
        cur.execute(query)
        
        # Fetch results into DataFrame
        df = cur.fetch_pandas_all()
        
        return df
        
    except Exception as e:
        raise Exception(f"Error reading from Snowflake: {str(e)}")
        
    finally:
        if cur is not None:
            cur.close() 
        if conn is not None:
            conn.close()
