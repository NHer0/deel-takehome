import pytest
from unittest.mock import MagicMock
import pandas as pd
from monitoring.snowflake_reader import read_snowflake_table

@pytest.fixture
def mock_snowflake_connection():
    """Fixture to create a mock Snowflake connection."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    return mock_conn, mock_cursor

@pytest.fixture
def sample_dataframe():
    """Fixture to create a sample DataFrame for testing."""
    return pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie']
    })

def test_read_snowflake_table_custom_query(mock_snowflake_connection, sample_dataframe):
    """Test reading a table with a custom query."""
    mock_conn, mock_cursor = mock_snowflake_connection
    mock_cursor.fetch_pandas_all.return_value = sample_dataframe
    
    custom_query = "select id, name from test_table where id > 1"
    
    # Test the function
    result = read_snowflake_table(
        query=custom_query,
        conn=mock_conn
    )
    
    # Verify the custom query was used
    mock_cursor.execute.assert_called_once_with(custom_query)
    
    # Verify the result
    pd.testing.assert_frame_equal(result, sample_dataframe)

def test_read_snowflake_table_execution_error(mock_snowflake_connection):
    """Test handling of query execution errors."""
    mock_conn, mock_cursor = mock_snowflake_connection
    mock_cursor.execute.side_effect = Exception("Query execution failed")

    custom_query = "select id, name from test_table where id > 1"
    
    # Test that the function raises an exception
    with pytest.raises(Exception) as exc_info:
        read_snowflake_table(
            conn=mock_conn,
            query=custom_query
        )
    
    assert "Error reading from Snowflake" in str(exc_info.value)
    assert "Query execution failed" in str(exc_info.value)

def test_read_snowflake_table_cursor_cleanup(mock_snowflake_connection):
    """Test that cursor is properly closed even when an error occurs."""
    mock_conn, mock_cursor = mock_snowflake_connection
    mock_cursor.execute.side_effect = Exception("Test error")

    custom_query = "select id, name from test_table where id > 1"
    
    try:
        read_snowflake_table(
            query=custom_query,
            conn=mock_conn
        )
    except Exception:
        pass
    
    # Verify cursor was closed
    mock_cursor.close.assert_called_once()