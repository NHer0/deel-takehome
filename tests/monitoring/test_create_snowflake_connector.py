import os
import pytest
from unittest.mock import patch, MagicMock
from monitoring.create_snowflake_connector import create_snowflake_connector

@pytest.fixture
def mock_snowflake_connector():
    """Fixture to mock the Snowflake connector."""
    with patch('snowflake.connector.connect') as mock_connect:
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        yield mock_connect

@pytest.fixture
def valid_connection_params():
    """Fixture with valid connection parameters."""
    return {
        "user": "test_user",
        "password": "test_password",
        "account": "test_account",
        "warehouse": "test_warehouse",
        "role": "test_role",
        "database": "test_database",
        "schema": "test_schema"
    }

def test_create_snowflake_connector_with_params(mock_snowflake_connector, valid_connection_params):
    """Test creating a connection with explicit parameters."""
    conn = create_snowflake_connector(connection_params=valid_connection_params)
    
    # Verify the connection was created with correct parameters
    mock_snowflake_connector.assert_called_once_with(**valid_connection_params)
    assert conn == mock_snowflake_connector.return_value

def test_create_snowflake_connector_missing_required_params():
    """Test that missing required parameters raise ValueError."""
    # Test with missing user
    with pytest.raises(ValueError) as exc_info:
        create_snowflake_connector({
            "password": "test_password",
            "account": "test_account",
            "warehouse": "test_warehouse",
            "role": "test_role"
        })
    assert "Missing required connection parameters" in str(exc_info.value)
    assert "user" in str(exc_info.value)

def test_create_snowflake_connector_connection_error(mock_snowflake_connector, valid_connection_params):
    """Test that connection errors are properly handled."""
    # Simulate a connection error
    mock_snowflake_connector.side_effect = Exception("Connection failed")
    
    with pytest.raises(Exception) as exc_info:
        create_snowflake_connector(connection_params=valid_connection_params)
    
    assert "Error connecting to Snowflake" in str(exc_info.value)
    assert "Connection failed" in str(exc_info.value)