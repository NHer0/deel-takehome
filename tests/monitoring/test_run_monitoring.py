import pytest
from unittest.mock import Mock, patch
import pandas as pd
from monitoring.run_monitoring import run_monitoring

@pytest.fixture
def mock_connection_params():
    return {
        "user": "test_user",
        "password": "test_password",
        "account": "test_account",
        "warehouse": "test_warehouse",
        "role": "test_role",
        "database": "test_database",
        "schema": "test_schema"
    }

@pytest.fixture
def mock_snowflake_conn():
    return Mock()

@pytest.fixture
def sample_results_df():
    return pd.DataFrame({
        'id': ['1', '2', '3'],
        'value': [200.0, 180.0, 150.0],
        'difference': [100.0, 80.0, 50.0]
    })

def test_run_monitoring_success(mock_snowflake_conn, sample_results_df):
    """Test successful monitoring run with results"""
    with patch('monitoring.run_monitoring.create_snowflake_connector', return_value=mock_snowflake_conn), \
         patch('monitoring.run_monitoring.read_snowflake_table', return_value=sample_results_df):
        
        result = run_monitoring(
            table_name='test_table',
            target_column='amount',
            id_column='id',
            date_column='date',
            threshold=100.0,
            start_date='2024-01-01',
            database='test_db',
            schema='test_schema'
        )
        
        # Verify the result contains expected information
        assert 'Monitoring Results for test_table.amount:' in result
        assert 'Found 3 records exceeding threshold' in result
        assert '1. ID: 1' in result
        assert 'Value: 200.0' in result
        assert 'Difference: 100.0' in result

def test_run_monitoring_empty_results(mock_snowflake_conn):
    """Test monitoring run with no results"""
    empty_df = pd.DataFrame(columns=['ID', 'VALUE', 'DIFFERENCE'])
    
    with patch('monitoring.run_monitoring.create_snowflake_connector', return_value=mock_snowflake_conn), \
         patch('monitoring.run_monitoring.read_snowflake_table', return_value=empty_df):
        
        result = run_monitoring(
            table_name='test_table',
            target_column='amount',
            id_column='id',
            date_column='date',
            threshold=1000.0,  # High threshold to ensure no results
            start_date='2024-01-01',
            database='test_db',
            schema='test_schema'
        )
        
        expected = "No records found exceeding threshold in test_table.amount"
        assert result == expected

def test_run_monitoring_connection_error():
    """Test handling of connection errors"""
    with patch('monitoring.run_monitoring.create_snowflake_connector', side_effect=Exception("Connection failed")):
        with pytest.raises(Exception) as exc_info:
            run_monitoring(
                table_name='test_table',
                target_column='amount',
                id_column='id',
                date_column='date',
                threshold=100.0,
                start_date='2024-01-01',
                database='test_db',
                schema='test_schema'
            )
        
        assert "Error running monitoring" in str(exc_info.value)
        assert "Connection failed" in str(exc_info.value)

def test_run_monitoring_query_error(mock_snowflake_conn):
    """Test handling of query execution errors"""
    with patch('monitoring.run_monitoring.create_snowflake_connector', return_value=mock_snowflake_conn), \
         patch('monitoring.run_monitoring.read_snowflake_table', side_effect=Exception("Query failed")):
        
        with pytest.raises(Exception) as exc_info:
            run_monitoring(
                table_name='test_table',
                target_column='amount',
                id_column='id',
                date_column='date',
                threshold=100.0,
                start_date='2024-01-01',
                database='test_db',
                schema='test_schema'
            )
        
        assert "Error running monitoring" in str(exc_info.value)
        assert "Query failed" in str(exc_info.value)

def test_run_monitoring_with_slack_success(mock_connection_params, mock_snowflake_conn, sample_results_df):
    """Test successful monitoring run with Slack notification"""
    with patch('monitoring.run_monitoring.create_snowflake_connector', return_value=mock_snowflake_conn), \
         patch('monitoring.run_monitoring.read_snowflake_table', return_value=sample_results_df), \
         patch('monitoring.run_monitoring.send_monitoring_results_to_slack') as mock_slack:
        
        result = run_monitoring(
            table_name='test_table',
            target_column='amount',
            id_column='id',
            date_column='date',
            threshold=100.0,
            start_date='2024-01-01',
            database='test_db',
            schema='test_schema',
            slack_channel='#monitoring-alerts',
            slack_token='xoxb-test-token'
        )
        
        # Verify the result contains expected information
        assert 'Monitoring Results for test_table.amount:' in result
        assert 'Found 3 records exceeding threshold' in result
        
        # Verify Slack notification was sent
        mock_slack.assert_called_once()
        call_args = mock_slack.call_args[1]
        assert call_args['channel'] == '#monitoring-alerts'
        assert call_args['token'] == 'xoxb-test-token'
        assert 'Monitoring Results for test_table.amount:' in call_args['message']

def test_run_monitoring_with_slack_error(mock_connection_params, mock_snowflake_conn, sample_results_df):
    """Test monitoring run with Slack notification error"""
    with patch('monitoring.run_monitoring.create_snowflake_connector', return_value=mock_snowflake_conn), \
         patch('monitoring.run_monitoring.read_snowflake_table', return_value=sample_results_df), \
         patch('monitoring.run_monitoring.send_monitoring_results_to_slack', side_effect=Exception("Slack error")):
        
        result = run_monitoring(
            table_name='test_table',
            target_column='amount',
            id_column='id',
            date_column='date',
            threshold=100.0,
            start_date='2024-01-01',
            database='test_db',
            schema='test_schema',
            slack_channel='#monitoring-alerts',
            slack_token='xoxb-test-token'
        )
        
        # Verify the result is still returned despite Slack error
        assert 'Monitoring Results for test_table.amount:' in result
        assert 'Found 3 records exceeding threshold' in result

def test_run_monitoring_without_slack_params(mock_connection_params, mock_snowflake_conn, sample_results_df):
    """Test monitoring run without Slack parameters"""
    with patch('monitoring.run_monitoring.create_snowflake_connector', return_value=mock_snowflake_conn), \
         patch('monitoring.run_monitoring.read_snowflake_table', return_value=sample_results_df), \
         patch('monitoring.run_monitoring.send_monitoring_results_to_slack') as mock_slack:
        
        result = run_monitoring(
            table_name='test_table',
            target_column='amount',
            id_column='id',
            date_column='date',
            threshold=100.0,
            start_date='2024-01-01',
            database='test_db',
            schema='test_schema'
        )
        
        # Verify the result contains expected information
        assert 'Monitoring Results for test_table.amount:' in result
        assert 'Found 3 records exceeding threshold' in result
        
        # Verify Slack notification was not sent
        mock_slack.assert_not_called()
