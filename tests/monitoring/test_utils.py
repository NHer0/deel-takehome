import pytest
from monitoring.utils import format_monitoring_results

@pytest.fixture
def sample_results():
    return [
        {'id': '1', 'value': 200.0, 'difference': 100.0},
        {'id': '2', 'value': 180.0, 'difference': 80.0},
        {'id': '3', 'value': 150.0, 'difference': 50.0},
        {'id': '4', 'value': 140.0, 'difference': 40.0},
        {'id': '5', 'value': 130.0, 'difference': 30.0},
        {'id': '6', 'value': 120.0, 'difference': 20.0}
    ]

def test_format_monitoring_results_with_data(sample_results):
    """Test formatting results when there are records exceeding threshold"""
    formatted = format_monitoring_results(
        results=sample_results,
        table_name='test_table',
        target_column='amount'
    )
    
    # Split the output into lines for easier testing
    lines = formatted.split('\n')
    
    # Check header
    assert lines[0] == "Monitoring Results for test_table.amount:"
    assert lines[1] == "Found 6 records exceeding threshold"
    assert lines[3] == "Top 5 records:"
    
    # Check first record (highest difference)
    assert lines[5] == "1. ID: 1"
    assert lines[6] == "   Value: 200.0"
    assert lines[7] == "   Difference: 100.0"
    
    # Verify only top 5 records are shown
    assert len(lines) == 24  # Header + 5 records with their details

def test_format_monitoring_results_empty():
    """Test formatting results when no records exceed threshold"""
    formatted = format_monitoring_results(
        results=[],
        table_name='test_table',
        target_column='amount'
    )
    
    expected = "No records found exceeding threshold in test_table.amount"
    assert formatted == expected

def test_format_monitoring_results_less_than_five_records():
    """Test formatting results when there are fewer than 5 records"""
    results = [
        {'id': '1', 'value': 200.0, 'difference': 100.0},
        {'id': '2', 'value': 180.0, 'difference': 80.0}
    ]
    
    formatted = format_monitoring_results(
        results=results,
        table_name='test_table',
        target_column='amount'
    )
    print(formatted)
    
    lines = formatted.split('\n')
    print(lines)
    
    # Check header
    assert lines[0] == "Monitoring Results for test_table.amount:"
    assert lines[1] == "Found 2 records exceeding threshold"
    assert lines[3] == "Top 5 records:"
    
    # Check both records are shown
    assert lines[5] == "1. ID: 1"
    assert lines[9] == "2. ID: 2"
    
    # Verify correct number of lines
    assert len(lines) == 12  # Header + 2 records with their details 