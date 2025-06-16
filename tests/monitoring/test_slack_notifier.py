import pytest
from unittest.mock import Mock, patch
from monitoring.slack_notifier import send_monitoring_results_to_slack
from slack_sdk.errors import SlackApiError

@pytest.fixture
def sample_message():
    return """Monitoring Results for test_table.amount:
Found 3 records exceeding threshold:
1. ID: 1
   Value: 200.0
   Difference: 100.0
2. ID: 2
   Value: 180.0
   Difference: 80.0
3. ID: 3
   Value: 150.0
   Difference: 50.0"""

def test_send_monitoring_results_success(sample_message):
    """Test successful sending of monitoring results to Slack"""
    mock_response = {"ok": True}
    mock_client = Mock()
    mock_client.chat_postMessage.return_value = mock_response
    
    with patch('monitoring.slack_notifier.WebClient', return_value=mock_client):
        result = send_monitoring_results_to_slack(
            message=sample_message,
            channel="#monitoring-alerts",
            token="xoxb-test-token"
        )
        
        # Verify the message was sent correctly
        mock_client.chat_postMessage.assert_called_once_with(
            channel="#monitoring-alerts",
            text=sample_message
        )
        assert result is True

def test_send_monitoring_results_error(sample_message):
    """Test handling of Slack API errors"""
    mock_client = Mock()
    mock_client.chat_postMessage.side_effect = SlackApiError(
        message="Error sending message",
        response={"error": "channel_not_found"}
    )
    
    with patch('monitoring.slack_notifier.WebClient', return_value=mock_client):
        with pytest.raises(SlackApiError) as exc_info:
            send_monitoring_results_to_slack(
                message=sample_message,
                channel="#non-existent-channel",
                token="xoxb-test-token"
            )
        
        assert "Error sending message" in str(exc_info.value)
        assert "channel_not_found" in str(exc_info.value) 