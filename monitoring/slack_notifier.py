from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

def send_monitoring_results_to_slack(
    message: str,
    channel: str,
    token: str
) -> bool:
    """
    Send monitoring results to a Slack channel.
    
    Args:
        message (str): The message to send (formatted monitoring results)
        channel (str): The Slack channel to send the message to
        token (str): Slack bot token
        
    Returns:
        bool: True if message was sent successfully, False otherwise
        
    Raises:
        SlackApiError: If there's an error sending the message to Slack
    """
    try:
        client = WebClient(token=token)
        
        # Send message to Slack
        response = client.chat_postMessage(
            channel=channel,
            text=message
        )
        
        return response["ok"]
        
    except SlackApiError as e:
        print(f"Error sending message to Slack: {e.response['error']}")
        raise
