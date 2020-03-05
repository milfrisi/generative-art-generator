from datetime import datetime

def get_timestamp():
    """Return the current timestamp as a ISO formatted string"""
    timestamp = datetime.now().isoformat()
    return timestamp

