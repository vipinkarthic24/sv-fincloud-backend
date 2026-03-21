"""
IST Time Utility — Asia/Kolkata timezone helper

Usage:
    from utils.ist_time import get_ist_now, IST

    get_ist_now()           → datetime with IST tzinfo
    get_ist_now().isoformat() → ISO string with +05:30 offset
    get_ist_now().date()    → today's date in IST
"""

import pytz
from datetime import datetime

IST = pytz.timezone("Asia/Kolkata")


def get_ist_now() -> datetime:
    """Return current datetime in IST (Asia/Kolkata)."""
    return datetime.now(IST)
