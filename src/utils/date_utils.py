"""
Date and time utility functions
"""

import sys
from datetime import datetime, timedelta
from typing import Tuple, Optional

# Colors for output
class Colors:
    RED = '\033[0;31m'
    NC = '\033[0m'  # No Color

def validate_date(date_str: str) -> None:
    """Validate date format."""
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        print(f"{Colors.RED}Error: Invalid date format: {date_str}{Colors.NC}", file=sys.stderr)
        print("Date must be in YYYY-MM-DD format", file=sys.stderr)
        sys.exit(1)

def parse_date_range(start_date: Optional[str] = None,
                     end_date: Optional[str] = None,
                     single_date: Optional[str] = None) -> Tuple[str, str]:
    """
    Parse date range from arguments.

    Args:
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        single_date: Single date (will sync from this date to today)

    Returns:
        Tuple of (start_date, end_date) as strings
    """
    if single_date:
        start_date = single_date
        # Set end date to today in UTC
        try:
            from datetime import timezone
            end_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        except ImportError:
            end_date = datetime.utcnow().strftime("%Y-%m-%d")

    if not start_date or not end_date:
        raise ValueError("Start date and end date are required")

    # Validate dates
    validate_date(start_date)
    validate_date(end_date)

    # Validate date range
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    if start_dt > end_dt:
        print(f"{Colors.RED}Error: Start date must be before or equal to end date{Colors.NC}", file=sys.stderr)
        sys.exit(1)

    return (start_date, end_date)

