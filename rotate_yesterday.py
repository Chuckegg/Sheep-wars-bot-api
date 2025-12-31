"""
Helper script to copy daily snapshot to yesterday snapshot before daily refresh.
This preserves yesterday's stats before overwriting today's daily snapshot.
"""
import os
from pathlib import Path

# Import database helper
from db_helper import rotate_daily_to_yesterday, get_tracked_users

SCRIPT_DIR = Path(__file__).parent.absolute()
# TRACKED_FILE = str(SCRIPT_DIR / "tracked_users.txt")  # Now using database


def load_tracked_users() -> list[str]:
    """Load tracked usernames from database."""
    return get_tracked_users()


def rotate_yesterday():
    """Copy daily snapshot to yesterday snapshot for all tracked users."""
    users = load_tracked_users()
    
    if not users:
        print("[SKIP] No tracked users found")
        return 0
    
    print(f"[INFO] Rotating daily->yesterday for {len(users)} users...")
    results = rotate_daily_to_yesterday(users)
    
    success_count = sum(1 for success in results.values() if success)
    
    for username, success in results.items():
        status = "[OK]" if success else "[ERROR]"
        print(f"{status} {username}")
    
    print(f"\n[SUMMARY] Rotated daily->yesterday for {success_count}/{len(users)} users")
    return success_count


if __name__ == "__main__":
    rotate_yesterday()
