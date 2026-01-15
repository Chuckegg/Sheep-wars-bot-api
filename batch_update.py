import os
import subprocess
import sys
import argparse
from pathlib import Path

# Import database helper
from db_helper import rotate_daily_to_yesterday, reset_weekly_snapshots, get_tracked_users

SCRIPT_DIR = Path(__file__).parent.absolute()
# TRACKED_FILE = str(SCRIPT_DIR / "tracked_users.txt")  # Now using database


def load_tracked_users() -> list[str]:
    """Load tracked usernames from database."""
    return get_tracked_users()


def run_api_get(username: str, api_key: str, snapshot_flags: list[str]) -> bool:
    """Run api_get.py for a user with given snapshot flags.
    
    Note: api_key parameter is ignored since api_get.py only reads from API_KEY.txt
    
    Returns True if successful, False otherwise.
    """
    try:
        cmd = [sys.executable, "api_get.py", "-ign", username]
        # api_get.py doesn't accept -key parameter, it only reads from API_KEY.txt
        cmd.extend(snapshot_flags)
        
        result = subprocess.run(cmd, cwd=str(SCRIPT_DIR), capture_output=True, text=True, timeout=30)
        if result.returncode != 0:
            print(f"[ERROR] api_get.py failed for {username}", flush=True)
            print(f"  stdout: {result.stdout}", flush=True)
            print(f"  stderr: {result.stderr}", flush=True)
        return result.returncode == 0
    except subprocess.TimeoutExpired:
        print(f"[ERROR] api_get.py timed out for {username} after 30 seconds", flush=True)
        return False
    except Exception as e:
        print(f"[ERROR] Failed to run api_get.py for {username}: {e}", flush=True)
        return False


def batch_update(schedule: str, api_key: str | None = None) -> dict:
    """Update all tracked users with appropriate snapshots.
    
    Args:
        schedule: One of 'session', 'daily', 'yesterday', 'weekly', 'monthly', 'all', or 'all-session'
        api_key: Optional Hypixel API key; falls back to env var or hardcoded default
    
    Returns:
        Dict with results: {username: (success, snapshots_taken)}
    """
    # Special handling for 'yesterday' schedule - rotate daily->yesterday without API calls
    if schedule == 'yesterday':
        print("[INFO] Running yesterday rotation (copying daily->yesterday snapshots)", flush=True)
        users = load_tracked_users()
        results = rotate_daily_to_yesterday(users)
        # Return results in the expected format
        return {username: (success, ['rotate']) for username, success in results.items()}
    
    # Special handling for 'weekly' schedule - reset weekly snapshots to current lifetime
    if schedule == 'weekly':
        print("[INFO] Running weekly reset (setting weekly snapshots to current lifetime)", flush=True)
        users = load_tracked_users()
        results = reset_weekly_snapshots(users)
        # Return results in the expected format
        return {username: (success, ['weekly_reset']) for username, success in results.items()}
    
    users = load_tracked_users()
    if not users:
        print("[INFO] No tracked users found", flush=True)
        return {}
    
    if api_key is None:
        api_key = os.environ.get("HYPIXEL_API_KEY") or "0adb2317-d343-4275-aa22-e7a980eb59df"
    
    results = {}
    
    # Map schedule to snapshot types
    schedule_map = {
        'session': ['-session'],
        'daily': ['-daily'],
        'monthly': ['-monthly'],
        'all': ['-daily', '-monthly'],
        'all-session': ['-session', '-daily', '-monthly']
    }
    
    print(f"[INFO] Processing {len(users)} tracked users with schedule '{schedule}'...", flush=True)
    for idx, username in enumerate(users, 1):
        snapshots_to_take = schedule_map.get(schedule, [])
        
        if not snapshots_to_take:
            print(f"[SKIP] {username} - invalid schedule", flush=True)
            results[username] = (True, [])
            continue
        
        print(f"[RUN] [{idx}/{len(users)}] {username} - updating stats and taking snapshots: {', '.join(snapshots_to_take)}", flush=True)
        
        # Always update current stats first (lifetime values), then take snapshots
        # This ensures the all-time stats are fresh before calculating deltas
        success = run_api_get(username, api_key, snapshots_to_take)
        
        if success:
            print(f"[OK] {username} - success", flush=True)
            results[username] = (True, snapshots_to_take)
        else:
            print(f"[ERROR] {username} - failed", flush=True)
            results[username] = (False, snapshots_to_take)
    
    print(f"\n[SUMMARY] Completed {sum(1 for s, _ in results.values() if s)}/{len(users)} users successfully", flush=True)
    return results


def main():
    parser = argparse.ArgumentParser(description="Batch update tracked users with API snapshots")
    parser.add_argument("-schedule", choices=["session", "daily", "yesterday", "weekly", "monthly", "all", "all-session"], default="all",
                        help="Which snapshots to take")
    parser.add_argument("-key", "--api-key", help="Hypixel API key (optional, uses env or default)")
    args = parser.parse_args()
    
    results = batch_update(args.schedule, args.api_key)
    
    # Print summary
    successful = sum(1 for success, _ in results.values() if success)
    print(f"\n[SUMMARY] {successful}/{len(results)} users updated successfully")
    
    for username, (success, snapshots) in results.items():
        status = "[OK]" if success else "[ERROR]"
        print(f"  {status} {username}: {', '.join(snapshots) if snapshots else 'no snapshots'}")


if __name__ == "__main__":
    main()
