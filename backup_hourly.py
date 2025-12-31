#!/usr/bin/env python3
"""
Hourly backup script for stats.db
Creates timestamped backups and deletes backups older than 24 hours.

Usage:
    python backup_hourly.py

Setup with cron (Linux):
    crontab -e
    # Add this line to run every hour:
    0 * * * * cd /home/timothy/backup && /usr/bin/python3 backup_hourly.py >> backup_hourly.log 2>&1

Setup with Task Scheduler (Windows):
    - Create a new task that runs hourly
    - Action: Start program python.exe
    - Arguments: backup_hourly.py
    - Start in: /home/timothy/backup
"""

import os
import shutil
from pathlib import Path
from datetime import datetime, timedelta

# Configuration
SCRIPT_DIR = Path(__file__).parent.absolute()
DB_FILE = SCRIPT_DIR / "stats.db"
BACKUP_DIR = SCRIPT_DIR / "backups"
BACKUP_RETENTION_HOURS = 24


def create_backup() -> bool:
    """Create an hourly backup of stats.db with timestamp.
    
    Returns:
        bool: True if backup succeeded, False otherwise
    """
    try:
        print(f"[BACKUP] Script directory: {SCRIPT_DIR}")
        print(f"[BACKUP] Database file path: {DB_FILE}")
        print(f"[BACKUP] Backup directory: {BACKUP_DIR}")
        print(f"[BACKUP] Database file exists: {DB_FILE.exists()}")
        print(f"[BACKUP] Database file readable: {os.access(DB_FILE, os.R_OK) if DB_FILE.exists() else 'N/A'}")
        print(f"[BACKUP] Backup dir exists: {BACKUP_DIR.exists()}")
        print(f"[BACKUP] Backup dir writable: {os.access(BACKUP_DIR, os.W_OK) if BACKUP_DIR.exists() else 'N/A'}")
        
        # Create backup directory if it doesn't exist
        try:
            BACKUP_DIR.mkdir(exist_ok=True, mode=0o755)
            print(f"[BACKUP] Backup directory ensured: {BACKUP_DIR}")
        except PermissionError:
            # Fallback: try creating in home directory
            fallback_dir = Path.home() / "backup_api_backups"
            print(f"[FALLBACK] Cannot create {BACKUP_DIR}, trying {fallback_dir}")
            fallback_dir.mkdir(exist_ok=True, mode=0o755)
            global BACKUP_DIR
            BACKUP_DIR = fallback_dir
            print(f"[FALLBACK] Using alternate backup directory: {BACKUP_DIR}")
        except Exception as e:
            print(f"[ERROR] Failed to create backup directory: {e}")
            # Last resort: use temp directory
            import tempfile
            BACKUP_DIR = Path(tempfile.gettempdir()) / "api_backups"
            BACKUP_DIR.mkdir(exist_ok=True)
            print(f"[FALLBACK] Using temporary directory: {BACKUP_DIR}")
        
        # Check if source file exists
        if not DB_FILE.exists():
            print(f"[ERROR] Source file not found: {DB_FILE}")
            return False
        
        # Generate timestamp for backup filename (YYYY-MM-DD_HH-00-00)
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-00-00")
        backup_filename = f"stats_{timestamp}.db"
        backup_path = BACKUP_DIR / backup_filename
        
        # Check if backup for this hour already exists
        if backup_path.exists():
            print(f"[SKIP] Backup already exists: {backup_filename}")
            return True
        
        # Copy the file with fallback methods
        print(f"[BACKUP] Creating backup: {backup_filename}")
        copy_success = False
        
        # Method 1: Try shutil.copy2 (preserves metadata)
        try:
            shutil.copy2(DB_FILE, backup_path)
            copy_success = True
            print(f"[BACKUP] Copy method: shutil.copy2")
        except Exception as e:
            print(f"[FALLBACK] shutil.copy2 failed: {e}, trying alternative...")
            
            # Method 2: Try shutil.copy (without metadata)
            try:
                shutil.copy(DB_FILE, backup_path)
                copy_success = True
                print(f"[FALLBACK] Copy method: shutil.copy")
            except Exception as e2:
                print(f"[FALLBACK] shutil.copy failed: {e2}, trying manual read/write...")
                
                # Method 3: Manual byte copy
                try:
                    with open(DB_FILE, 'rb') as src:
                        with open(backup_path, 'wb') as dst:
                            dst.write(src.read())
                    copy_success = True
                    print(f"[FALLBACK] Copy method: manual byte copy")
                except Exception as e3:
                    print(f"[ERROR] All copy methods failed: {e3}")
                    return False
        
        if copy_success:
            # Verify the backup was created and has content
            if backup_path.exists() and backup_path.stat().st_size > 0:
                print(f"[SUCCESS] Backup created: {backup_filename} ({backup_path.stat().st_size} bytes)")
                return True
            else:
                print(f"[ERROR] Backup file is empty or missing: {backup_filename}")
                return False
        
        return False
        
    except Exception as e:
        print(f"[ERROR] Unexpected error during backup: {e}")
        import traceback
        traceback.print_exc()
        return False


def cleanup_old_backups():
    """Delete backups older than BACKUP_RETENTION_HOURS."""
    try:
        if not BACKUP_DIR.exists():
            print("[CLEANUP] Backup directory doesn't exist, nothing to clean up")
            return
        
        now = datetime.now()
        cutoff = now - timedelta(hours=BACKUP_RETENTION_HOURS)
        deleted_count = 0
        kept_count = 0
        
        print(f"[CLEANUP] Scanning for backups older than {cutoff.strftime('%Y-%m-%d %H:%M:%S')}")
        
        for backup_file in BACKUP_DIR.glob("stats_*.db"):
            try:
                # Get file modification time
                mtime = datetime.fromtimestamp(backup_file.stat().st_mtime)
                
                if mtime < cutoff:
                    backup_file.unlink()
                    print(f"[CLEANUP] Deleted old backup: {backup_file.name} (from {mtime.strftime('%Y-%m-%d %H:%M:%S')})")
                    deleted_count += 1
                else:
                    kept_count += 1
            except Exception as e:
                print(f"[CLEANUP] Error processing {backup_file.name}: {e}")
        
        print(f"[CLEANUP] Deleted {deleted_count} old backup(s), kept {kept_count} recent backup(s)")
        
    except Exception as e:
        print(f"[ERROR] Cleanup failed: {e}")


def main():
    print("=" * 60)
    print(f"Database Backup - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    # Create backup
    success = create_backup()
    
    if success:
        print("\n[SUCCESS] Backup completed successfully")
        
        # Clean up old backups
        print("\n" + "-" * 60)
        cleanup_old_backups()
    else:
        print("\n[FAILURE] Backup failed")
    
    print("=" * 60)


if __name__ == "__main__":
    main()
