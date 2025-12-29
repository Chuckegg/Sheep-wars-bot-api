#!/usr/bin/env python3
"""
Hourly backup script for stats.xlsx
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
import time
from pathlib import Path
from datetime import datetime, timedelta

# Configuration
SCRIPT_DIR = Path(__file__).parent.absolute()
EXCEL_FILE = SCRIPT_DIR / "stats.xlsx"
BACKUP_DIR = SCRIPT_DIR / "backups"
BACKUP_RETENTION_HOURS = 24


def create_backup() -> bool:
    """Create an hourly backup of stats.xlsx with timestamp.
    
    Returns:
        bool: True if backup succeeded, False otherwise
    """
    try:
        print(f"[BACKUP] Script directory: {SCRIPT_DIR}")
        print(f"[BACKUP] Excel file path: {EXCEL_FILE}")
        print(f"[BACKUP] Backup directory: {BACKUP_DIR}")
        print(f"[BACKUP] Excel file exists: {EXCEL_FILE.exists()}")
        print(f"[BACKUP] Excel file readable: {os.access(EXCEL_FILE, os.R_OK) if EXCEL_FILE.exists() else 'N/A'}")
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
        if not EXCEL_FILE.exists():
            print(f"[ERROR] Source file not found: {EXCEL_FILE}")
            return False
        
        # Generate timestamp for backup filename (YYYY-MM-DD_HH-00-00)
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-00-00")
        backup_filename = f"stats_{timestamp}.xlsx"
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
            shutil.copy2(EXCEL_FILE, backup_path)
            copy_success = True
            print(f"[BACKUP] Copy method: shutil.copy2")
        except Exception as e:
            print(f"[FALLBACK] shutil.copy2 failed: {e}, trying alternative...")
            
            # Method 2: Try shutil.copy (without metadata)
            try:
                shutil.copy(EXCEL_FILE, backup_path)
                copy_success = True
                print(f"[FALLBACK] Copy method: shutil.copy")
            except Exception as e2:
                print(f"[FALLBACK] shutil.copy failed: {e2}, trying manual read/write...")
                
                # Method 3: Manual read/write
                try:
                    with open(EXCEL_FILE, 'rb') as src:
                        with open(backup_path, 'wb') as dst:
                            dst.write(src.read())
                    copy_success = True
                    print(f"[FALLBACK] Copy method: manual read/write")
                except Exception as e3:
                    print(f"[ERROR] All copy methods failed: {e3}")
                    return False
        
        # Verify the backup was created
        if copy_success and backup_path.exists():
            file_size = backup_path.stat().st_size
            print(f"[SUCCESS] Backup created: {backup_filename} ({file_size:,} bytes)")
            return True
        else:
            print(f"[ERROR] Backup file was not created: {backup_filename}")
            return False
            
    except Exception as e:
        print(f"[ERROR] Failed to create backup: {e}")
        import traceback
        traceback.print_exc()
        return False


def cleanup_old_backups() -> int:
    """Delete backups older than BACKUP_RETENTION_HOURS.
    
    Returns:
        int: Number of backups deleted
    """
    try:
        if not BACKUP_DIR.exists():
            print("[INFO] Backup directory does not exist yet")
            return 0
        
        # Calculate cutoff time
        cutoff_time = datetime.now() - timedelta(hours=BACKUP_RETENTION_HOURS)
        cutoff_timestamp = cutoff_time.timestamp()
        
        deleted_count = 0
        total_freed_bytes = 0
        
        # Iterate through backup files
        for backup_file in BACKUP_DIR.glob("stats_*.xlsx"):
            try:
                # Get file modification time
                file_mtime = backup_file.stat().st_mtime
                
                # Check if file is older than retention period
                if file_mtime < cutoff_timestamp:
                    file_size = backup_file.stat().st_size
                    backup_age_hours = (time.time() - file_mtime) / 3600
                    
                    print(f"[DELETE] Removing old backup: {backup_file.name} (age: {backup_age_hours:.1f}h, size: {file_size:,} bytes)")
                    backup_file.unlink()
                    
                    deleted_count += 1
                    total_freed_bytes += file_size
                    
            except Exception as e:
                print(f"[WARNING] Failed to delete {backup_file.name}: {e}")
                continue
        
        if deleted_count > 0:
            print(f"[CLEANUP] Deleted {deleted_count} old backup(s), freed {total_freed_bytes:,} bytes")
        else:
            print("[CLEANUP] No old backups to delete")
        
        return deleted_count
        
    except Exception as e:
        print(f"[ERROR] Failed during cleanup: {e}")
        return 0


def list_current_backups() -> None:
    """List all current backups with their ages and sizes."""
    try:
        if not BACKUP_DIR.exists():
            print("[INFO] No backups directory found")
            return
        
        backups = sorted(BACKUP_DIR.glob("stats_*.xlsx"), key=lambda p: p.stat().st_mtime, reverse=True)
        
        if not backups:
            print("[INFO] No backups found")
            return
        
        print(f"\n[INFO] Current backups ({len(backups)} total):")
        total_size = 0
        
        for backup_file in backups:
            try:
                stat = backup_file.stat()
                file_size = stat.st_size
                file_mtime = stat.st_mtime
                age_hours = (time.time() - file_mtime) / 3600
                
                total_size += file_size
                
                # Format age
                if age_hours < 1:
                    age_str = f"{age_hours * 60:.0f}m"
                else:
                    age_str = f"{age_hours:.1f}h"
                
                print(f"  - {backup_file.name:30s} | Age: {age_str:>6s} | Size: {file_size:>10,} bytes")
                
            except Exception as e:
                print(f"  - {backup_file.name} [ERROR: {e}]")
        
        print(f"[INFO] Total backup storage: {total_size:,} bytes ({total_size / 1024 / 1024:.2f} MB)\n")
        
    except Exception as e:
        print(f"[ERROR] Failed to list backups: {e}")


def main():
    """Main execution function."""
    print("=" * 70)
    print(f"[START] Hourly backup script - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)
    
    # Create new backup
    backup_success = create_backup()
    
    # Clean up old backups
    deleted_count = cleanup_old_backups()
    
    # List current backups
    list_current_backups()
    
    # Summary
    print("=" * 70)
    if backup_success:
        print("[COMPLETE] Backup cycle completed successfully")
    else:
        print("[COMPLETE] Backup cycle completed with errors")
    print("=" * 70)
    
    return 0 if backup_success else 1


if __name__ == "__main__":
    exit(main())
