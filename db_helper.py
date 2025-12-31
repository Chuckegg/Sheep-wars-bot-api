#!/usr/bin/env python3
"""
Database helper module for stats.db SQLite database operations.
Provides a centralized interface for all database operations.
"""

import sqlite3
import time
from pathlib import Path
from typing import Dict, List, Optional, Set
from contextlib import contextmanager


DB_FILE = Path(__file__).parent / "stats.db"


@contextmanager
def get_db_connection(db_path: Optional[Path] = None):
    """Context manager for database connections with automatic cleanup.
    
    Args:
        db_path: Optional custom path to database file
        
    Yields:
        sqlite3.Connection: Database connection object
    """
    path = db_path or DB_FILE
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row  # Enable dict-like access
    try:
        yield conn
    finally:
        conn.close()


def init_database(db_path: Optional[Path] = None):
    """Initialize database schema if it doesn't exist.
    
    Args:
        db_path: Optional custom path to database file
    """
    with get_db_connection(db_path) as conn:
        cursor = conn.cursor()
        
        # User stats table - stores all stat values for each user
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS user_stats (
                username TEXT NOT NULL,
                stat_name TEXT NOT NULL,
                lifetime REAL DEFAULT 0,
                session REAL DEFAULT 0,
                daily REAL DEFAULT 0,
                yesterday REAL DEFAULT 0,
                monthly REAL DEFAULT 0,
                PRIMARY KEY (username, stat_name)
            )
        ''')
        
        # User metadata table - stores level, icon, colors, etc.
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS user_meta (
                username TEXT PRIMARY KEY,
                level INTEGER DEFAULT 0,
                icon TEXT DEFAULT '',
                ign_color TEXT DEFAULT NULL,
                guild_tag TEXT DEFAULT NULL,
                guild_hex TEXT DEFAULT NULL
            )
        ''')
        
        # Create indexes for faster lookups
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_username ON user_stats(username)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_stat_name ON user_stats(stat_name)')
        
        conn.commit()


def get_all_usernames() -> List[str]:
    """Get list of all usernames in the database.
    
    Returns:
        List of usernames
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT DISTINCT username FROM user_stats ORDER BY username')
        return [row[0] for row in cursor.fetchall()]


def get_user_stats(username: str) -> Dict[str, Dict[str, float]]:
    """Get all stats for a specific user.
    
    Args:
        username: Username to query
        
    Returns:
        Dict mapping stat_name to dict of period values
        Example: {"kills": {"lifetime": 100, "session": 5, "daily": 10, ...}, ...}
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT stat_name, lifetime, session, daily, yesterday, monthly
            FROM user_stats
            WHERE username = ?
        ''', (username,))
        
        stats = {}
        for row in cursor.fetchall():
            stats[row[0]] = {
                'lifetime': row[1],
                'session': row[2],
                'daily': row[3],
                'yesterday': row[4],
                'monthly': row[5]
            }
        return stats


def get_user_meta(username: str) -> Optional[Dict]:
    """Get metadata for a specific user.
    
    Args:
        username: Username to query
        
    Returns:
        Dict with metadata or None if user not found
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT level, icon, ign_color, guild_tag, guild_hex
            FROM user_meta
            WHERE username = ?
        ''', (username,))
        
        row = cursor.fetchone()
        if row:
            return {
                'level': row[0],
                'icon': row[1],
                'ign_color': row[2],
                'guild_tag': row[3],
                'guild_hex': row[4]
            }
        return None


def update_user_stats(username: str, stats: Dict[str, float], 
                     snapshot_sections: Optional[Set[str]] = None):
    """Update user stats with new API data.
    
    This function:
    1. Updates lifetime values with current API data
    2. Optionally takes snapshots for specified periods
    3. Calculates deltas (current - snapshot) for all periods
    
    Args:
        username: Username to update
        stats: Dict mapping stat_name to current lifetime value
        snapshot_sections: Set of periods to snapshot ("session", "daily", "monthly")
    """
    snapshot_sections = snapshot_sections or set()
    
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        for stat_name, lifetime_value in stats.items():
            # Get existing record or create new one
            cursor.execute('''
                SELECT session, daily, yesterday, monthly
                FROM user_stats
                WHERE username = ? AND stat_name = ?
            ''', (username, stat_name))
            
            existing = cursor.fetchone()
            if existing:
                # Existing stat - keep current snapshots unless we're updating them
                session_snap = existing[0] if existing[0] is not None else lifetime_value
                daily_snap = existing[1] if existing[1] is not None else lifetime_value
                yesterday_snap = existing[2] if existing[2] is not None else lifetime_value
                monthly_snap = existing[3] if existing[3] is not None else lifetime_value
            else:
                # New stat - initialize all snapshots to current lifetime value
                # This makes initial deltas = 0, which is correct for a new stat
                session_snap = lifetime_value
                daily_snap = lifetime_value
                yesterday_snap = lifetime_value
                monthly_snap = lifetime_value
            
            # Update snapshots if explicitly requested
            if "session" in snapshot_sections:
                session_snap = lifetime_value
            if "daily" in snapshot_sections:
                daily_snap = lifetime_value
            if "yesterday" in snapshot_sections:
                yesterday_snap = lifetime_value
            if "monthly" in snapshot_sections:
                monthly_snap = lifetime_value
            
            # Insert or update - store SNAPSHOTS, not deltas
            # Deltas are calculated on read
            cursor.execute('''
                INSERT OR REPLACE INTO user_stats 
                (username, stat_name, lifetime, session, daily, yesterday, monthly)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (username, stat_name, lifetime_value, 
                  session_snap, daily_snap, yesterday_snap, monthly_snap))
        
        conn.commit()


def get_user_stats_with_deltas(username: str) -> Dict[str, Dict[str, float]]:
    """Get user stats with calculated deltas.
    
    This returns the format expected by the bot:
    - lifetime: current value from API
    - session/daily/yesterday/monthly: calculated deltas (lifetime - snapshot)
    
    Args:
        username: Username to query
        
    Returns:
        Dict mapping stat_name to dict with lifetime and delta values
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT stat_name, lifetime, session, daily, yesterday, monthly
            FROM user_stats
            WHERE username = ?
        ''', (username,))
        
        stats = {}
        for row in cursor.fetchall():
            stat_name = row[0]
            lifetime = row[1] or 0
            session_snap = row[2] or 0
            daily_snap = row[3] or 0
            yesterday_snap = row[4] or 0
            monthly_snap = row[5] or 0
            
            stats[stat_name] = {
                'lifetime': lifetime,
                'session': lifetime - session_snap,
                'daily': lifetime - daily_snap,
                'yesterday': lifetime - yesterday_snap,
                'monthly': lifetime - monthly_snap
            }
        return stats


def update_user_meta(username: str, level: int = 0, icon: str = '',
                    ign_color: Optional[str] = None,
                    guild_tag: Optional[str] = None,
                    guild_hex: Optional[str] = None):
    """Update user metadata.
    
    Args:
        username: Username to update
        level: User level
        icon: Prestige icon
        ign_color: IGN color hex code
        guild_tag: Guild tag
        guild_hex: Guild color hex code
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO user_meta 
            (username, level, icon, ign_color, guild_tag, guild_hex)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (username, level, icon, ign_color, guild_tag, guild_hex))
        conn.commit()


def rotate_daily_to_yesterday(usernames: List[str]) -> Dict[str, bool]:
    """Copy daily snapshot to yesterday snapshot for specified users.
    
    This is called before the daily refresh to preserve yesterday's stats.
    
    Args:
        usernames: List of usernames to rotate
        
    Returns:
        Dict mapping username to success status
    """
    results = {}
    
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        for username in usernames:
            try:
                # Copy daily column to yesterday column for all stats
                cursor.execute('''
                    UPDATE user_stats
                    SET yesterday = daily
                    WHERE username = ?
                ''', (username,))
                
                results[username] = True
            except Exception as e:
                print(f"[ERROR] Failed to rotate {username}: {e}")
                results[username] = False
        
        conn.commit()
    
    return results


def user_exists(username: str) -> bool:
    """Check if a user exists in the database.
    
    Args:
        username: Username to check
        
    Returns:
        True if user has any stats, False otherwise
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT COUNT(*) FROM user_stats WHERE username = ?
        ''', (username,))
        count = cursor.fetchone()[0]
        return count > 0


def delete_user(username: str):
    """Delete all data for a user.
    
    Args:
        username: Username to delete
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('DELETE FROM user_stats WHERE username = ?', (username,))
        cursor.execute('DELETE FROM user_meta WHERE username = ?', (username,))
        conn.commit()


def get_database_stats() -> Dict:
    """Get database statistics.
    
    Returns:
        Dict with database statistics
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        cursor.execute('SELECT COUNT(DISTINCT username) FROM user_stats')
        user_count = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM user_stats')
        stat_count = cursor.fetchone()[0]
        
        return {
            'users': user_count,
            'total_stats': stat_count,
            'db_file': str(DB_FILE),
            'exists': DB_FILE.exists()
        }


def backup_database(backup_path: Path) -> bool:
    """Create a backup copy of the database.
    
    Args:
        backup_path: Destination path for backup
        
    Returns:
        True if successful, False otherwise
    """
    try:
        import shutil
        shutil.copy2(DB_FILE, backup_path)
        return True
    except Exception as e:
        print(f"[ERROR] Database backup failed: {e}")
        return False
