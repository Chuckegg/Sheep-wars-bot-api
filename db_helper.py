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
        
        # User links table - maps usernames to Discord IDs
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS user_links (
                username TEXT PRIMARY KEY,
                discord_id TEXT NOT NULL
            )
        ''')
        
        # Default users table - maps Discord IDs to default usernames
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS default_users (
                discord_id TEXT PRIMARY KEY,
                username TEXT NOT NULL
            )
        ''')
        
        # Tracked streaks table - stores winstreaks and killstreaks
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tracked_streaks (
                username TEXT PRIMARY KEY,
                winstreak INTEGER DEFAULT 0,
                killstreak INTEGER DEFAULT 0,
                last_wins INTEGER DEFAULT 0,
                last_losses INTEGER DEFAULT 0,
                last_kills INTEGER DEFAULT 0,
                last_deaths INTEGER DEFAULT 0
            )
        ''')
        
        # Tracked users table - list of users being actively tracked
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tracked_users (
                username TEXT PRIMARY KEY,
                added_at INTEGER DEFAULT (strftime('%s', 'now'))
            )
        ''')
        
        # Create indexes for faster lookups
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_username ON user_stats(username)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_stat_name ON user_stats(stat_name)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_discord_id ON user_links(discord_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_default_discord ON default_users(discord_id)')
        
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


def update_user_meta(username: str, level: Optional[int] = None, icon: Optional[str] = None,
                    ign_color: Optional[str] = None,
                    guild_tag: Optional[str] = None,
                    guild_hex: Optional[str] = None):
    """Update user metadata.
    
    None values are ignored (existing values preserved).
    To clear a text value (color/guild), pass an empty string "".
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        # Check if user exists
        cursor.execute('SELECT * FROM user_meta WHERE username = ?', (username,))
        row = cursor.fetchone()
        
        if row:
            # Update existing record - only update fields that are not None
            updates = []
            params = []
            
            if level is not None:
                updates.append("level = ?")
                params.append(level)
            if icon is not None:
                updates.append("icon = ?")
                params.append(icon)
            if ign_color is not None:
                updates.append("ign_color = ?")
                params.append(ign_color if ign_color != "" else None)
            if guild_tag is not None:
                updates.append("guild_tag = ?")
                val = guild_tag if guild_tag != "" else None
                params.append(str(val) if val is not None else None)
            if guild_hex is not None:
                updates.append("guild_hex = ?")
                params.append(guild_hex if guild_hex != "" else None)
            
            if updates:
                params.append(username)
                sql = f"UPDATE user_meta SET {', '.join(updates)} WHERE username = ?"
                cursor.execute(sql, params)
        else:
            # Insert new record
            cursor.execute('''
                INSERT INTO user_meta 
                (username, level, icon, ign_color, guild_tag, guild_hex)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                username, 
                level if level is not None else 0, 
                icon if icon is not None else '', 
                ign_color if ign_color != "" else None, 
                str(guild_tag) if guild_tag and guild_tag != "" else None, 
                guild_hex if guild_hex != "" else None
            ))
        
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
            SELECT COUNT(*) FROM user_stats WHERE LOWER(username) = LOWER(?)
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
        cursor.execute('DELETE FROM user_stats WHERE LOWER(username) = LOWER(?)', (username,))
        cursor.execute('DELETE FROM user_meta WHERE LOWER(username) = LOWER(?)', (username,))
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


# ============================================================================
# User Links Functions (username <-> Discord ID mappings)
# ============================================================================

def get_discord_id(username: str) -> Optional[str]:
    """Get Discord ID for a username.
    
    Args:
        username: Minecraft username
        
    Returns:
        Discord ID or None if not found
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT discord_id FROM user_links WHERE LOWER(username) = LOWER(?)', (username,))
        row = cursor.fetchone()
        return row['discord_id'] if row else None


def set_discord_link(username: str, discord_id: str):
    """Link a username to a Discord ID.
    
    Args:
        username: Minecraft username
        discord_id: Discord user ID
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO user_links (username, discord_id)
            VALUES (?, ?)
            ON CONFLICT(username) DO UPDATE SET discord_id = excluded.discord_id
        ''', (username.lower(), discord_id))
        conn.commit()


def get_all_user_links() -> Dict[str, str]:
    """Get all username -> Discord ID mappings.
    
    Returns:
        Dictionary mapping usernames to Discord IDs
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT username, discord_id FROM user_links')
        return {row['username']: row['discord_id'] for row in cursor.fetchall()}


# ============================================================================
# Default Users Functions (Discord ID -> default username mappings)
# ============================================================================

def get_default_username(discord_id: str) -> Optional[str]:
    """Get default username for a Discord ID.
    
    Args:
        discord_id: Discord user ID
        
    Returns:
        Username or None if not found
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT username FROM default_users WHERE discord_id = ?', (discord_id,))
        row = cursor.fetchone()
        return row['username'] if row else None


def set_default_username(discord_id: str, username: str):
    """Set default username for a Discord ID.
    
    Args:
        discord_id: Discord user ID
        username: Minecraft username
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO default_users (discord_id, username)
            VALUES (?, ?)
            ON CONFLICT(discord_id) DO UPDATE SET username = excluded.username
        ''', (discord_id, username))
        conn.commit()


def get_all_default_users() -> Dict[str, str]:
    """Get all Discord ID -> username mappings.
    
    Returns:
        Dictionary mapping Discord IDs to usernames
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT discord_id, username FROM default_users')
        return {row['discord_id']: row['username'] for row in cursor.fetchall()}


# ============================================================================
# Tracked Streaks Functions
# ============================================================================

def get_tracked_streaks(username: str) -> Optional[Dict]:
    """Get streak tracking data for a username.
    
    Args:
        username: Minecraft username
        
    Returns:
        Dictionary with streak data or None if not found
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT winstreak, killstreak, last_wins, last_losses, last_kills, last_deaths
            FROM tracked_streaks WHERE username = ?
        ''', (username,))
        row = cursor.fetchone()
        if row:
            return {
                'winstreak': row['winstreak'],
                'killstreak': row['killstreak'],
                'last_wins': row['last_wins'],
                'last_losses': row['last_losses'],
                'last_kills': row['last_kills'],
                'last_deaths': row['last_deaths']
            }
        return None


def update_tracked_streaks(username: str, streak_data: Dict):
    """Update streak tracking data for a username.
    
    Args:
        username: Minecraft username
        streak_data: Dictionary with streak data
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO tracked_streaks 
            (username, winstreak, killstreak, last_wins, last_losses, last_kills, last_deaths)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(username) DO UPDATE SET
                winstreak = excluded.winstreak,
                killstreak = excluded.killstreak,
                last_wins = excluded.last_wins,
                last_losses = excluded.last_losses,
                last_kills = excluded.last_kills,
                last_deaths = excluded.last_deaths
        ''', (
            username,
            streak_data.get('winstreak', 0),
            streak_data.get('killstreak', 0),
            streak_data.get('last_wins', 0),
            streak_data.get('last_losses', 0),
            streak_data.get('last_kills', 0),
            streak_data.get('last_deaths', 0)
        ))
        conn.commit()


def get_all_tracked_streaks() -> Dict[str, Dict]:
    """Get all tracked streaks.
    
    Returns:
        Dictionary mapping usernames to streak data
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT username, winstreak, killstreak, last_wins, last_losses, last_kills, last_deaths
            FROM tracked_streaks
        ''')
        result = {}
        for row in cursor.fetchall():
            result[row['username']] = {
                'winstreak': row['winstreak'],
                'killstreak': row['killstreak'],
                'last_wins': row['last_wins'],
                'last_losses': row['last_losses'],
                'last_kills': row['last_kills'],
                'last_deaths': row['last_deaths']
            }
        return result


# ============================================================================
# Tracked Users Functions
# ============================================================================

def add_tracked_user(username: str):
    """Add a username to tracked users.
    
    Args:
        username: Minecraft username to track
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR IGNORE INTO tracked_users (username)
            VALUES (?)
        ''', (username,))
        conn.commit()


def remove_tracked_user(username: str):
    """Remove a username from tracked users.
    
    Args:
        username: Minecraft username to stop tracking
        
    Returns:
        bool: True if user was removed, False if not found
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('DELETE FROM tracked_users WHERE LOWER(username) = LOWER(?)', (username,))
        conn.commit()
        return cursor.rowcount > 0


def is_tracked_user(username: str) -> bool:
    """Check if a username is being tracked.
    
    Args:
        username: Minecraft username
        
    Returns:
        True if user is tracked, False otherwise
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT 1 FROM tracked_users WHERE LOWER(username) = LOWER(?)', (username,))
        return cursor.fetchone() is not None


def get_tracked_users() -> List[str]:
    """Get list of all tracked usernames.
    
    Returns:
        List of tracked usernames
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT username FROM tracked_users ORDER BY username')
        return [row['username'] for row in cursor.fetchall()]


def set_tracked_users(usernames: List[str]):
    """Replace all tracked users with new list.
    
    Args:
        usernames: List of usernames to track
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('DELETE FROM tracked_users')
        for username in usernames:
            cursor.execute('INSERT OR IGNORE INTO tracked_users (username) VALUES (?)', (username,))
        conn.commit()
