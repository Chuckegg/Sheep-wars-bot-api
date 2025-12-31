#!/usr/bin/env python3
"""
Transfer stats data from another stats.db to the current stats.db.
This is useful for migrating or merging data between databases.
"""

import sqlite3
from pathlib import Path
import argparse


def transfer_stats(source_db='backup_stats.db', dest_db='stats.db'):
    """
    Transfer stats from source database to destination database.
    
    Args:
        source_db: Path to source stats.db file
        dest_db: Path to destination stats.db file
    """
    
    source_path = Path(source_db)
    dest_path = Path(dest_db)
    
    # Check if files exist
    if not source_path.exists():
        print(f"[ERROR] Source file '{source_db}' not found!")
        return False
    
    if not dest_path.exists():
        print(f"[ERROR] Destination file '{dest_db}' not found!")
        return False
    
    try:
        # Connect to both databases
        print(f"[DB] Connecting to source: {source_db}")
        source_conn = sqlite3.connect(str(source_path))
        source_conn.row_factory = sqlite3.Row
        
        print(f"[DB] Connecting to destination: {dest_db}")
        dest_conn = sqlite3.connect(str(dest_path))
        
        source_cursor = source_conn.cursor()
        dest_cursor = dest_conn.cursor()
        
        # Get all usernames from source
        source_cursor.execute('SELECT DISTINCT username FROM user_stats ORDER BY username')
        usernames = [row[0] for row in source_cursor.fetchall()]
        
        print(f"[INFO] Found {len(usernames)} users in source database")
        
        transferred_users = 0
        transferred_stats = 0
        
        for username in usernames:
            print(f"\n[TRANSFER] Processing user: {username}")
            
            # Get all stats for this user from source
            source_cursor.execute('''
                SELECT stat_name, lifetime, session, daily, yesterday, monthly
                FROM user_stats
                WHERE username = ?
            ''', (username,))
            
            stats = source_cursor.fetchall()
            
            # Get metadata for this user from source
            source_cursor.execute('''
                SELECT level, icon, ign_color, guild_tag, guild_hex
                FROM user_meta
                WHERE username = ?
            ''', (username,))
            
            meta = source_cursor.fetchone()
            
            # Insert/update stats in destination
            for stat in stats:
                dest_cursor.execute('''
                    INSERT OR REPLACE INTO user_stats 
                    (username, stat_name, lifetime, session, daily, yesterday, monthly)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (username, stat[0], stat[1], stat[2], stat[3], stat[4], stat[5]))
                transferred_stats += 1
            
            # Insert/update metadata in destination
            if meta:
                dest_cursor.execute('''
                    INSERT OR REPLACE INTO user_meta 
                    (username, level, icon, ign_color, guild_tag, guild_hex)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (username, meta[0], meta[1], meta[2], meta[3], meta[4]))
            
            print(f"  [OK] Transferred {len(stats)} stats for {username}")
            transferred_users += 1
        
        # Commit changes
        dest_conn.commit()
        
        print(f"\n{'='*60}")
        print(f"[SUCCESS] Transfer complete!")
        print(f"  Users transferred: {transferred_users}")
        print(f"  Total stats transferred: {transferred_stats}")
        print(f"{'='*60}")
        
        # Close connections
        source_conn.close()
        dest_conn.close()
        
        return True
        
    except Exception as e:
        print(f"[ERROR] Transfer failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    parser = argparse.ArgumentParser(description="Transfer stats between SQLite databases")
    parser.add_argument("-source", "--source-db", default="backup_stats.db",
                        help="Source database file (default: backup_stats.db)")
    parser.add_argument("-dest", "--dest-db", default="stats.db",
                        help="Destination database file (default: stats.db)")
    args = parser.parse_args()
    
    success = transfer_stats(args.source_db, args.dest_db)
    
    if success:
        print("\n[SUCCESS] Transfer completed successfully")
    else:
        print("\n[FAILURE] Transfer failed")


if __name__ == "__main__":
    main()
