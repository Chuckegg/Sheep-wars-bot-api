#!/usr/bin/env python3
"""
Convert user_colors.json to stats.db SQLite database.
Updates the ign_color column in the user_meta table.
"""

import sqlite3
import json
from pathlib import Path
import sys

# Define paths
BASE_DIR = Path(__file__).parent
COLORS_FILE = BASE_DIR / "user_colors.json"
DB_FILE = BASE_DIR / "stats.db"

def migrate_colors():
    if not COLORS_FILE.exists():
        print(f"[ERROR] {COLORS_FILE} not found.")
        return

    if not DB_FILE.exists():
        print(f"[ERROR] {DB_FILE} not found. Please run the bot or convert_to_db.py first.")
        return

    print(f"[INFO] Loading colors from {COLORS_FILE}...")
    try:
        with open(COLORS_FILE, 'r', encoding='utf-8') as f:
            colors_data = json.load(f)
    except Exception as e:
        print(f"[ERROR] Failed to load JSON: {e}")
        return

    print(f"[INFO] Found {len(colors_data)} entries. Migrating to database...")
    
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    updated_count = 0
    inserted_count = 0
    
    for username, color in colors_data.items():
        # Handle potential nested structure or simple key-value
        hex_color = color
        if isinstance(color, dict):
            hex_color = color.get('color') or color.get('hex') or color.get('ign_color')
        
        if not hex_color or not isinstance(hex_color, str):
            print(f"[WARN] Skipping {username}: Invalid color format {color}")
            continue
            
        # Ensure hex format starts with #
        if not hex_color.startswith('#'):
            hex_color = f"#{hex_color}"
            
        # Check if user exists in user_meta
        cursor.execute("SELECT 1 FROM user_meta WHERE LOWER(username) = LOWER(?)", (username,))
        exists = cursor.fetchone()
        
        if exists:
            cursor.execute("""
                UPDATE user_meta 
                SET ign_color = ? 
                WHERE LOWER(username) = LOWER(?)
            """, (hex_color, username))
            updated_count += 1
        else:
            # Insert new record if user doesn't exist
            cursor.execute("""
                INSERT INTO user_meta (username, ign_color)
                VALUES (?, ?)
            """, (username, hex_color))
            inserted_count += 1
            
    conn.commit()
    conn.close()
    
    print(f"[SUCCESS] Migration complete.\n  Updated: {updated_count}\n  Inserted: {inserted_count}")

if __name__ == "__main__":
    migrate_colors()