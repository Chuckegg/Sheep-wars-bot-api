import time
import sys
from pathlib import Path
from db_helper import get_all_usernames, update_user_meta
from api_get import read_api_key_file, get_uuid, get_hypixel_guild, extract_guild_info

def fix_guilds():
    print("[GUILD FIX] Starting guild data repair...")
    api_key = read_api_key_file()
    if not api_key:
        print("[ERROR] No API key found in API_KEY.txt")
        return

    usernames = get_all_usernames()
    print(f"[GUILD FIX] Found {len(usernames)} users to check.")

    success_count = 0
    for i, username in enumerate(usernames):
        try:
            print(f"[GUILD FIX] [{i1}/{len(usernames)}] Processing {username}...")
            
            # Get UUID
            try:
                uuid, proper_name = get_uuid(username)
            except Exception as e:
                print(f"  [WARN] Failed to get UUID for {username}: {e}")
                continue

            # Get Guild
            try:
                guild_data = get_hypixel_guild(uuid, api_key)
                tag, color = extract_guild_info(guild_data)
                
                # Update DB - this will overwrite any numbers with the correct string or NULL
                update_user_meta(username, guild_tag=tag, guild_hex=color)
                print(f"  [OK] Updated {username}: Guild=[{tag}], Color={color}")
                success_count = 1
                
            except Exception as e:
                print(f"  [WARN] Failed to get guild for {username}: {e}")
            
            time.sleep(0.5) # Avoid rate limits
            
        except Exception as e:
            print(f"[ERROR] Error processing {username}: {e}")

    print(f"[GUILD FIX] Completed. Updated {success_count}/{len(usernames)} users.")

if __name__ == "__main__":
    fix_guilds()
