import os
import argparse
import json
from pathlib import Path
from typing import Dict, Optional
import requests

# Import database helper
from db_helper import (
    init_database,
    update_user_stats,
    update_user_meta,
    get_user_stats_with_deltas
)

SCRIPT_DIR = Path(__file__).parent.absolute()


def read_api_key_file() -> Optional[str]:
    """Read API key from API_KEY.txt next to the script, if present."""
    key_path = SCRIPT_DIR / "API_KEY.txt"
    if key_path.exists():
        try:
            content = key_path.read_text(encoding="utf-8").strip()
            if content:
                return content
        except Exception:
            # ignore read errors and fall back to other sources
            pass
    return None


# -------- Wool Games level calculation --------

def experience_to_level(exp: float) -> int:
    """Calculate Wool Games level from experience with prestige scaling.
    
    At each prestige (0, 100, 200, 300, etc), the XP requirements reset:
    - Level X+0 to X+1: 1000 XP
    - Level X+1 to X+2: 2000 XP
    - Level X+2 to X+3: 3000 XP
    - Level X+3 to X+4: 4000 XP
    - Level X+4 to X+5: 5000 XP
    - Level X+5 to X+100: 5000 XP each (95 levels)
    
    Total XP per prestige (100 levels): 1000+2000+3000+4000+5000 + 95*5000 = 490000
    """
    if exp <= 0:
        return 0
    
    XP_PER_PRESTIGE = 490000
    prestige_count = int(exp / XP_PER_PRESTIGE)
    remaining_xp = exp - (prestige_count * XP_PER_PRESTIGE)
    
    # Calculate level within current prestige (0-99)
    if remaining_xp < 1000:
        level_in_prestige = 0
    elif remaining_xp < 3000:  # 1000 + 2000
        level_in_prestige = 1
    elif remaining_xp < 6000:  # 1000 + 2000 + 3000
        level_in_prestige = 2
    elif remaining_xp < 10000:  # 1000 + 2000 + 3000 + 4000
        level_in_prestige = 3
    elif remaining_xp < 15000:  # 1000 + 2000 + 3000 + 4000 + 5000
        level_in_prestige = 4
    else:
        # Level 5+ in prestige: 5000 XP each
        remaining_after_first_5 = remaining_xp - 15000
        # Use floor division so partial progress doesn't round up to the next level
        level_in_prestige = 5 + int(remaining_after_first_5 // 5000)
    
    # Convert to 1-based display level (Hypixel shows levels starting at 1)
    return prestige_count * 100 + level_in_prestige + 1


# -------- API helpers --------

def get_uuid(username: str) -> tuple[str, str]:
    """Get UUID and properly-cased username from Mojang API.
    
    Returns:
        tuple[str, str]: (uuid, properly_cased_username)
    """
    # Try Mojang
    try:
        r = requests.get(f"https://api.mojang.com/users/profiles/minecraft/{username}", timeout=5)
        if r.status_code == 200:
            data = r.json()
            return data["id"], data.get("name", username)
    except Exception:
        pass
        
    # Try PlayerDB fallback
    try:
        r = requests.get(f"https://playerdb.co/api/player/minecraft/{username}", timeout=5)
        if r.status_code == 200:
            data = r.json()
            if data.get('success'):
                meta = data.get('data', {}).get('player', {})
                return meta.get('raw_id'), meta.get('username', username)
    except Exception:
        pass

    raise requests.exceptions.RequestException(f"Could not resolve UUID for {username}")


def get_hypixel_player(uuid: str, api_key: str) -> Dict:
    r = requests.get(
        "https://api.hypixel.net/v2/player",
        headers={"API-Key": api_key},
        params={"uuid": uuid},
        timeout=20,
    )
    r.raise_for_status()
    return r.json()


def get_hypixel_guild(uuid: str, api_key: str) -> Dict:
    """Fetch guild information for a player from Hypixel API."""
    r = requests.get(
        "https://api.hypixel.net/v2/guild",
        headers={"API-Key": api_key},
        params={"player": uuid},
        timeout=20,
    )
    r.raise_for_status()
    return r.json()


def extract_guild_info(guild_json: Dict) -> tuple[Optional[str], Optional[str]]:
    """Extract guild tag and tag color from Hypixel guild API response.
    
    Returns (tag, tagColor). Returns ("", "") if not in a guild.
    """
    if not isinstance(guild_json, dict):
        return None, None
    
    guild = guild_json.get("guild")
    if not guild or not isinstance(guild, dict):
        return "", ""
    
    tag = guild.get("tag") or ""
    tag_color = guild.get("tagColor") or ""
    
    return tag, tag_color


def extract_player_rank(player_json: Dict) -> Optional[str]:
    """Extract the player's rank from Hypixel API response.
    
    Returns rank in order of priority, skipping "NONE" values:
    rank, monthlyPackageRank, newPackageRank, packageRank
    """
    player = player_json.get("player", {}) if isinstance(player_json, dict) else {}
    if not isinstance(player, dict):
        return None
    
    # Check in order of priority, skip "NONE" values
    rank = player.get("rank")
    if rank and rank.upper() != "NONE":
        return rank
    
    monthly = player.get("monthlyPackageRank")
    if monthly and monthly.upper() != "NONE":
        return monthly
    
    new_package = player.get("newPackageRank")
    if new_package and new_package.upper() != "NONE":
        return new_package
    
    package = player.get("packageRank")
    if package and package.upper() != "NONE":
        return package
    
    return None


def extract_wool_games_flat(player_json: Dict) -> Dict:
    """Extract Wool Games data and flatten into a single stats dict.
    Includes progression.available_layers/experience, coins, sheep_wars.stats*, and playtime.
    """
    player = player_json.get("player", {}) if isinstance(player_json, dict) else {}
    stats_root = player.get("stats", {}) if isinstance(player, dict) else {}
    # Try common wool keys
    wool_keys = ["WOOL_GAMES", "Wool_Games", "WoolGames", "WoolWars"]
    wool = None
    for k in wool_keys:
        if k in stats_root:
            wool = stats_root[k]
            break
    if not isinstance(wool, dict):
        return {}

    flat: Dict[str, float] = {}

    # progression fields
    progression = wool.get("progression")
    if isinstance(progression, dict):
        if "available_layers" in progression:
            flat["available_layers"] = progression.get("available_layers")
        if "experience" in progression:
            exp_val = progression.get("experience") or 0
            flat["experience"] = exp_val
            # Derive wool games level from experience with prestige scaling
            try:
                flat["level"] = experience_to_level(exp_val)
            except Exception:
                flat["level"] = 0

    # coins
    if "coins" in wool:
        flat["coins"] = wool.get("coins")

    # sheep_wars stats
    sheep_stats = (wool.get("sheep_wars", {}) or {}).get("stats")
    if isinstance(sheep_stats, dict):
        for k, v in sheep_stats.items():
            flat[k] = v

    # playtime
    if "playtime" in wool:
        flat["playtime"] = wool.get("playtime")

    return flat


def get_rank_color(rank: Optional[str]) -> str:
    """Get the default color for a rank.
    
    Returns hex color code based on rank priority.
    """
    if not rank:
        return "#FFFFFF"  # White for no rank
    
    rank_upper = rank.upper()
    
    # Rank color mapping
    rank_colors = {
        "ADMIN": "#FF5555",           # Red (c)
        "SUPERSTAR": "#FFAA00",       # Gold (6)
        "MVP_PLUS": "#55FFFF",        # Aqua (b)
        "MVP_PLUS_PLUS": "#FFAA00",   # Gold (6) - MVP++ permanent is gold too
        "MVP": "#55FFFF",             # Aqua (b)
        "VIP_PLUS": "#00AA00",        # Dark Green (2)
        "VIP": "#55FF55",             # Green (a)
    }
    
    return rank_colors.get(rank_upper, "#FFFFFF")  # Default to white


def save_user_color_and_rank(username: str, rank: Optional[str], guild_tag: Optional[str] = None, guild_color: Optional[str] = None):
    """Save or update user's rank and guild info in database.
    
    Only assigns color automatically for NEW users based on their rank.
    Existing users keep their custom color.
    """
    from db_helper import get_user_meta, update_user_meta
    
    username_key = username
    
    # Check if user already exists in database
    existing_meta = get_user_meta(username_key)
    
    if existing_meta:
        # User exists - only update rank and guild info, preserve their color
        print(f"[DEBUG] User {username} already exists with data: {existing_meta}")
        
        old_color = existing_meta.get('ign_color')
        print(f"[DEBUG] Preserving existing color {old_color}, updating rank to {rank}, guild: {guild_tag}")
        
        # Update only rank and guild, keep color
        update_user_meta(username_key, 
                        ign_color=old_color,
                        guild_tag=guild_tag, 
                        guild_hex=guild_color)
    else:
        # NEW USER - assign color based on rank automatically
        auto_color = get_rank_color(rank)
        print(f"[DEBUG] NEW USER {username} - assigning auto color {auto_color} for rank {rank}, guild: {guild_tag}")
        
        update_user_meta(username_key,
                        ign_color=auto_color,
                        guild_tag=guild_tag,
                        guild_hex=guild_color)


def api_update_database(username: str, api_key: str, snapshot_sections: set[str] | None = None):
    """Update user stats in database from Hypixel API.
    
    Args:
        username: Minecraft username
        api_key: Hypixel API key
        snapshot_sections: Set of periods to snapshot ("session", "daily", "yesterday", "monthly")
        
    Returns:
        Dict with update results
    """
    try:
        # Ensure database exists
        init_database()
        
        # Get UUID and properly cased username
        uuid, proper_username = get_uuid(username)
        print(f"[API] Fetching data for {proper_username} (UUID: {uuid})")
        
        # Get player data from Hypixel
        data = get_hypixel_player(uuid, api_key)
        
    except (requests.exceptions.RequestException, json.JSONDecodeError) as e:
        should_fallback = False
        if isinstance(e, requests.exceptions.HTTPError) and e.response is not None:
            if e.response.status_code == 429:
                print(f"[WARNING] Rate limited (429) for {username}. Attempting snapshot fallback.")
                should_fallback = True
            elif e.response.status_code >= 500:
                print(f"[WARNING] API Server Error ({e.response.status_code}) for {username}. Attempting snapshot fallback.")
                should_fallback = True
        elif isinstance(e, (requests.exceptions.ConnectionError, requests.exceptions.Timeout)):
            print(f"[WARNING] Connection error for {username}: {e}. Attempting snapshot fallback.")
            should_fallback = True

        if should_fallback:
            # For rate limiting: just take snapshots without updating lifetime values
            print("[INFO] Taking snapshots from existing database values")
            try:
                # Get existing stats
                existing_stats = get_user_stats_with_deltas(username)
                if existing_stats:
                    # Extract just lifetime values
                    lifetime_stats = {stat: data['lifetime'] for stat, data in existing_stats.items()}
                    # Update with snapshots
                    update_user_stats(username, lifetime_stats, snapshot_sections)
                    print(f"[FALLBACK] Snapshots taken for {username}")
                    return {
                        "skipped": True,
                        "reason": "rate_limited",
                        "username": username,
                        "snapshots_written": True,
                    }
                else:
                    print(f"[ERROR] No existing data found for {username}")
                    return {
                        "skipped": True,
                        "reason": "rate_limited",
                        "username": username,
                        "snapshots_written": False,
                    }
            except Exception as fe:
                print(f"[ERROR] Snapshot fallback failed: {fe}")
                return {
                    "skipped": True,
                    "reason": "rate_limited",
                    "username": username,
                    "snapshots_written": False,
                }
        else:
            # Non-recoverable error (e.g. 404)
            print(f"[ERROR] API request failed for {username}: {e}")
            return {
                "skipped": True,
                "reason": "api_error",
                "error": str(e),
                "username": username,
                "snapshots_written": False,
            }
    
    # Extract Wool Games stats
    current = extract_wool_games_flat(data)
    if not current:
        raise RuntimeError(f"No Wool Games -> Sheep Wars stats for {proper_username}")

    print(f"[API] Extracted {len(current)} stats for {proper_username}")

    # Fetch guild information
    print(f"[DEBUG] Fetching guild information for {proper_username} (UUID: {uuid})")
    try:
        guild_data = get_hypixel_guild(uuid, api_key)
        # Save guild data to file for inspection
        guild_file = SCRIPT_DIR / "guild_info.json"
        with open(guild_file, 'w') as f:
            json.dump(guild_data, f, indent=2)
        print(f"[DEBUG] Guild data saved to guild_info.json")
        guild_tag, guild_color = extract_guild_info(guild_data)
        print(f"[DEBUG] Extracted guild tag: {guild_tag}, color: {guild_color}")
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 429:
            print(f"[DEBUG] Rate limited (429) fetching guild data for {proper_username}. Using cached data.")
            guild_tag, guild_color = None, None
        else:
            print(f"[DEBUG] Failed to fetch guild data: {e}")
            guild_tag, guild_color = None, None
    except Exception as e:
        print(f"[DEBUG] Failed to fetch guild data: {e}")
        guild_tag, guild_color = None, None

    # Extract and save player rank and guild info to database
    rank = extract_player_rank(data)
    print(f"[DEBUG] Extracted rank for {proper_username}: {rank}")
    save_user_color_and_rank(proper_username, rank, guild_tag, guild_color)

    # Update database with stats
    print(f"[DB] Updating stats for {proper_username}")
    update_user_stats(proper_username, current, snapshot_sections)
    
    # Update metadata
    level = int(current.get('level', 0))
    # Calculate prestige icon (placeholder - you can add icon logic here)
    icon = None
    update_user_meta(proper_username, level, icon, None, guild_tag, guild_color)
    
    # Get processed stats with deltas for return value
    processed_stats = get_user_stats_with_deltas(proper_username)
    
    print(f"[DB] Successfully updated {proper_username}")
    
    return {
        "uuid": uuid,
        "stats": current,
        "processed_stats": processed_stats,
        "database": "stats.db",
        "username": proper_username
    }


def main():
    parser = argparse.ArgumentParser(description="API-based Wool Games stats to SQLite database")
    parser.add_argument("-ign", "--username", required=True, help="Minecraft IGN")
    parser.add_argument("-session", action="store_true", help="Take session snapshot")
    parser.add_argument("-daily", action="store_true", help="Take daily snapshot")
    parser.add_argument("-yesterday", action="store_true", help="Take yesterday snapshot")
    parser.add_argument("-monthly", action="store_true", help="Take monthly snapshot")
    args = parser.parse_args()

    # Only use the API key from API_KEY.txt (no CLI/env/default fallback)
    api_key = read_api_key_file()
    if not api_key:
        raise RuntimeError(
            "Missing API key: create API_KEY.txt next to api_get.py containing your Hypixel API key"
        )
    
    sections = set()
    if args.session:
        sections.add("session")
    if args.daily:
        sections.add("daily")
    if args.yesterday:
        sections.add("yesterday")
    if args.monthly:
        sections.add("monthly")

    res = api_update_database(args.username, api_key, snapshot_sections=sections)
    print(json.dumps(res, default=str))


if __name__ == "__main__":
    main()
