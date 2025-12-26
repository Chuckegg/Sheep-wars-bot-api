import discord
from discord.ext import commands
import subprocess
import sys
from openpyxl import load_workbook
import os
import re
import shutil
from zoneinfo import ZoneInfo
import json
from pathlib import Path
import io
try:
    from PIL import Image, ImageDraw, ImageFont
except Exception:
    Image = None

# Get the directory where discord.py is located
BOT_DIR = Path(__file__).parent.absolute()

# tracked/users + creator info
TRACKED_FILE = os.path.join(os.path.dirname(__file__), "tracked_users.txt")
USER_LINKS_FILE = os.path.join(os.path.dirname(__file__), "user_links.json")
USER_COLORS_FILE = os.path.join(os.path.dirname(__file__), "user_colors.json")
CREATOR_NAME = "chuckegg"
# Optionally set a numeric Discord user ID for direct DM (recommended for reliability)
CREATOR_ID = "542467909549555734"
CREATOR_TZ = ZoneInfo("America/New_York")

def safe_save_workbook(wb, filepath: str) -> bool:
    """Safely save a workbook with backup and error recovery.
    
    Creates a backup before saving. If save fails, restores from backup.
    Always ensures workbook is closed properly.
    
    Args:
        wb: The openpyxl Workbook object to save
        filepath: Path to the Excel file
        
    Returns:
        bool: True if save succeeded, False otherwise
    """
    backup_path = str(filepath) + ".backup"
    backup_created = False
    
    try:
        # Create backup if file exists
        if os.path.exists(filepath):
            try:
                shutil.copy2(filepath, backup_path)
                backup_created = True
                print(f"[BACKUP] Created backup: {backup_path}")
            except Exception as backup_err:
                print(f"[WARNING] Failed to create backup: {backup_err}")
                # Continue anyway - we'll try to save without backup
        
        # Attempt to save
        wb.save(str(filepath))
        print(f"[SAVE] Successfully saved: {filepath}")
        
        # Remove backup after successful save
        if backup_created and os.path.exists(backup_path):
            try:
                os.remove(backup_path)
            except Exception:
                pass  # Not critical if backup removal fails
        
        return True
        
    except Exception as save_err:
        print(f"[ERROR] Failed to save workbook: {save_err}")
        
        # Try to restore from backup if save failed
        if backup_created and os.path.exists(backup_path):
            try:
                shutil.copy2(backup_path, filepath)
                print(f"[RESTORE] Restored from backup after save failure")
            except Exception as restore_err:
                print(f"[ERROR] Failed to restore from backup: {restore_err}")
        
        return False
        
    finally:
        # Always try to close the workbook
        try:
            wb.close()
            print(f"[CLEANUP] Workbook closed")
        except Exception as close_err:
            print(f"[WARNING] Error closing workbook: {close_err}")

# sanitize output for Discord (remove problematic unicode/control chars)
def sanitize_output(text: str) -> str:
    if text is None:
        return ""
    # Replace a few common emoji with ASCII labels
    replacements = {
        '✅': '[OK]',
        '❌': '[ERROR]',
        '⚠️': '[WARNING]',
        '📊': '[DATA]',
        '📋': '[INFO]',
        '⏭️': '[SKIP]',
    }
    for k, v in replacements.items():
        text = text.replace(k, v)

    # Remove C0 control chars except newline and tab
    text = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f]', '', str(text))
    # Collapse very long whitespace
    text = re.sub(r"\s{3,}", ' ', text)
    return text


def _to_number(val):
    """Coerce worksheet cell values to a numeric type (float).

    Returns 0 for None/empty/unparseable values. Strips commas before parsing.
    """
    if val is None:
        return 0
    if isinstance(val, (int, float)):
        return val
    s = str(val).strip()
    if s == "":
        return 0
    # Remove thousands separators
    s = s.replace(",", "")
    try:
        return float(s)
    except Exception:
        return 0

# Helper function to run scripts with proper working directory
def run_script(script_name, args, timeout=30):
    """Run a Python script in the bot directory with proper working directory"""
    return subprocess.run(
        [sys.executable, script_name, *args],
        cwd=str(BOT_DIR),
        capture_output=True,
        text=True,
        timeout=timeout
    )

def run_script_batch(script_name, args):
    """Run a batch script with extended timeout (5 minutes for large user lists)"""
    return subprocess.run(
        [sys.executable, script_name, *args],
        cwd=str(BOT_DIR),
        capture_output=True,
        text=True,
        timeout=300  # 5 minutes for batch operations
    )

# additional imports for background tasks
import asyncio
import datetime
import random

def format_playtime(seconds: int) -> str:
    """Convert playtime from seconds to human-readable format.
    
    - >= 30 days: 'x months, x days' (if days > 1) or 'x months'
    - >= 24 hours: 'x days, x hours' (if hours > 0) or 'x days'
    - >= 60 minutes: 'x hours x minutes' (if minutes > 0) or 'x hours'
    - >= 60 seconds: 'x minutes x seconds' (if seconds > 0) or 'x minutes'
    - else: 'x seconds'
    """
    if not isinstance(seconds, (int, float)):
        return "0 seconds"
    
    seconds = int(seconds)
    if seconds < 0:
        return "0 seconds"
    
    # Define time units (in ascending order)
    days = seconds // 86400
    remaining_seconds = seconds % 86400
    
    hours = remaining_seconds // 3600
    remaining_seconds %= 3600
    
    minutes = remaining_seconds // 60
    secs = remaining_seconds % 60
    
    months = days // 30
    days_in_month = days % 30
    
    # Build the string based on which units are significant
    if seconds < 60:
        return f"{seconds} second{'s' if seconds != 1 else ''}"
    elif seconds < 3600:  # Less than 1 hour
        if secs > 0:
            return f"{minutes} minute{'s' if minutes != 1 else ''} {secs} second{'s' if secs != 1 else ''}"
        else:
            return f"{minutes} minute{'s' if minutes != 1 else ''}"
    elif seconds < 86400:  # Less than 1 day
        if minutes > 0:
            return f"{hours} hour{'s' if hours != 1 else ''} {minutes} minute{'s' if minutes != 1 else ''}"
        else:
            return f"{hours} hour{'s' if hours != 1 else ''}"
    elif seconds < 2592000:  # Less than 30 days
        if hours > 0:
            return f"{days} day{'s' if days != 1 else ''} {hours} hour{'s' if hours != 1 else ''}"
        else:
            return f"{days} day{'s' if days != 1 else ''}"
    else:  # 30+ days
        if days_in_month > 1:
            return f"{months} month{'s' if months != 1 else ''} {days_in_month} day{'s' if days_in_month != 1 else ''}"
        elif days_in_month == 1:
            return f"{months} month{'s' if months != 1 else ''} 1 day"
        else:
            return f"{months} month{'s' if months != 1 else ''}"


# Prestige icons per 100 levels (index 0 = levels 0-99)
PRESTIGE_ICONS = [
    "❤", "✙", "✫", "✈", "✠", "♙", "⚡", "☢", "✏", "☯",
    "☃️", "۞", "✤", "♫", "♚", "❉", "Σ", "￡", "✖", "❁",
    "✚", "✯", "✆", "❥", "☾⋆⁺", "⚜", "✦", "⚝", "✉", "ツ",
    "❣", "✮", "✿", "✲", "❂", "ƒ", "$", "⋚⋚", "Φ", "✌",
]

# Prestige colors (RGB tuples for Discord embed colors)
# Levels: 0, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000+
PRESTIGE_COLORS = {
    0: (119, 119, 119),      # GRAY (§7)
    100: (255, 255, 255),    # WHITE (§f)
    200: (255, 85, 85),      # RED (§c)
    300: (255, 170, 0),      # GOLD (§6)
    400: (255, 255, 85),     # YELLOW (§e)
    500: (85, 255, 85),      # LIGHT_GREEN (§a)
    600: (0, 170, 170),      # DARK_AQUA (§3)
    700: (170, 0, 170),      # DARK_PURPLE (§5)
    800: (255, 85, 255),     # LIGHT_PURPLE (§d)
    900: None,               # Rainbow (special handling)
    1000: (255, 255, 255),   # WHITE (§f)
    1100: (170, 170, 170),   # &7 -> GRAY
    1200: (255, 85, 85),     # &c -> RED
    1300: (255, 170, 0),     # &6 -> GOLD
    1400: (255, 255, 85),    # &e -> YELLOW
    1500: (85, 255, 85),     # &a -> GREEN
    1600: (0, 170, 170),     # &3 -> DARK_AQUA
    1700: (255, 85, 255),    # &d -> LIGHT_PURPLE
    1800: (170, 0, 170),     # &5 -> DARK_PURPLE
    1900: (255, 170, 0),     # &6 -> GOLD
    2000: (0, 170, 0),       # &2 -> DARK_GREEN
    2100: (170, 170, 170),   # &7 -> GRAY
    2200: (255, 255, 85),    # &e -> YELLOW
    2300: (255, 255, 85),    # &e -> YELLOW
    2400: (85, 255, 255),    # &b -> AQUA
    2500: (85, 255, 85),     # &a -> GREEN
    2600: (85, 255, 255),    # &b -> AQUA
    2700: (255, 85, 255),    # &d -> LIGHT_PURPLE
    2800: (170, 0, 170),     # &5 -> DARK_PURPLE
    2900: (170, 0, 170),     # &5 -> DARK_PURPLE
    3000: (0, 0, 0),         # &0 -> BLACK
    3100: (255, 255, 255),   # &f -> WHITE
    3200: (255, 85, 85),     # &c -> RED
    3300: (255, 170, 0),     # &6 -> GOLD
    3400: (255, 255, 85),    # &e -> YELLOW
    3500: (85, 255, 85),     # &a -> GREEN
    3600: (0, 170, 170),     # &3 -> DARK_AQUA
    3700: (255, 85, 255),    # &d -> LIGHT_PURPLE
    3800: (170, 0, 170),     # &5 -> DARK_PURPLE
    3900: (255, 170, 0),     # &6 -> GOLD
    4000: (85, 85, 85),      # &8 -> DARK_GRAY
}


def get_prestige_icon(level: int) -> str:
    try:
        lvl = int(level)
    except Exception:
        lvl = 0
    base = (lvl // 100) * 100
    # If a raw pattern exists and contains an icon, extract it (strip color codes)
    raw = PRESTIGE_RAW_PATTERNS.get(base)
    if raw:
        stripped = re.sub(r'&[0-9a-fA-F]', '', raw)
        # Look for content inside brackets
        m = re.search(r"\[(.*?)\]", stripped)
        if m:
            inner = m.group(1)
            # remove leading digits (the level number) to get icon
            icon = re.sub(r'^[0-9]+', '', inner).strip()
            if icon:
                return icon

    # Fallback to PRESTIGE_ICONS list
    idx = max(0, lvl // 100)
    if idx >= len(PRESTIGE_ICONS):
        idx = len(PRESTIGE_ICONS) - 1
    return PRESTIGE_ICONS[idx]

def get_prestige_color(level: int) -> tuple:
    """Get RGB color tuple for a given prestige level.
    Supports levels 0-1000. Returns default dark gray for levels outside this range.
    """
    try:
        lvl = int(level)
    except Exception:
        lvl = 0

    base = (lvl // 100) * 100

    # If a raw pattern exists for this prestige base, prefer its first color code
    raw = PRESTIGE_RAW_PATTERNS.get(base)
    if raw:
        m = re.search(r'&([0-9a-fA-F])', raw)
        if m:
            code = m.group(1).lower()
            hexcol = MINECRAFT_CODE_TO_HEX.get(code)
            if hexcol:
                return hex_to_rgb(hexcol)

    # Otherwise fall back to explicit PRESTIGE_COLORS mapping
    for prestige_level in sorted(PRESTIGE_COLORS.keys(), reverse=True):
        if lvl >= prestige_level:
            color = PRESTIGE_COLORS[prestige_level]
            # Handle Rainbow (None) by returning a default color or cycling
            if color is None:
                return (255, 100, 200)
            return color

    # Fallback to gray if below 0
    return (119, 119, 119)

def get_ansi_color_code(level: int) -> str:
    """Get ANSI color code for a given prestige level."""
    color = get_prestige_color(level)
    
    # Map RGB to closest basic ANSI color for Discord compatibility
    r, g, b = color
    
    # Determine which basic ANSI color is closest
    if r > 200 and g > 200 and b > 200:
        return "\u001b[0;37m"  # White
    elif r < 100 and g < 100 and b < 100:
        return "\u001b[0;30m"  # Gray
    elif r > 200 and g < 100 and b < 100:
        return "\u001b[0;31m"  # Red
    elif r > 200 and g > 150 and b < 100:
        return "\u001b[0;33m"  # Yellow/Gold
    elif r < 100 and g > 200 and b < 100:
        return "\u001b[0;32m"  # Green
    elif r < 100 and g > 150 and b > 150:
        return "\u001b[0;36m"  # Cyan
    elif r > 150 and g < 100 and b > 150:
        return "\u001b[0;35m"  # Magenta/Pink
    elif r > 200 and g > 200 and b < 100:
        return "\u001b[0;33m"  # Yellow
    else:
        return "\u001b[0;37m"  # Default White

def make_bold_ansi(code: str) -> str:
    """Convert a basic ANSI color code to bold variant.
    Expects codes like "\u001b[0;33m" and returns "\u001b[1;33m".
    """
    if not code:
        return code
    # If already contains bold flag, return as-is
    if "1;" in code or "\u001b[1m" in code:
        return code
    # If code already contains bold or is empty, return it
    if not code:
        return code
    if "1;" in code or "\u001b[1m" in code:
        return code
    # For any CSI like '\x1b[...m', insert '1;' after the '[' if not present
    m = re.match(r"^(\x1b\[)(?!1;)(.*)m$", code)
    if m:
        return f"{m.group(1)}1;{m.group(2)}m"
    return code


# Mapping of Minecraft color codes (§) to approximate ANSI codes for inline coloring
# Official Minecraft-ish main hex colors for § codes (main hex values)
MINECRAFT_CODE_TO_HEX = {
    '0': '#000000',
    '1': '#0000AA',
    '2': '#00AA00',
    '3': '#00AAAA',
    '4': '#AA0000',
    '5': '#AA00AA',
    '6': '#FFAA00',
    '7': '#AAAAAA',
    '8': '#555555',
    '9': '#5555FF',
    'a': '#55FF55',
    'b': '#55FFFF',
    'c': '#FF5555',
    'd': '#FF55FF',
    'e': '#FFFF55',
    'f': '#FFFFFF',
}

# Minecraft color name to hex (from Hypixel API)
MINECRAFT_NAME_TO_HEX = {
    'BLACK': '#000000',
    'DARK_BLUE': '#0000AA',
    'DARK_GREEN': '#00AA00',
    'DARK_AQUA': '#00AAAA',
    'DARK_RED': '#AA0000',
    'DARK_PURPLE': '#AA00AA',
    'GOLD': '#FFAA00',
    'GRAY': '#AAAAAA',
    'DARK_GRAY': '#555555',
    'BLUE': '#5555FF',
    'GREEN': '#55FF55',
    'AQUA': '#55FFFF',
    'RED': '#FF5555',
    'LIGHT_PURPLE': '#FF55FF',
    'YELLOW': '#FFFF55',
    'WHITE': '#FFFFFF',
}

def hex_to_rgb(h: str) -> tuple:
    h = h.lstrip('#')
    return tuple(int(h[i:i+2], 16) for i in (0, 2, 4))

def hex_to_ansi(h: str, background: bool = False) -> str:
    r, g, b = hex_to_rgb(h)
    if background:
        return f"\u001b[48;2;{r};{g};{b}m"
    return f"\u001b[38;2;{r};{g};{b}m"

def rgb_to_ansi256_index(r: int, g: int, b: int) -> int:
    """Convert RGB 0-255 to xterm-256 color index."""
    # Grayscale approximation
    if r == g == b:
        if r < 8:
            return 16
        if r > 248:
            return 231
        return 232 + int((r - 8) / 247 * 24)

    # 6x6x6 color cube
    ri = int(round((r / 255) * 5))
    gi = int(round((g / 255) * 5))
    bi = int(round((b / 255) * 5))
    return 16 + (36 * ri) + (6 * gi) + bi

def rgb_to_ansi256_escape(r: int, g: int, b: int, background: bool = False) -> str:
    idx = rgb_to_ansi256_index(r, g, b)
    if background:
        return f"\u001b[48;5;{idx}m"
    return f"\u001b[38;5;{idx}m"

def hex_to_ansi256(h: str, background: bool = False) -> str:
    r, g, b = hex_to_rgb(h)
    return rgb_to_ansi256_escape(r, g, b, background=background)

# Map Minecraft color codes to chosen xterm-256 indices for clearer, distinct colors
# These indices were selected to improve visual separation between gold/yellow/green
_MINECRAFT_256_INDEX = {
    '0': 16,   # black
    '1': 19,   # dark_blue
    '2': 28,   # dark_green
    '3': 37,   # dark_aqua
    '4': 124,  # dark_red
    '5': 127,  # dark_purple
    '6': 214,  # gold/orange
    '7': 248,  # gray
    '8': 239,  # dark_gray
    '9': 75,   # blue
    'a': 46,   # bright green
    'b': 51,   # aqua
    'c': 203,  # red
    'd': 201,  # pink
    'e': 227,  # yellow
    'f': 15,   # white
}

MINECRAFT_CODE_TO_ANSI_SGR = {k: f"\u001b[38;5;{idx}m" for k, idx in _MINECRAFT_256_INDEX.items()}

# Keep the 24-bit hex map for embed accent colors
MINECRAFT_CODE_TO_ANSI = {k: hex_to_ansi(v) for k, v in MINECRAFT_CODE_TO_HEX.items()}

# Patterns for multi-colored prestige prefixes. Key = prestige base (e.g. 1900),
# For flexibility we store raw Minecraft-style color sequences per prestige.
# Each string uses '&' followed by hex code, e.g. '&c[&61&e9&a0&30&5✖&d]'.
# The runtime parser below converts those into (code, text) pieces.
PRESTIGE_RAW_PATTERNS = {
    0: "&7[0❤]",
    100: "&f[100✙]",
    200: "&c[200✫]",
    300: "&6[300✈]",
    400: "&e[400✠]",
    500: "&a[500♙]",
    600: "&3[600⚡]",
    700: "&5[700✠]",
    800: "&d[800⏹]",
    900: "&9[&69&e0&20&3✏&d]",
    1000: "&0[&f1000☯&0]",
    1100: "&0[&71100☃️&0]",
    1200: "&0[&c1200۞&0]",
    1300: "&0[&61300✤&0]",
    1400: "&0[&e1400♫&0]",
    1500: "&0[&a1500♚&0]",
    1600: "&0[&31600❉&0]",
    1700: "&0[&d1700Σ&0]",
    1800: "&0[&51800￡&0]",
    1900: "&c[&61&e9&a0&30&5✖&d]",
    2000: "&0[2&80&700&f❁]",
    2100: "&f[2&710&80&0✚]",
    2200: "&f[2&e20&60&c✯]",
    2300: "&6[2&e30&a0&b✆]",
    2400: "&a[2&b40&30&5❥]",
    2500: "&f[2&a500&2☾⋆⁺]",
    2600: "&f[2&b60&30⚜&1]",
    2700: "&f[2&d700&5✦]",
    2800: "&3[2&580&d0&e⚝]",
    2900: "&d[&52&39&a0&e0&6✉&c]",
    3000: "&f[&03&80&00&80&0ツ&f]",
    3100: "&0[&F3&71&F0&70&F❣&0]",
    3200: "&0[&c3&42&c0&40&c✮&0]",
    3300: "&0[&63&c3&60&c0&6✿&0]",
    3400: "&0[&e3&64&e0&60&e✲&0]",
    3500: "&0[&a3&25&a0&20&a❂&0]",
    3600: "&0[&33&16&30&10&3ƒ&0]",
    3700: "&0[&d3&57&d0&50&d$&0]",
    3800: "&0[&53&48&50&40&5⋚⋚&0]",
    3900: "&4[&63&e9&20&10&5Φ&d]",
    4000: "&0[4&80&70&80&0✌]",
}


def _parse_raw_pattern(raw: str) -> list:
    """Parse a raw pattern into list of (code, text) pieces."""
    parts = []
    cur_code = None
    buf = ''
    i = 0
    while i < len(raw):
        ch = raw[i]
        if ch == '&' and i + 1 < len(raw):
            if buf:
                parts.append((cur_code or 'f', buf))
                buf = ''
            cur_code = raw[i+1].lower()
            i += 2
            continue
        else:
            buf += ch
            i += 1
    if buf:
        parts.append((cur_code or 'f', buf))
    return parts


def render_prestige_with_text(level: int, icon: str, ign: str, suffix: str = "", ign_color: str = None, 
                              guild_tag: str = None, guild_color: str = None, two_line: bool = False) -> io.BytesIO:
    """Render a prestige prefix with IGN, optional guild tag, and optional suffix text.
    
    Returns a BytesIO containing the rendered PNG image.
    If Pillow is not available, raises RuntimeError.
    ign_color: Hex color code for the IGN (e.g., '#FF5555')
    guild_tag: Guild tag to display after username (e.g., 'QUEBEC')
    guild_color: Color name from Hypixel API (e.g., 'DARK_AQUA')
    two_line: If True, formats as [level icon] username [guild] on first line, suffix on second line
    """
    if Image is None:
        raise RuntimeError("Pillow not available")
    
    base = (level // 100) * 100
    raw = PRESTIGE_RAW_PATTERNS.get(base)
    
    segments = []
    
    if raw:
        # Parse the pattern and replace the level number
        parts = _parse_raw_pattern(raw)
        
        # Build segments with the actual level
        concat = ''.join(t for (_, t) in parts)
        m = re.search(r"\d+", concat)
        
        if m:
            num_start, num_end = m.start(), m.end()
            pos = 0
            replaced = False
            
            for code, text in parts:
                part_start = pos
                part_end = pos + len(text)
                pos = part_end
                hexcol = MINECRAFT_CODE_TO_HEX.get(code.lower(), '#FFFFFF')
                
                if part_end <= num_start or part_start >= num_end:
                    segments.append((hexcol, text))
                    continue
                
                # Prefix before number
                prefix_len = max(0, num_start - part_start)
                if prefix_len > 0:
                    segments.append((hexcol, text[:prefix_len]))
                
                # Replace with actual level
                if not replaced:
                    # Check if this is a rainbow prestige
                    rainbow_bases = {k for k, v in PRESTIGE_COLORS.items() if v is None}
                    if base in rainbow_bases:
                        # Build rainbow colors
                        colors_in_span = []
                        pos2 = 0
                        for c_code, c_text in parts:
                            part_s = pos2
                            part_e = pos2 + len(c_text)
                            pos2 = part_e
                            overlap_s = max(part_s, num_start)
                            overlap_e = min(part_e, num_end)
                            if overlap_e > overlap_s:
                                hexcol_span = MINECRAFT_CODE_TO_HEX.get(c_code.lower(), '#FFFFFF')
                                for _ in range(overlap_e - overlap_s):
                                    colors_in_span.append(hexcol_span)
                        
                        if not colors_in_span:
                            RAINBOW_CODES = ['c', '6', 'e', 'a', 'b', 'd', '9', '3']
                            colors_in_span = [MINECRAFT_CODE_TO_HEX.get(c, '#FFFFFF') for c in RAINBOW_CODES]
                        
                        # Apply colors to level digits
                        for i, ch in enumerate(str(level)):
                            col = colors_in_span[i % len(colors_in_span)]
                            segments.append((col, ch))
                    else:
                        segments.append((hexcol, str(level)))
                    replaced = True
                
                # Suffix after number
                suffix_start_in_part = max(0, num_end - part_start)
                if suffix_start_in_part < len(text):
                    segments.append((hexcol, text[suffix_start_in_part:]))
        else:
            # No number found, use pattern as-is
            segments = [(MINECRAFT_CODE_TO_HEX.get(code, '#FFFFFF'), text) for code, text in parts]
    else:
        # Fallback: simple colored bracket
        color = get_prestige_color(level)
        hexcol = '#{:02x}{:02x}{:02x}'.format(*color)
        segments = [(hexcol, f"[{level}{icon}]")]
    
    # Add IGN with custom color if specified
    ign_hex = ign_color if ign_color else MINECRAFT_CODE_TO_HEX.get('f', '#FFFFFF')
    segments.append((ign_hex, f" {ign}"))
    
    # Add guild tag if provided
    if guild_tag and guild_color:
        guild_hex = MINECRAFT_NAME_TO_HEX.get(guild_color.upper(), '#FFFFFF')
        segments.append((guild_hex, f" [{guild_tag}]"))
    elif guild_tag:
        segments.append((MINECRAFT_CODE_TO_HEX.get('f', '#FFFFFF'), f" [{guild_tag}]"))
    
    if two_line and suffix:
        # Two-line format: first line ends after guild tag, second line is the suffix
        return _render_text_segments_to_image_multiline([segments, [(MINECRAFT_CODE_TO_HEX.get('f', '#FFFFFF'), suffix)]])
    elif suffix:
        # Single line format: append suffix with " - " prefix
        segments.append((MINECRAFT_CODE_TO_HEX.get('f', '#FFFFFF'), suffix))
    
    return _render_text_segments_to_image(segments)


def _render_text_segments_to_image(segments: list, font=None, padding=(8,6)) -> io.BytesIO:
    """Render colored text segments to a PNG and return a BytesIO."""
    if Image is None:
        raise RuntimeError("Pillow not available")
    if font is None:
        try:
            font = ImageFont.truetype("DejaVuSans.ttf", 18)
        except Exception:
            font = ImageFont.load_default()

    # Measure total size
    total_w = 0
    max_h = 0
    draw_dummy = ImageDraw.Draw(Image.new('RGBA', (1,1)))
    for color_hex, text in segments:
        bbox = draw_dummy.textbbox((0, 0), text, font=font)
        w = bbox[2] - bbox[0]
        h = bbox[3] - bbox[1]
        total_w += w
        max_h = max(max_h, h)

    img_w = total_w + padding[0]*2
    img_h = max_h + padding[1]*2
    img = Image.new('RGBA', (img_w, img_h), (0,0,0,0))
    draw = ImageDraw.Draw(img)

    x = padding[0]
    y = padding[1]
    for color_hex, text in segments:
        try:
            color = tuple(int(color_hex.lstrip('#')[i:i+2], 16) for i in (0,2,4))
        except Exception:
            color = (255,255,255)
        draw.text((x,y), text, font=font, fill=color)
        bbox = draw.textbbox((x, y), text, font=font)
        w = bbox[2] - bbox[0]
        x += w

    out = io.BytesIO()
    img.save(out, format='PNG')
    out.seek(0)
    return out


def _render_text_segments_to_image_multiline(lines: list, font=None, padding=(8,6), line_spacing=2) -> io.BytesIO:
    """Render multiple lines of colored text segments to a PNG.
    
    Args:
        lines: List of segment lists, where each segment list is [(color_hex, text), ...]
        font: Font to use
        padding: Horizontal and vertical padding
        line_spacing: Additional vertical space between lines
    """
    if Image is None:
        raise RuntimeError("Pillow not available")
    if font is None:
        try:
            font = ImageFont.truetype("DejaVuSans.ttf", 26)
        except Exception:
            font = ImageFont.load_default()

    draw_dummy = ImageDraw.Draw(Image.new('RGBA', (1,1)))
    
    # Measure each line
    line_widths = []
    line_heights = []
    for segments in lines:
        line_w = 0
        line_h = 0
        for color_hex, text in segments:
            bbox = draw_dummy.textbbox((0, 0), text, font=font)
            w = bbox[2] - bbox[0]
            h = bbox[3] - bbox[1]
            line_w += w
            line_h = max(line_h, h)
        line_widths.append(line_w)
        line_heights.append(line_h)
    
    # Calculate total image size
    max_w = max(line_widths) if line_widths else 0
    total_h = sum(line_heights) + (len(lines) - 1) * line_spacing if lines else 0
    
    img_w = max_w + padding[0] * 2
    img_h = total_h + padding[1] * 2
    img = Image.new('RGBA', (img_w, img_h), (0,0,0,0))
    draw = ImageDraw.Draw(img)
    
    # Draw each line (center each line horizontally)
    y = padding[1]
    for line_idx, segments in enumerate(lines):
        # Calculate starting x position to center this line
        line_width = line_widths[line_idx]
        x = (img_w - line_width) // 2
        
        for color_hex, text in segments:
            try:
                color = tuple(int(color_hex.lstrip('#')[i:i+2], 16) for i in (0,2,4))
            except Exception:
                color = (255,255,255)
            draw.text((x, y), text, font=font, fill=color)
            bbox = draw.textbbox((x, y), text, font=font)
            w = bbox[2] - bbox[0]
            x += w
        y += line_heights[line_idx] + line_spacing
    
    out = io.BytesIO()
    img.save(out, format='PNG')
    out.seek(0)
    return out


def render_stat_box(label: str, value: str, width: int = 200, height: int = 80):
    """Render a single stat box with label and value on black background."""
    if Image is None:
        raise RuntimeError("Pillow not available")
    
    img = Image.new('RGB', (width, height), (0, 0, 0))
    draw = ImageDraw.Draw(img)
    
    try:
        label_font = ImageFont.truetype("DejaVuSans-Bold.ttf", 16)
        value_font = ImageFont.truetype("DejaVuSans.ttf", 24)
    except Exception:
        try:
            label_font = ImageFont.truetype("DejaVuSans.ttf", 16)
            value_font = ImageFont.truetype("DejaVuSans.ttf", 24)
        except Exception:
            label_font = ImageFont.load_default()
            value_font = ImageFont.load_default()
    
    # Draw label (centered horizontally, near top)
    label_bbox = draw.textbbox((0, 0), label, font=label_font)
    label_w = label_bbox[2] - label_bbox[0]
    label_x = (width - label_w) // 2
    draw.text((label_x, 15), label, font=label_font, fill=(200, 200, 200))
    
    # Draw value (centered horizontally, below label)
    value_bbox = draw.textbbox((0, 0), value, font=value_font)
    value_w = value_bbox[2] - value_bbox[0]
    value_x = (width - value_w) // 2
    draw.text((value_x, 45), value, font=value_font, fill=(255, 255, 255))
    
    return img


def create_stats_composite_image(level: int, icon: str, ign: str, tab_name: str, 
                                  wins: int, losses: int, wl_ratio: float,
                                  kills: int, deaths: int, kd_ratio: float, 
                                  ign_color: str = None, guild_tag: str = None, guild_color: str = None) -> io.BytesIO:
    """Create a composite image with title and 2x3 grid of stats."""
    if Image is None:
        raise RuntimeError("Pillow not available")
    
    # Generate title image with custom username color and guild tag (two-line format)
    second_line = f"{tab_name.title()} Stats"
    title_io = render_prestige_with_text(level, icon, ign, second_line, ign_color, guild_tag, guild_color, two_line=True)
    title_img = Image.open(title_io)
    
    # Generate stat boxes
    stat_width = 200
    stat_height = 80
    spacing = 10
    
    stat_boxes = [
        render_stat_box("Wins", str(wins), stat_width, stat_height),
        render_stat_box("Losses", str(losses), stat_width, stat_height),
        render_stat_box("W/L Ratio", str(wl_ratio), stat_width, stat_height),
        render_stat_box("Kills", str(kills), stat_width, stat_height),
        render_stat_box("Deaths", str(deaths), stat_width, stat_height),
        render_stat_box("K/D Ratio", str(kd_ratio), stat_width, stat_height),
    ]
    
    # Calculate composite dimensions
    grid_width = stat_width * 3 + spacing * 2
    grid_height = stat_height * 2 + spacing
    
    # Don't scale up the title, only scale down if it's too wide
    title_width = title_img.width
    title_height = title_img.height
    if title_width > grid_width:
        scale_factor = grid_width / title_width
        title_width = grid_width
        title_height = int(title_img.height * scale_factor)
        title_img = title_img.resize((title_width, title_height), Image.LANCZOS)
    
    # Calculate horizontal centering offset for title
    title_x_offset = (grid_width - title_width) // 2
    
    composite_width = grid_width
    bottom_padding = 20  # Extra space at bottom
    composite_height = title_height + spacing + grid_height + bottom_padding
    
    # Create composite image with black background
    composite = Image.new('RGB', (composite_width, composite_height), (0, 0, 0))
    
    # Paste title centered horizontally
    composite.paste(title_img, (title_x_offset, 0), title_img if title_img.mode == 'RGBA' else None)
    
    # Paste stat boxes in 2x3 grid (no offset needed since grid matches width)
    y_offset = title_height + spacing
    
    for i, stat_box in enumerate(stat_boxes):
        row = i // 3
        col = i % 3
        x = col * (stat_width + spacing)
        y = y_offset + row * (stat_height + spacing)
        composite.paste(stat_box, (x, y))
    
    # Save to BytesIO
    out = io.BytesIO()
    composite.save(out, format='PNG')
    out.seek(0)
    return out


def create_leaderboard_image(tab_name: str, metric_label: str, leaderboard_data: list) -> io.BytesIO:
    """Create a leaderboard image similar to the SheepWars format.
    
    Args:
        tab_name: The period/tab name (e.g., "Lifetime", "Session")
        metric_label: The stat being displayed (e.g., "Magic Wool", "Kills")
        leaderboard_data: List of tuples (rank, player_name, level, icon, ign_color, value, is_playtime)
    
    Returns:
        BytesIO containing the rendered PNG image
    """
    if Image is None:
        raise RuntimeError("Pillow not available")
    
    # Dimensions and styling
    row_height = 50
    header_height = 60  # Reduced from 80 since we only have one title line now
    padding = 15
    
    # Column widths
    pos_width = 80
    player_width = 380
    stat_width = 180
    
    total_width = pos_width + player_width + stat_width + (padding * 4)
    num_rows = len(leaderboard_data)
    # Add extra padding at the bottom to ensure last row isn't cut off
    total_height = header_height + (row_height * (num_rows + 1)) + (padding * 2)
    
    # Create image with dark background
    img = Image.new('RGB', (total_width, total_height), (30, 30, 35))
    draw = ImageDraw.Draw(img)
    
    # Fonts
    try:
        title_font = ImageFont.truetype("DejaVuSans-Bold.ttf", 28)
        header_font = ImageFont.truetype("DejaVuSans-Bold.ttf", 20)
        data_font = ImageFont.truetype("DejaVuSans.ttf", 18)
        prestige_font = ImageFont.truetype("DejaVuSans.ttf", 16)
    except Exception:
        title_font = ImageFont.load_default()
        header_font = ImageFont.load_default()
        data_font = ImageFont.load_default()
        prestige_font = ImageFont.load_default()
    
    # Draw title - just "{tab_name} {metric_label} Leaderboard"
    title_text = f"{tab_name} {metric_label} Leaderboard"
    
    # Calculate title position
    bbox = draw.textbbox((0, 0), title_text, font=title_font)
    title_width = bbox[2] - bbox[0]
    title_x = (total_width - title_width) // 2
    
    draw.text((title_x, 20), title_text, font=title_font, fill=(220, 220, 220))
    
    # Draw header row background (darker shade)
    header_y = header_height
    draw.rectangle([(0, header_y), (total_width, header_y + row_height)], fill=(45, 45, 50))
    
    # Draw header text
    header_texts = ["Pos", "Player", metric_label]
    header_x_positions = [
        padding + pos_width // 2,
        padding + pos_width + padding + player_width // 2,
        padding + pos_width + padding + player_width + padding + stat_width // 2
    ]
    
    for text, x_pos in zip(header_texts, header_x_positions):
        bbox = draw.textbbox((0, 0), text, font=header_font)
        text_width = bbox[2] - bbox[0]
        draw.text((x_pos - text_width // 2, header_y + 15), text, font=header_font, fill=(200, 200, 200))
    
    # Draw data rows
    y_offset = header_y + row_height
    
    for rank, player_name, level, icon, ign_color, value, is_playtime in leaderboard_data:
        # Alternate row colors for better readability
        if rank % 2 == 0:
            draw.rectangle([(0, y_offset), (total_width, y_offset + row_height)], fill=(38, 38, 43))
        else:
            draw.rectangle([(0, y_offset), (total_width, y_offset + row_height)], fill=(30, 30, 35))
        
        # Draw rank number
        rank_text = f"#{rank}"
        bbox = draw.textbbox((0, 0), rank_text, font=data_font)
        rank_width = bbox[2] - bbox[0]
        rank_x = padding + pos_width // 2 - rank_width // 2
        draw.text((rank_x, y_offset + 15), rank_text, font=data_font, fill=(255, 255, 255))
        
        # Draw player name with prestige prefix
        player_x = padding + pos_width + padding
        
        # Get prestige colors - need to handle bracket color vs content color
        base = (level // 100) * 100
        raw = PRESTIGE_RAW_PATTERNS.get(base)
        bracket_color = (170, 170, 170)  # Default gray for brackets
        content_color = (170, 170, 170)  # Default gray for content
        
        if raw:
            # Extract color codes - first for bracket, second for content if different
            color_codes = re.findall(r'&([0-9a-fA-F])', raw)
            if len(color_codes) >= 2:
                # If we have at least 2 color codes, use first for bracket, second for content
                code1 = color_codes[0].lower()
                code2 = color_codes[1].lower()
                hexcol1 = MINECRAFT_CODE_TO_HEX.get(code1)
                hexcol2 = MINECRAFT_CODE_TO_HEX.get(code2)
                if hexcol1:
                    bracket_color = tuple(int(hexcol1.lstrip('#')[i:i+2], 16) for i in (0,2,4))
                if hexcol2:
                    content_color = tuple(int(hexcol2.lstrip('#')[i:i+2], 16) for i in (0,2,4))
            elif len(color_codes) == 1:
                # If only one color code, use it for both
                code = color_codes[0].lower()
                hexcol = MINECRAFT_CODE_TO_HEX.get(code)
                if hexcol:
                    bracket_color = tuple(int(hexcol.lstrip('#')[i:i+2], 16) for i in (0,2,4))
                    content_color = bracket_color
        
        # Draw prestige with proper colors: [<bracket>level icon<bracket>]
        # Draw opening bracket
        draw.text((player_x, y_offset + 15), "[", font=prestige_font, fill=bracket_color)
        bbox = draw.textbbox((player_x, y_offset + 15), "[", font=prestige_font)
        current_x = bbox[2] - bbox[0]
        
        # Draw level and icon in content color
        level_icon_text = f"{level}{icon}"
        draw.text((player_x + current_x, y_offset + 15), level_icon_text, font=prestige_font, fill=content_color)
        bbox = draw.textbbox((player_x + current_x, y_offset + 15), level_icon_text, font=prestige_font)
        current_x += bbox[2] - bbox[0]
        
        # Draw closing bracket
        draw.text((player_x + current_x, y_offset + 15), "]", font=prestige_font, fill=bracket_color)
        bbox = draw.textbbox((player_x + current_x, y_offset + 15), "]", font=prestige_font)
        current_x += bbox[2] - bbox[0]
        
        prestige_width = current_x
        
        # Draw player name with custom color
        try:
            if ign_color:
                player_color = tuple(int(ign_color.lstrip('#')[i:i+2], 16) for i in (0,2,4))
            else:
                player_color = (255, 255, 255)
        except Exception:
            player_color = (255, 255, 255)
        
        name_x = player_x + prestige_width + 5
        draw.text((name_x, y_offset + 15), player_name, font=data_font, fill=player_color)
        
        # Draw stat value
        if is_playtime:
            formatted_value = format_playtime(int(value))
        else:
            # Format large numbers with commas
            if isinstance(value, float):
                formatted_value = f"{value:,.2f}" if value % 1 else f"{int(value):,}"
            else:
                formatted_value = f"{int(value):,}"
        
        value_bbox = draw.textbbox((0, 0), formatted_value, font=data_font)
        value_width = value_bbox[2] - value_bbox[0]
        value_x = padding + pos_width + padding + player_width + padding + stat_width // 2 - value_width // 2
        draw.text((value_x, y_offset + 15), formatted_value, font=data_font, fill=(255, 255, 255))
        
        y_offset += row_height
    
    # Save to BytesIO
    out = io.BytesIO()
    img.save(out, format='PNG')
    out.seek(0)
    return out


def render_prestige_range_image(base: int, end_display: int) -> io.BytesIO:
    """Render an image showing the colored start and end prestige brackets from raw pattern."""
    raw = PRESTIGE_RAW_PATTERNS.get(base)
    if not raw:
        # Fallback to simple text
        parts = [(MINECRAFT_CODE_TO_HEX.get('f', '#FFFFFF'), f'[{base}] - [{end_display}]')]
        return _render_text_segments_to_image(parts)

    parts = _parse_raw_pattern(raw)

    def _build_replaced_segments(parts, replacement_str, rainbow=False):
        """Replace the first numeric span in the concatenated parts with replacement_str once, preserving color segments.

        If `rainbow` is True, expand the replacement into per-character colored segments cycling a rainbow palette.
        """
        concat = ''.join(t for (_, t) in parts)
        m = re.search(r"\d+", concat)
        if not m:
            return [(MINECRAFT_CODE_TO_HEX.get(code.lower(), '#FFFFFF'), text) for code, text in parts]

        num_start, num_end = m.start(), m.end()
        out_parts = []
        pos = 0
        replaced = False
        for code, text in parts:
            part_start = pos
            part_end = pos + len(text)
            pos = part_end
            hexcol = MINECRAFT_CODE_TO_HEX.get(code.lower(), '#FFFFFF')

            if part_end <= num_start or part_start >= num_end:
                out_parts.append((hexcol, text))
                continue

            # prefix
            prefix_len = max(0, num_start - part_start)
            if prefix_len > 0:
                prefix = text[:prefix_len]
                out_parts.append((hexcol, prefix))

            # replacement
            if not replaced:
                if rainbow:
                    # Build the original color sequence that covered the numeric span
                    colors_in_span = []
                    span_pos = 0
                    # Re-iterate to collect per-char colors within the numeric span
                    pos2 = 0
                    for c_code, c_text in parts:
                        part_s = pos2
                        part_e = pos2 + len(c_text)
                        pos2 = part_e
                        overlap_s = max(part_s, num_start)
                        overlap_e = min(part_e, num_end)
                        if overlap_e > overlap_s:
                            hex_here = MINECRAFT_CODE_TO_HEX.get(c_code.lower(), '#FFFFFF')
                            # number of covered chars in original
                            count = overlap_e - overlap_s
                            colors_in_span.extend([hex_here] * count)

                    if not colors_in_span:
                        # fallback rainbow cycle
                        RAINBOW_CODES = ['c', '6', 'e', 'a', 'b', 'd', '9', '3']
                        colors_in_span = [MINECRAFT_CODE_TO_HEX.get(code, '#FFFFFF') for code in RAINBOW_CODES]

                    # Apply colors across the replacement string, repeating as needed
                    repl = str(replacement_str)
                    for i, ch in enumerate(repl):
                        col = colors_in_span[i % len(colors_in_span)]
                        out_parts.append((col, ch))
                else:
                    out_parts.append((hexcol, replacement_str))
                replaced = True

            # suffix
            suffix_start_in_part = max(0, num_end - part_start)
            if suffix_start_in_part < len(text):
                suffix = text[suffix_start_in_part:]
                out_parts.append((hexcol, suffix))

        return out_parts

    # Choose fallback icons for bases where emoji fonts may be missing
    BAD_ICON_BASES = {800, 1200, 1800, 2800, 3800}

    # Determine if this prestige base should be rainbow (PRESTIGE_COLORS maps to None)
    rainbow_bases = {k for k, v in PRESTIGE_COLORS.items() if v is None}

    start_segments = _build_replaced_segments(parts, str(base), rainbow=(base in rainbow_bases))
    end_segments = _build_replaced_segments(parts, str(end_display), rainbow=(end_display in rainbow_bases))

    # If problematic base, replace any non-ascii icon with fallback from PRESTIGE_ICONS
    if base in BAD_ICON_BASES or end_display in BAD_ICON_BASES:
        def _replace_bad_icons(segments, base_val):
            res = []
            for col, txt in segments:
                # replace any non-basic symbol at end inside brackets with fallback
                newtxt = re.sub(r"\[(\s*\d+)([^\d\]]+)\]", lambda m: f"[{m.group(1)}{PRESTIGE_ICONS[(base_val//100) % len(PRESTIGE_ICONS)]}]", txt)
                res.append((col, newtxt))
            return res
        start_segments = _replace_bad_icons(start_segments, base)
        end_segments = _replace_bad_icons(end_segments, base)

    combined = []
    combined.extend(start_segments)
    combined.append((MINECRAFT_CODE_TO_HEX.get('f', '#FFFFFF'), ' \u0013 '))
    combined.extend(end_segments)

    return _render_text_segments_to_image(combined)


def render_all_prestiges_combined(spacing: int = 6) -> io.BytesIO:
    """Render all prestiges as individual images and combine them vertically into one PNG."""
    if Image is None:
        raise RuntimeError("Pillow not available")

    # Build a 4-column layout where columns are offsets [0,1000,2000,3000]
    offsets = [0, 1000, 2000, 3000]

    # Rows are the base mods 0,100,...,900 (we limit to prestiges up to 4000)
    base_mods = [i * 100 for i in range(0, 10)]

    # Prepare grid of images (rows x cols). Use placeholder transparent images for missing cells.
    grid = []
    for base_mod in base_mods:
        row_imgs = []
        for off in offsets:
            key = base_mod + off
            if key in PRESTIGE_RAW_PATTERNS:
                try:
                    imgio = render_prestige_range_image(key, key + 99)
                    imgio.seek(0)
                    im = Image.open(imgio).convert('RGBA')
                except Exception:
                    im = Image.new('RGBA', (200, 40), (0,0,0,0))
            else:
                im = Image.new('RGBA', (200, 40), (0,0,0,0))
            row_imgs.append(im)
        grid.append(row_imgs)

    # Compute uniform cell size
    max_w = max((im.width for row in grid for im in row), default=200)
    max_h = max((im.height for row in grid for im in row), default=40)

    # Optional title at the top
    title_text = "Wool Games prestiges 0-4000"
    try:
        title_font = ImageFont.truetype("DejaVuSans-Bold.ttf", 22)
    except Exception:
        title_font = ImageFont.load_default()
    title_bbox = Image.new('RGBA', (1,1))
    draw_tmp = ImageDraw.Draw(title_bbox)
    tb = draw_tmp.textbbox((0,0), title_text, font=title_font)
    title_h = tb[3] - tb[1] + 12

    cols = len(offsets)
    rows = len(grid)

    total_w = cols * max_w + spacing * (cols - 1)
    total_h = title_h + rows * max_h + spacing * (rows - 1)

    combined = Image.new('RGBA', (total_w, total_h), (0,0,0,0))

    # Draw title centered with subtle shadow
    draw = ImageDraw.Draw(combined)
    title_x = (total_w) // 2
    draw.text((title_x+1, 6), title_text, font=title_font, fill=(0,0,0), anchor='mm')
    draw.text((title_x, 5), title_text, font=title_font, fill=(220,220,220), anchor='mm')

    y = title_h
    for row in grid:
        x = 0
        for im in row:
            # center each image within its cell
            paste_x = x + (max_w - im.width) // 2
            paste_y = y + (max_h - im.height) // 2
            combined.paste(im, (paste_x, paste_y), im)
            x += max_w + spacing
        y += max_h + spacing

    out = io.BytesIO()
    combined.save(out, format='PNG')
    out.seek(0)
    return out



def format_prestige_ansi(level: int, icon: str) -> str:
    """Return an ANSI-colored prestige bracket+level+icon string.

    If a multi-color pattern exists for the prestige base (e.g. 1900), use it;
    otherwise color the whole bracket using the single prestige color.
    """
    reset = "\u001b[0m"
    try:
        lvl = int(level)
    except Exception:
        lvl = 0
    base = (lvl // 100) * 100
    # If a raw pattern exists, parse it into (code, text) pieces
    if base in PRESTIGE_RAW_PATTERNS:
        raw = PRESTIGE_RAW_PATTERNS[base]
        parts = []
        cur_code = None
        buf = ''
        i = 0
        while i < len(raw):
            ch = raw[i]
            if ch == '&' and i + 1 < len(raw):
                # flush buf
                if buf:
                    parts.append((cur_code or 'f', buf))
                    buf = ''
                cur_code = raw[i+1].lower()
                i += 2
                continue
            else:
                buf += ch
                i += 1
        if buf:
            parts.append((cur_code or 'f', buf))

        out = []
        for code, text in parts:
            # Use chosen xterm-256 SGR for inline/code-block rendering
            sgr = MINECRAFT_CODE_TO_ANSI_SGR.get(code.lower(), "\u001b[37m")
            out.append(make_bold_ansi(sgr) + text)

        joined = ''.join(out) + reset
        # When a raw pattern exists we trust it includes the correct icon and colors.
        return joined

    # Fallback: color whole bracket with single color for the level
    ansi = get_ansi_color_code(level)
    bold = make_bold_ansi(ansi)
    return f"{bold}[{level}{icon}]{reset}"


async def _send_paged_ansi_followups(interaction: discord.Interaction, lines: list[str], block: str = 'ansi'):
    """Send potentially-long ANSI lines as one or more followup messages, each <= 2000 chars.

    Splits `lines` into code-block chunks and sends them via `interaction.followup.send`.
    Falls back to sanitized plain text if sending fails.
    """
    wrapper_open = f"```{block}\n"
    wrapper_close = "\n```"
    max_len = 2000

    chunks = []
    cur_lines = []
    # start with the wrapper overhead
    cur_len = len(wrapper_open) + len(wrapper_close)

    for ln in lines:
        ln_with_nl = ln + "\n"
        lnlen = len(ln_with_nl)
        if cur_len + lnlen > max_len:
            # flush current chunk
            if cur_lines:
                chunks.append("".join(cur_lines).rstrip('\n'))
            cur_lines = [ln_with_nl]
            cur_len = len(wrapper_open) + len(wrapper_close) + lnlen
        else:
            cur_lines.append(ln_with_nl)
            cur_len += lnlen

    if cur_lines:
        chunks.append("".join(cur_lines).rstrip('\n'))

    # Send chunks as followups
    for chunk in chunks:
        content = wrapper_open + chunk + wrapper_close
        try:
            await interaction.followup.send(content)
        except Exception:
            # fallback: send sanitized text without ANSI wrapper
            try:
                await interaction.followup.send(sanitize_output(chunk))
            except Exception:
                # give up silently
                pass

def load_tracked_users():
    if not os.path.exists(TRACKED_FILE):
        return []
    with open(TRACKED_FILE, "r", encoding="utf-8") as f:
        lines = [l.strip() for l in f.readlines() if l.strip()]
    return lines

def add_tracked_user(ign: str) -> bool:
    users = load_tracked_users()
    key = ign.casefold()
    for u in users:
        if u.casefold() == key:
            return False
    # append
    with open(TRACKED_FILE, "a", encoding="utf-8") as f:
        f.write(ign + "\n")
    return True

def load_user_links():
    """Load username -> Discord user ID mappings from JSON file"""
    if not os.path.exists(USER_LINKS_FILE):
        return {}
    try:
        with open(USER_LINKS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def save_user_links(links: dict):
    """Save username -> Discord user ID mappings to JSON file"""
    with open(USER_LINKS_FILE, "w", encoding="utf-8") as f:
        json.dump(links, f, indent=2)

def link_user_to_ign(discord_user_id: int, ign: str):
    """Link a Discord user ID to a Minecraft username (case-insensitive)"""
    links = load_user_links()
    # Store with original case but search case-insensitively
    links[ign.casefold()] = str(discord_user_id)
    save_user_links(links)

def is_user_authorized(discord_user_id: int, ign: str) -> bool:
    """Check if a Discord user is authorized to manage a username"""
    links = load_user_links()
    key = ign.casefold()
    return links.get(key) == str(discord_user_id)

def remove_tracked_user(ign: str) -> bool:
    """Remove a username from tracked users list"""
    users = load_tracked_users()
    key = ign.casefold()
    found = False
    new_users = []
    for u in users:
        if u.casefold() == key:
            found = True
        else:
            new_users.append(u)
    
    if found:
        with open(TRACKED_FILE, "w", encoding="utf-8") as f:
            for u in new_users:
                f.write(u + "\n")
    return found

def unlink_user_from_ign(ign: str) -> bool:
    """Remove username -> Discord user ID link"""
    links = load_user_links()
    key = ign.casefold()
    if key in links:
        del links[key]
        save_user_links(links)
        return True
    return False

def remove_user_color(ign: str) -> bool:
    """Remove username from user_colors.json"""
    try:
        if not os.path.exists(USER_COLORS_FILE):
            return False
        with open(USER_COLORS_FILE, 'r') as f:
            color_data = json.load(f)
        
        key = ign.casefold()
        if key in color_data:
            del color_data[key]
            with open(USER_COLORS_FILE, 'w') as f:
                json.dump(color_data, f, indent=2)
            return True
        return False
    except Exception as e:
        print(f"[ERROR] Failed to remove user color for {ign}: {e}")
        return False

def delete_user_sheet(ign: str) -> bool:
    """Delete username's sheet from stats.xlsx"""
    wb = None
    try:
        excel_file = BOT_DIR / "stats.xlsx"
        if not excel_file.exists():
            return False
        
        wb = load_workbook(str(excel_file))
        key = ign.casefold()
        sheet_to_delete = None
        
        for sheet_name in wb.sheetnames:
            if sheet_name.casefold() == key:
                sheet_to_delete = sheet_name
                break
        
        if sheet_to_delete:
            del wb[sheet_to_delete]
            save_success = safe_save_workbook(wb, str(excel_file))
            if not save_success:
                print(f"[ERROR] Failed to save after deleting sheet for {ign}")
                return False
            return True
        
        # Sheet not found, just close workbook
        try:
            wb.close()
        except Exception:
            pass
        return False
    except Exception as e:
        print(f"[ERROR] Failed to delete sheet for {ign}: {e}")
        if wb is not None:
            try:
                wb.close()
            except Exception:
                pass
        return False

async def cleanup_untracked_user_delayed(ign: str, delay_seconds: int = 60):
    """Schedule cleanup of untracked user data after a delay.
    
    Waits for delay_seconds, then checks if the user is still untracked.
    If they're still untracked, removes their color data and sheet.
    """
    try:
        print(f"[CLEANUP] Scheduled cleanup for '{ign}' in {delay_seconds} seconds")
        await asyncio.sleep(delay_seconds)
        
        # Check if user is now tracked
        tracked_users = load_tracked_users()
        print(f"[CLEANUP] After {delay_seconds}s delay, checking if '{ign}' is tracked")
        print(f"[CLEANUP] Tracked users list: {tracked_users}")
        
        key = ign.casefold()
        for tracked_user in tracked_users:
            if tracked_user.casefold() == key:
                # User is now tracked, don't clean up
                print(f"[CLEANUP] SKIPPING cleanup for '{ign}' - found in tracked_users.txt as '{tracked_user}'")
                return
        
        # User is still untracked, proceed with cleanup
        print(f"[CLEANUP] User '{ign}' NOT FOUND in tracked_users.txt")
        print(f"[CLEANUP] Reason: User was queried via /sheepwars but is not in tracked list")
        print(f"[CLEANUP] Proceeding with cleanup: removing color data and deleting sheet")
        
        color_removed = remove_user_color(ign)
        sheet_deleted = delete_user_sheet(ign)
        
        print(f"[CLEANUP] Cleanup complete for '{ign}' - color_removed={color_removed}, sheet_deleted={sheet_deleted}")
    except asyncio.CancelledError:
        print(f"[CLEANUP] Cleanup task cancelled for '{ign}'")
    except Exception as e:
        print(f"[CLEANUP] ERROR during cleanup for '{ign}': {e}")
        import traceback
        traceback.print_exc()

async def send_fetch_message(message: str):
    # DM the creator (prefer explicit ID if set)
    user = None
    if CREATOR_ID is not None:
        try:
            uid = int(CREATOR_ID)
            user = bot.get_user(uid) or await bot.fetch_user(uid)
        except Exception:
            user = None
    if user is None:
        # fallback to name/display name search across guilds
        for guild in bot.guilds:
            for member in guild.members:
                if member.bot:
                    continue
                name_match = member.name.casefold() == CREATOR_NAME.casefold()
                display_match = member.display_name.casefold() == CREATOR_NAME.casefold()
                if name_match or display_match:
                    user = member
                    break
            if user:
                break
    if user:
        try:
            await user.send(message)
            return
        except Exception as e:
            # Common cause: user has DMs disabled (Discord error 50007). Fall back to channel.
            print(f"[WARNING] Could not DM creator: {e}")
    # fallback: send to system channel or first writable channel
    for guild in bot.guilds:
        channel = None
        if guild.system_channel and guild.system_channel.permissions_for(guild.me).send_messages:
            channel = guild.system_channel
        else:
            for ch in guild.text_channels:
                if ch.permissions_for(guild.me).send_messages:
                    channel = ch
                    break
        if channel:
            try:
                await channel.send(message)
                break
            except Exception:
                continue

async def _delayed_refresh_user(username: str, delay: float):
    """Sleep for `delay` seconds then run api_get.py for the given username."""
    try:
        await asyncio.sleep(delay)
        await asyncio.to_thread(run_script, "api_get.py", ["-ign", username])
    except asyncio.CancelledError:
        return
    except Exception as e:
        print(f"[REFRESH] Error refreshing {username}: {e}")


async def staggered_stats_refresher(interval_minutes: int = 10):
    """Background task that refreshes every tracked user's stats once per `interval_minutes`.

    Each user's refresh is scheduled at a random point during the interval to spread load.
    """
    interval = interval_minutes * 60
    buffer = 5  # seconds buffer to avoid scheduling at the very end
    while True:
        try:
            users = load_tracked_users()
            if not users:
                await asyncio.sleep(interval)
                continue

            # assign a random delay in [0, interval-buffer) to each user, then schedule
            tasks = []
            for u in users:
                d = random.uniform(0, max(0, interval - buffer))
                tasks.append(asyncio.create_task(_delayed_refresh_user(u, d)))

            # wait for the interval to elapse; leave any straggling tasks to finish in background
            await asyncio.sleep(interval)

            # optionally gather any finished tasks and suppress exceptions
            for t in tasks:
                if t.done():
                    try:
                        t.result()
                    except Exception:
                        pass

        except Exception as e:
            print(f"[REFRESH] Staggered refresher error: {e}")
            await asyncio.sleep(interval)


async def scheduler_loop():
    """Automatic scheduler for daily and monthly snapshots"""
    last_run = None
    while True:
        now = datetime.datetime.now(tz=CREATOR_TZ)
        # Run snapshot updates at 9:30 AM
        if now.hour == 9 and now.minute == 30:
            today = now.date()
            if last_run != today:
                try:
                    # Step 1: Run yesterday snapshot (before daily overwrites it)
                    def run_yesterday():
                        return run_script_batch("batch_update.py", ["-schedule", "yesterday"])
                    
                    yesterday_result = await asyncio.to_thread(run_yesterday)
                    if yesterday_result.returncode != 0:
                        await send_fetch_message(f"Warning: Yesterday snapshot failed at {now.strftime('%I:%M %p')}")
                    
                    # Step 2: Determine which snapshots to take
                    # Daily: always
                    # Monthly: only on 1st of month
                    if now.day == 1:
                        schedule = "all"  # daily + monthly
                    else:
                        schedule = "daily"
                    
                    # Step 3: Run batch_update.py for daily (and monthly if 1st)
                    def run_batch():
                        return run_script_batch("batch_update.py", ["-schedule", schedule])
                    
                    result = await asyncio.to_thread(run_batch)
                    if result.returncode == 0:
                        msg = f"Daily snapshot completed at {now.strftime('%I:%M %p')}"
                        if now.day == 1:
                            msg += " (including monthly snapshots)"
                        await send_fetch_message(msg)
                    else:
                        error_msg = result.stderr or result.stdout or "Unknown error"
                        await send_fetch_message(f"Daily snapshot failed: {error_msg[:200]}")
                except Exception as e:
                    await send_fetch_message(f"Snapshot update error: {str(e)}")
                
                last_run = today
        
        await asyncio.sleep(20)

# Helper class for stats tab view
class StatsTabView(discord.ui.View):
    def __init__(self, sheet, ign, level_value: int, prestige_icon: str):
        super().__init__()
        self.sheet = sheet
        self.ign = ign
        self.level_value = level_value
        self.prestige_icon = prestige_icon
        self.current_tab = "all-time"
        
        # Load custom color for this username
        self._load_color()
    
    def _load_color(self):
        """Load or reload the color and guild info for this username from user_colors.json"""
        self.ign_color = None
        self.guild_tag = None
        self.guild_color = None
        try:
            if os.path.exists(USER_COLORS_FILE):
                with open(USER_COLORS_FILE, 'r') as f:
                    color_data = json.load(f)
                    user_entry = color_data.get(self.ign.lower())
                    # Handle both old format (string) and new format (dict with color and rank)
                    if isinstance(user_entry, str):
                        self.ign_color = user_entry
                    elif isinstance(user_entry, dict):
                        self.ign_color = user_entry.get('color')
                        self.guild_tag = user_entry.get('guild_tag')
                        self.guild_color = user_entry.get('guild_color')
                    print(f"[DEBUG] Loaded color for {self.ign}: {self.ign_color}, guild: [{self.guild_tag}] ({self.guild_color})")
        except Exception as e:
            print(f"[WARNING] Failed to load color for {self.ign}: {e}")
        
        # Find rows dynamically by searching column A for stat names
        self.stat_rows = self._find_stat_rows()
        
        # Column mappings for each period
        self.column_map = {
            "all-time": "B",      # All-time values
            # Use the DELTA columns so the bot shows session/daily/yesterday/monthly changes
            "session": "C",       # Session Delta
            "daily": "E",         # Daily Delta
            "yesterday": "G",     # Yesterday Delta
            "monthly": "I",       # Monthly Delta
        }
        self.update_buttons()

    def _find_stat_rows(self):
        """Find row numbers for each stat by searching column A"""
        rows = {}
        for i in range(1, 100):  # Search first 100 rows
            stat_name = self.sheet[f'A{i}'].value
            if stat_name:
                stat_key = str(stat_name).lower()
                if stat_key == 'kills':
                    rows['kills'] = i
                elif stat_key == 'deaths':
                    rows['deaths'] = i
                elif stat_key == 'wins':
                    rows['wins'] = i
                elif stat_key == 'losses':
                    rows['losses'] = i
        return rows
    
    def update_buttons(self):
        # Update button styles based on current tab
        for child in self.children:
            if isinstance(child, discord.ui.Button):
                if child.custom_id == self.current_tab:
                    child.style = discord.ButtonStyle.primary
                else:
                    child.style = discord.ButtonStyle.secondary
    
    def get_stats_embed(self, tab_name):
        col = self.column_map[tab_name]

        # Get values from the appropriate column and coerce to numbers
        kills = _to_number(self.sheet[f"{col}{self.stat_rows['kills']}"].value)
        deaths = _to_number(self.sheet[f"{col}{self.stat_rows['deaths']}"].value)
        wins = _to_number(self.sheet[f"{col}{self.stat_rows['wins']}"].value)
        losses = _to_number(self.sheet[f"{col}{self.stat_rows['losses']}"].value)
        
        # Calculate K/D and W/L ratios dynamically
        kd_ratio = round(kills / deaths, 2) if deaths > 0 else kills
        wl_ratio = round(wins / losses, 2) if losses > 0 else wins

        embed = discord.Embed(title="")
        
        return embed, wins, losses, wl_ratio, kills, deaths, kd_ratio
    
    def generate_composite_image(self, tab_name):
        """Generate composite image with title and stats."""
        embed, wins, losses, wl_ratio, kills, deaths, kd_ratio = self.get_stats_embed(tab_name)
        
        if Image is not None:
            try:
                img_io = create_stats_composite_image(
                    self.level_value, self.prestige_icon, self.ign, tab_name,
                    wins, losses, wl_ratio, kills, deaths, kd_ratio, 
                    self.ign_color, self.guild_tag, self.guild_color
                )
                filename = f"{self.ign}_{tab_name}_stats.png"
                # Return None for embed to avoid border
                return None, discord.File(img_io, filename=filename)
            except Exception as e:
                print(f"[WARNING] Composite image generation failed: {e}")
                # Fallback to text fields
                embed.add_field(name="Wins", value=f"```{str(wins)}```", inline=True)
                embed.add_field(name="Losses", value=f"```{str(losses)}```", inline=True)
                embed.add_field(name="W/L Ratio", value=f"```{str(wl_ratio)}```", inline=True)
                embed.add_field(name="Kills", value=f"```{str(kills)}```", inline=True)
                embed.add_field(name="Deaths", value=f"```{str(deaths)}```", inline=True)
                embed.add_field(name="K/D Ratio", value=f"```{str(kd_ratio)}```", inline=True)
                return embed, None
        else:
            # Fallback to text fields if Pillow not available
            embed.add_field(name="Wins", value=f"```{str(wins)}```", inline=True)
            embed.add_field(name="Losses", value=f"```{str(losses)}```", inline=True)
            embed.add_field(name="W/L Ratio", value=f"```{str(wl_ratio)}```", inline=True)
            embed.add_field(name="Kills", value=f"```{str(kills)}```", inline=True)
            embed.add_field(name="Deaths", value=f"```{str(deaths)}```", inline=True)
            embed.add_field(name="K/D Ratio", value=f"```{str(kd_ratio)}```", inline=True)
            return embed, None
    
    @discord.ui.button(label="All-time", custom_id="all-time", style=discord.ButtonStyle.primary)
    async def all_time_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.current_tab = "all-time"
        self.update_buttons()
        embed, file = self.generate_composite_image(self.current_tab)
        if file:
            await interaction.response.edit_message(view=self, attachments=[file])
        else:
            await interaction.response.edit_message(embed=embed, view=self)
    
    @discord.ui.button(label="Session", custom_id="session", style=discord.ButtonStyle.secondary)
    async def session_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.current_tab = "session"
        self.update_buttons()
        embed, file = self.generate_composite_image(self.current_tab)
        if file:
            await interaction.response.edit_message(view=self, attachments=[file])
        else:
            await interaction.response.edit_message(embed=embed, view=self)
    
    @discord.ui.button(label="Daily", custom_id="daily", style=discord.ButtonStyle.secondary)
    async def daily_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.current_tab = "daily"
        self.update_buttons()
        embed, file = self.generate_composite_image(self.current_tab)
        if file:
            await interaction.response.edit_message(view=self, attachments=[file])
        else:
            await interaction.response.edit_message(embed=embed, view=self)
    
    @discord.ui.button(label="Yesterday", custom_id="yesterday", style=discord.ButtonStyle.secondary)
    async def yesterday_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.current_tab = "yesterday"
        self.update_buttons()
        embed, file = self.generate_composite_image(self.current_tab)
        if file:
            await interaction.response.edit_message(view=self, attachments=[file])
        else:
            await interaction.response.edit_message(embed=embed, view=self)
    
    @discord.ui.button(label="Monthly", custom_id="monthly", style=discord.ButtonStyle.secondary)
    async def monthly_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.current_tab = "monthly"
        self.update_buttons()
        embed, file = self.generate_composite_image(self.current_tab)
        if file:
            await interaction.response.edit_message(view=self, attachments=[file])
        else:
            await interaction.response.edit_message(embed=embed, view=self)


# Leaderboard view for switching between periods
class LeaderboardView(discord.ui.View):
    def __init__(self, metric: str, wb):
        super().__init__()
        self.metric = metric
        self.wb = wb
        self.current_period = "lifetime"
        
        # Column mappings for each period
        self.column_map = {
            "lifetime": "B",      # All-time values
            # Use the DELTA columns for period comparisons
            "session": "C",       # Session Delta
            "daily": "E",         # Daily Delta
            "yesterday": "G",     # Yesterday Delta
            "monthly": "I",       # Monthly Delta
        }
        
        self.metric_labels = {
            "kills": "Kills",
            "kills_void": "Void Kills",
            "kills_explosive": "Explosive Kills",
            "kills_melee": "Melee Kills",
            "kills_bow": "Bow Kills",
            "deaths": "Deaths",
            "deaths_void": "Void Deaths",
            "deaths_explosive": "Explosive Deaths",
            "deaths_melee": "Melee Deaths",
            "deaths_bow": "Bow Deaths",
            "kdr": "K/D Ratio",
            "wins": "Wins",
            "losses": "Losses",
            "wlr": "W/L Ratio",
            "experience": "Experience",
            "level": "Level",
            "coins": "Coins",
            "damage_dealt": "Damage Dealt",
            "games_played": "Games Played",
            "sheep_thrown": "Sheep Thrown",
            "magic_wool_hit": "Magic Wool Hit",
            "playtime": "Playtime",
        }
        
        # Load user colors
        self.user_colors = {}
        try:
            if os.path.exists(USER_COLORS_FILE):
                with open(USER_COLORS_FILE, 'r') as f:
                    color_data = json.load(f)
                    for username, user_entry in color_data.items():
                        if isinstance(user_entry, str):
                            self.user_colors[username] = user_entry
                        elif isinstance(user_entry, dict):
                            self.user_colors[username] = user_entry.get('color')
        except Exception as e:
            print(f"[WARNING] Failed to load user colors: {e}")
        
        self.update_buttons()
    
    def update_buttons(self):
        for child in self.children:
            if isinstance(child, discord.ui.Button):
                if child.custom_id == self.current_period:
                    child.style = discord.ButtonStyle.primary
                else:
                    child.style = discord.ButtonStyle.secondary
    
    def generate_leaderboard_image(self, period: str):
        """Generate leaderboard image for the given period."""
        col = self.column_map[period]
        metric_label = self.metric_labels[self.metric]
        
        # Collect all player stats with level info
        leaderboard = []
        for sheet_name in self.wb.sheetnames:
            if sheet_name.casefold() == "sheep wars historical data":
                continue
            try:
                sheet = self.wb[sheet_name]
                
                # Find stat rows dynamically
                stat_rows = {}
                for i in range(1, 100):
                    stat_name = sheet[f'A{i}'].value
                    if stat_name:
                        stat_key = str(stat_name).lower()
                        if stat_key not in stat_rows:  # Store first occurrence
                            stat_rows[stat_key] = i
                
                # Get base values from appropriate column
                metric_value = None
                
                if self.metric == "kdr":
                    kills = _to_number(sheet[f"{col}{stat_rows.get('kills', 1)}"].value)
                    deaths = _to_number(sheet[f"{col}{stat_rows.get('deaths', 1)}"].value)
                    metric_value = round(kills / deaths, 2) if deaths > 0 else kills
                elif self.metric == "wlr":
                    wins = _to_number(sheet[f"{col}{stat_rows.get('wins', 1)}"].value)
                    losses = _to_number(sheet[f"{col}{stat_rows.get('losses', 1)}"].value)
                    metric_value = round(wins / losses, 2) if losses > 0 else wins
                else:
                    # Find the stat row for this metric
                    metric_key = self.metric
                    if metric_key in stat_rows:
                        value = sheet[f"{col}{stat_rows[metric_key]}"].value
                        metric_value = _to_number(value)
                
                if metric_value is not None and isinstance(metric_value, (int, float)):
                    # Get level for prestige display
                    try:
                        level_value = 0
                        level_row = None
                        exp_row = None
                        for i in range(1, 100):
                            name = sheet[f'A{i}'].value
                            if not name:
                                continue
                            key = str(name).lower()
                            if key == 'level' and level_row is None:
                                level_row = i
                            elif key == 'experience' and exp_row is None:
                                exp_row = i
                        if level_row is not None:
                            level_value = int(sheet[f'B{level_row}'].value or 0)
                        elif exp_row is not None:
                            exp = sheet[f'B{exp_row}'].value or 0
                            level_value = int((exp or 0) / 5000)
                    except Exception:
                        level_value = 0
                    
                    icon = get_prestige_icon(level_value)
                    is_playtime = self.metric == "playtime"
                    
                    # Get user color
                    ign_color = self.user_colors.get(sheet_name.lower())
                    
                    leaderboard.append((sheet_name, float(metric_value), metric_value, is_playtime, level_value, icon, ign_color))
                
            except Exception:
                continue
        
        # Sort by value descending
        leaderboard.sort(key=lambda x: x[1], reverse=True)
        
        if not leaderboard:
            # Return None if no data
            return None, None
        
        # Prepare data for image generation (top 10 only)
        image_data = []
        for i, entry in enumerate(leaderboard[:10], 1):
            player = entry[0]
            value = entry[2]
            is_playtime = entry[3]
            level_value = entry[4]
            icon = entry[5]
            ign_color = entry[6]
            
            image_data.append((i, player, level_value, icon, ign_color, value, is_playtime))
        
        if Image is not None:
            try:
                img_io = create_leaderboard_image(period.title(), metric_label, image_data)
                filename = f"leaderboard_{self.metric}_{period}.png"
                return None, discord.File(img_io, filename=filename)
            except Exception as e:
                print(f"[WARNING] Leaderboard image generation failed: {e}")
                # Fall back to embed
                return self.get_leaderboard_embed(period), None
        else:
            # Fall back to embed if Pillow not available
            return self.get_leaderboard_embed(period), None
    
    def get_leaderboard_embed(self, period: str):
        col = self.column_map[period]
        metric_label = self.metric_labels[self.metric]
        
        # Collect all player stats with level info
        leaderboard = []
        for sheet_name in self.wb.sheetnames:
            if sheet_name.casefold() == "sheep wars historical data":
                continue
            try:
                sheet = self.wb[sheet_name]
                
                # Find stat rows dynamically
                stat_rows = {}
                for i in range(1, 100):
                    stat_name = sheet[f'A{i}'].value
                    if stat_name:
                        stat_key = str(stat_name).lower()
                        if stat_key not in stat_rows:  # Store first occurrence
                            stat_rows[stat_key] = i
                
                # Get base values from appropriate column
                metric_value = None
                
                if self.metric == "kdr":
                    kills = _to_number(sheet[f"{col}{stat_rows.get('kills', 1)}"].value)
                    deaths = _to_number(sheet[f"{col}{stat_rows.get('deaths', 1)}"].value)
                    metric_value = round(kills / deaths, 2) if deaths > 0 else kills
                elif self.metric == "wlr":
                    wins = _to_number(sheet[f"{col}{stat_rows.get('wins', 1)}"].value)
                    losses = _to_number(sheet[f"{col}{stat_rows.get('losses', 1)}"].value)
                    metric_value = round(wins / losses, 2) if losses > 0 else wins
                else:
                    # Find the stat row for this metric
                    metric_key = self.metric
                    if metric_key in stat_rows:
                        value = sheet[f"{col}{stat_rows[metric_key]}"].value
                        metric_value = _to_number(value)
                
                if metric_value is not None and isinstance(metric_value, (int, float)):
                    # Get level for prestige display
                    try:
                        level_value = 0
                        level_row = None
                        exp_row = None
                        for i in range(1, 100):
                            name = sheet[f'A{i}'].value
                            if not name:
                                continue
                            key = str(name).lower()
                            if key == 'level' and level_row is None:
                                level_row = i
                            elif key == 'experience' and exp_row is None:
                                exp_row = i
                        if level_row is not None:
                            level_value = int(sheet[f'B{level_row}'].value or 0)
                        elif exp_row is not None:
                            exp = sheet[f'B{exp_row}'].value or 0
                            level_value = int((exp or 0) / 5000)
                    except Exception:
                        level_value = 0
                    
                    icon = get_prestige_icon(level_value)
                    is_playtime = self.metric == "playtime"
                    leaderboard.append((sheet_name, float(metric_value), metric_value, is_playtime, level_value, icon))
                
            except Exception:
                continue
        
        # Sort by value descending
        leaderboard.sort(key=lambda x: x[1], reverse=True)
        
        # Build embed
        embed = discord.Embed(
            title=f"{period.title()} {metric_label} Leaderboard",
            color=discord.Color.from_rgb(54, 57, 63)
        )
        
        if not leaderboard:
            embed.description = "No data available"
        else:
            # Top 10 with colored prestige prefix
            description_lines = []
            ansi_code = get_ansi_color_code
            reset_code = "\u001b[0m"
            
            for i, entry in enumerate(leaderboard[:10], 1):
                player = entry[0]
                value = entry[2]
                is_playtime = entry[3]
                level_value = entry[4]
                icon = entry[5]
                
                medal = {1: "1.", 2: "2.", 3: "3."}.get(i, f"{i}.")
                # Use multi-color formatter for leaderboard prefix
                prestige_display = format_prestige_ansi(level_value, icon)
                
                # Format value based on type
                if is_playtime:
                    formatted_value = format_playtime(int(value))
                else:
                    formatted_value = f"{value}"
                
                description_lines.append(f"{medal} {prestige_display} {player}: {formatted_value}")
            
            embed.description = f"```ansi\n" + "\n".join(description_lines) + "\n```"
        
        return embed
    
    @discord.ui.button(label="Lifetime", custom_id="lifetime", style=discord.ButtonStyle.primary)
    async def lifetime_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.current_period = "lifetime"
        self.update_buttons()
        embed, file = self.generate_leaderboard_image(self.current_period)
        if file:
            await interaction.response.edit_message(view=self, attachments=[file])
        else:
            await interaction.response.edit_message(embed=embed, view=self)
    
    @discord.ui.button(label="Session", custom_id="session", style=discord.ButtonStyle.secondary)
    async def session_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.current_period = "session"
        self.update_buttons()
        embed, file = self.generate_leaderboard_image(self.current_period)
        if file:
            await interaction.response.edit_message(view=self, attachments=[file])
        else:
            await interaction.response.edit_message(embed=embed, view=self)
    
    @discord.ui.button(label="Daily", custom_id="daily", style=discord.ButtonStyle.secondary)
    async def daily_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.current_period = "daily"
        self.update_buttons()
        embed, file = self.generate_leaderboard_image(self.current_period)
        if file:
            await interaction.response.edit_message(view=self, attachments=[file])
        else:
            await interaction.response.edit_message(embed=embed, view=self)
    
    @discord.ui.button(label="Yesterday", custom_id="yesterday", style=discord.ButtonStyle.secondary)
    async def yesterday_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.current_period = "yesterday"
        self.update_buttons()
        embed, file = self.generate_leaderboard_image(self.current_period)
        if file:
            await interaction.response.edit_message(view=self, attachments=[file])
        else:
            await interaction.response.edit_message(embed=embed, view=self)
    
    @discord.ui.button(label="Monthly", custom_id="monthly", style=discord.ButtonStyle.secondary)
    async def monthly_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.current_period = "monthly"
        self.update_buttons()
        embed, file = self.generate_leaderboard_image(self.current_period)
        if file:
            await interaction.response.edit_message(view=self, attachments=[file])
        else:
            await interaction.response.edit_message(embed=embed, view=self)


# Create bot with command tree for slash commands
intents = discord.Intents.default()
bot = commands.Bot(command_prefix="!", intents=intents)

# Approval system for claim command
class ApprovalView(discord.ui.View):
    def __init__(self, ign: str, requester: str, requester_id: int, original_interaction: discord.Interaction):
        super().__init__(timeout=None)
        self.ign = ign
        self.requester = requester
        self.requester_id = requester_id
        self.original_interaction = original_interaction
        self.approved = None
        self.done_event = asyncio.Event()
    
    @discord.ui.button(label="Accept", style=discord.ButtonStyle.success)
    async def accept_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.approved = True
        self.done_event.set()
        await interaction.response.edit_message(content=f"You accepted claim for {self.ign}.", view=None)
    
    @discord.ui.button(label="Deny", style=discord.ButtonStyle.danger)
    async def deny_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.approved = False
        self.done_event.set()
        await interaction.response.edit_message(content=f"You denied claim for {self.ign}.", view=None)

# Bot token
# Read from BOT_TOKEN.txt in the same directory
TOKEN_FILE = os.path.join(os.path.dirname(__file__), "BOT_TOKEN.txt")
try:
    with open(TOKEN_FILE, "r", encoding="utf-8") as f:
        DISCORD_TOKEN = f.read().strip()
except Exception as e:
    DISCORD_TOKEN = None
    print(f"[ERROR] Failed to read BOT_TOKEN.txt: {e}")
if not DISCORD_TOKEN:
    raise ValueError("BOT_TOKEN.txt is missing or empty")

@bot.event
async def on_ready():
    import time
    bot_instance_id = int(time.time() * 1000) % 100000
    print(f"[OK] Bot logged in as {bot.user} - Instance ID: {bot_instance_id}")
    try:
        synced = await bot.tree.sync()
        print(f"[OK] Synced {len(synced)} command(s) - Instance ID: {bot_instance_id}")
    except Exception as e:
        print(f"[ERROR] Failed to sync commands: {e}")
    # start background tasks once
    if not getattr(bot, "background_tasks_started", False):
        bot.loop.create_task(scheduler_loop())
        bot.loop.create_task(staggered_stats_refresher(interval_minutes=10))
        bot.background_tasks_started = True
        print(f"[OK] Background tasks started - Instance ID: {bot_instance_id}")

@bot.tree.command(name="track", description="Create a stats sheet for a player (no authorization required)")
@discord.app_commands.describe(ign="Minecraft IGN")
async def track(interaction: discord.Interaction, ign: str):
    if not interaction.response.is_done():
        try:
            await interaction.response.defer()
        except (discord.errors.NotFound, discord.errors.HTTPException):
            return
    
    try:
        # Check if user is already in tracked_users.txt
        tracked_users = load_tracked_users()
        key = ign.casefold()
        for tracked_user in tracked_users:
            if tracked_user.casefold() == key:
                await interaction.followup.send(f"{tracked_user} is already being tracked.")
                return
        
        # Create sheet using api_get.py
        # Initialize session, daily, and monthly snapshots (yesterday will be populated from daily rotation)
        result = run_script("api_get.py", ["-ign", ign, "-session", "-daily", "-monthly"])

        if result.returncode == 0:
            print(f"[OK] api_get.py succeeded for {ign}")
            
            # Verify the sheet was actually created and get the properly-cased username
            excel_file = BOT_DIR / "stats.xlsx"
            if not excel_file.exists():
                await interaction.followup.send(f"[ERROR] Excel file was not created for {ign}.")
                return
            
            # Load and check if the sheet exists, and get the actual sheet name (proper case)
            actual_ign = ign  # Default to input if not found
            wb = None
            try:
                wb = load_workbook(str(excel_file))
                sheet_exists = False
                key = ign.casefold()
                for sheet_name in wb.sheetnames:
                    if sheet_name.casefold() == key:
                        sheet_exists = True
                        actual_ign = sheet_name  # Get the properly cased username from the sheet name
                        break
                
                if not sheet_exists:
                    await interaction.followup.send(f"[ERROR] Sheet for {ign} was not created.")
                    return
            except Exception as e:
                await interaction.followup.send(f"[ERROR] Could not verify sheet creation: {str(e)}")
                return
            finally:
                if wb is not None:
                    try:
                        wb.close()
                    except Exception:
                        pass
            
            # Add to tracked users list using the properly-cased username
            added = add_tracked_user(actual_ign)
            
            if added:
                await interaction.followup.send(f"{actual_ign} is now being tracked. Use `/claim ign:{actual_ign}` to link this username to your Discord account.")
            else:
                await interaction.followup.send(f"{actual_ign} is already being tracked.")
        else:
            err = (result.stderr or result.stdout) or "Unknown error"
            print(f"[ERROR] api_get.py failed for {ign}:")
            print(f"  stdout: {result.stdout}")
            print(f"  stderr: {result.stderr}")
            await interaction.followup.send(f"Error creating sheet for {ign}:\n```{sanitize_output(err[:500])}```")
            
    except subprocess.TimeoutExpired:
        await interaction.followup.send("[ERROR] Command timed out (30s limit)")
    except Exception as e:
        await interaction.followup.send(f"[ERROR] {str(e)}")

@bot.tree.command(name="claim", description="Link a Minecraft username to your Discord account")
@discord.app_commands.describe(ign="Minecraft IGN")
async def claim(interaction: discord.Interaction, ign: str):
    if not interaction.response.is_done():
        try:
            await interaction.response.defer()
        except (discord.errors.NotFound, discord.errors.HTTPException):
            return
    
    try:
        # Check if username is tracked
        users = load_tracked_users()
        found = False
        for u in users:
            if u.casefold() == ign.casefold():
                found = True
                break
        
        if not found:
            await interaction.followup.send(f"[ERROR] {ign} is not being tracked. Use `/track ign:{ign}` first.")
            return
        
        # Check if already claimed
        links = load_user_links()
        if ign.casefold() in links:
            claimed_by = links[ign.casefold()]
            if claimed_by == str(interaction.user.id):
                await interaction.followup.send(f"[ERROR] You have already claimed {ign}.")
            else:
                await interaction.followup.send(f"[ERROR] {ign} is already claimed by another user.")
            return
        
        # Get creator user
        creator = None
        if CREATOR_ID is not None:
            try:
                uid = int(CREATOR_ID)
                creator = bot.get_user(uid) or await bot.fetch_user(uid)
            except Exception:
                pass
        
        if creator is None:
            await interaction.followup.send("[ERROR] Cannot reach creator for approval. Contact administrator.")
            return
        
        # Send waiting message to requester
        requester_name = interaction.user.name
        await interaction.followup.send(f"Asked Chuckegg for approval to claim {ign}. Please wait for confirmation.")
        
        # Create approval view and send to creator
        view = ApprovalView(ign, requester_name, interaction.user.id, interaction)
        try:
            await creator.send(f"{requester_name} wants to claim {ign}.", view=view)
        except Exception as e:
            await interaction.followup.send(f"[ERROR] Could not send approval request to creator: {str(e)}")
            return
        
        # Wait for approval (no timeout)
        await view.done_event.wait()
        
        # Process based on approval
        if view.approved:
            link_user_to_ign(interaction.user.id, ign)
            await interaction.followup.send(f"Chuckegg has approved your claim. {ign} is now linked to your Discord account.")
        else:
            await interaction.followup.send(f"Chuckegg has denied your claim for {ign}.")
            
    except Exception as e:
        await interaction.followup.send(f"[ERROR] {str(e)}")

@bot.tree.command(name="unclaim", description="Unlink a Minecraft username from your Discord account")
@discord.app_commands.describe(ign="Minecraft IGN to unclaim")
async def unclaim(interaction: discord.Interaction, ign: str):
    if not interaction.response.is_done():
        try:
            await interaction.response.defer()
        except (discord.errors.NotFound, discord.errors.HTTPException):
            return
    
    # Check if user is authorized to unclaim this username
    if not is_user_authorized(interaction.user.id, ign):
        await interaction.followup.send(f"[ERROR] You are not authorized to unclaim {ign}. Only the user who claimed this username can unclaim it.")
        return
    
    try:
        # Remove from user links
        removed_link = unlink_user_from_ign(ign)
        
        if removed_link:
            await interaction.followup.send(f"Successfully unclaimed {ign}. You are no longer linked to this username.")
        else:
            await interaction.followup.send(f"[WARNING] No claim found for {ign}.")
            
    except Exception as e:
        await interaction.followup.send(f"[ERROR] Failed to unclaim: {str(e)}")

@bot.tree.command(name="untrack", description="Remove all tracking data for a Minecraft username")
@discord.app_commands.describe(ign="Minecraft IGN to untrack")
async def untrack(interaction: discord.Interaction, ign: str):
    if not interaction.response.is_done():
        try:
            await interaction.response.defer()
        except (discord.errors.NotFound, discord.errors.HTTPException):
            return
    
    # Check if user is authorized to untrack this username (must have it claimed)
    if not is_user_authorized(interaction.user.id, ign):
        await interaction.followup.send(f"[ERROR] You are not authorized to untrack {ign}. Only the user who claimed this username can untrack it.")
        return
    
    try:
        # Find the properly-cased username from Excel file
        excel_file = BOT_DIR / "stats.xlsx"
        actual_ign = ign
        if excel_file.exists():
            wb = None
            try:
                wb = load_workbook(str(excel_file))
                key = ign.casefold()
                for sheet_name in wb.sheetnames:
                    if sheet_name.casefold() == key:
                        actual_ign = sheet_name
                        break
            except Exception:
                pass
            finally:
                if wb is not None:
                    try:
                        wb.close()
                    except Exception:
                        pass
        
        # Remove from all locations
        removed_tracked = remove_tracked_user(actual_ign)
        removed_link = unlink_user_from_ign(actual_ign)
        removed_color = remove_user_color(actual_ign)
        removed_sheet = delete_user_sheet(actual_ign)
        
        if removed_tracked or removed_link or removed_color or removed_sheet:
            results = []
            if removed_tracked:
                results.append("tracked users list")
            if removed_link:
                results.append("user links")
            if removed_color:
                results.append("user colors")
            if removed_sheet:
                results.append("stats sheet")
            
            await interaction.followup.send(f"Successfully untracked {actual_ign}. Removed from: {', '.join(results)}.")
        else:
            await interaction.followup.send(f"[WARNING] {ign} was not found in any tracking data.")
            
    except Exception as e:
        await interaction.followup.send(f"[ERROR] Failed to untrack: {str(e)}")

# Create color choices from MINECRAFT_CODE_TO_HEX
COLOR_CHOICES = [
    discord.app_commands.Choice(name="Black", value="0"),
    discord.app_commands.Choice(name="Dark Blue", value="1"),
    discord.app_commands.Choice(name="Dark Green", value="2"),
    discord.app_commands.Choice(name="Dark Aqua", value="3"),
    discord.app_commands.Choice(name="Dark Red", value="4"),
    discord.app_commands.Choice(name="Dark Purple", value="5"),
    discord.app_commands.Choice(name="Gold", value="6"),
    discord.app_commands.Choice(name="Gray", value="7"),
    discord.app_commands.Choice(name="Dark Gray", value="8"),
    discord.app_commands.Choice(name="Blue", value="9"),
    discord.app_commands.Choice(name="Green", value="a"),
    discord.app_commands.Choice(name="Aqua", value="b"),
    discord.app_commands.Choice(name="Red", value="c"),
    discord.app_commands.Choice(name="Light Purple/Pink", value="d"),
    discord.app_commands.Choice(name="Yellow", value="e"),
    discord.app_commands.Choice(name="White", value="f"),
]

@bot.tree.command(name="color", description="Set a custom color for your username in stats displays")
@discord.app_commands.describe(
    ign="Minecraft IGN",
    color="Color for your username"
)
@discord.app_commands.choices(color=COLOR_CHOICES)
async def color(interaction: discord.Interaction, ign: str, color: discord.app_commands.Choice[str]):
    if not interaction.response.is_done():
        try:
            await interaction.response.defer()
        except (discord.errors.NotFound, discord.errors.HTTPException):
            return
    
    # Check if user is authorized to change color for this username
    if not is_user_authorized(interaction.user.id, ign):
        await interaction.followup.send(f"[ERROR] You are not authorized to change the color for {ign}. Only the user who claimed this username can change its color.")
        return
    
    try:
        # Load or create color preferences
        color_data = {}
        if os.path.exists(USER_COLORS_FILE):
            with open(USER_COLORS_FILE, 'r') as f:
                color_data = json.load(f)
        
        # Get hex color from code
        color_code = color.value
        hex_color = MINECRAFT_CODE_TO_HEX.get(color_code, '#FFFFFF')
        
        # Store the color preference with new structure
        username_key = ign.lower()
        if username_key in color_data:
            # Update existing entry, preserve rank if it exists
            if isinstance(color_data[username_key], dict):
                color_data[username_key]['color'] = hex_color
            else:
                # Old format, convert to new format
                color_data[username_key] = {'color': hex_color, 'rank': None}
        else:
            # New entry
            color_data[username_key] = {'color': hex_color, 'rank': None}
        
        # Save to file
        with open(USER_COLORS_FILE, 'w') as f:
            json.dump(color_data, f, indent=2)
        
        await interaction.followup.send(f"Successfully set {ign}'s username color to {color.name}!")
        
    except Exception as e:
        await interaction.followup.send(f"[ERROR] Failed to set color: {str(e)}")

@bot.tree.command(name="reset", description="Reset session snapshot for a player")
@discord.app_commands.describe(ign="Minecraft IGN")
async def reset(interaction: discord.Interaction, ign: str):
    if not interaction.response.is_done():
        try:
            await interaction.response.defer()
        except (discord.errors.NotFound, discord.errors.HTTPException):
            return
    
    # Check if user is authorized to reset session for this username
    if not is_user_authorized(interaction.user.id, ign):
        await interaction.followup.send(f"[ERROR] You are not authorized to reset session for {ign}. Only the user who claimed this username can reset its session.")
        return
    
    try:
        result = run_script("api_get.py", ["-ign", ign, "-session"])

        if result.returncode == 0:
            await interaction.followup.send(f"Session snapshot reset for {ign}.")
        else:
            err = (result.stderr or result.stdout) or "Unknown error"
            await interaction.followup.send(f"[ERROR] {sanitize_output(err)}")
    except subprocess.TimeoutExpired:
        await interaction.followup.send("[ERROR] Command timed out (30s limit)")
    except Exception as e:
        await interaction.followup.send(f"[ERROR] {str(e)}")

@bot.tree.command(name="dmme", description="Send yourself a test DM from the bot")
async def dmme(interaction: discord.Interaction):
    if not interaction.response.is_done():
        try:
            await interaction.response.defer(ephemeral=True)
        except (discord.errors.NotFound, discord.errors.HTTPException):
            return
    try:
        await interaction.user.send("Hello! This is a private message from the bot.")
        await interaction.followup.send("Sent you a DM.", ephemeral=True)
    except Exception as e:
        await interaction.followup.send("Couldn't DM you. Check your privacy settings (Allow DMs from server members).", ephemeral=True)


@bot.tree.command(name="prestige", description="Display a prestige prefix for any level")
@discord.app_commands.describe(
    level="The prestige level (e.g., 1964)",
    ign="Optional: Username to display after the prefix"
)
async def prestige(interaction: discord.Interaction, level: int, ign: str = None):
    if not interaction.response.is_done():
        try:
            await interaction.response.defer()
        except (discord.errors.NotFound, discord.errors.HTTPException):
            return
    
    try:
        # Validate level range
        if level < 0 or level > 10000:
            await interaction.followup.send("[ERROR] Level must be between 0 and 10000")
            return
        
        # Get prestige icon for this level
        icon = get_prestige_icon(level)
        
        # Build the colored prefix
        colored_prefix = format_prestige_ansi(level, icon)
        
        # Add IGN if provided
        if ign:
            display_text = f"{colored_prefix} {ign}"
        else:
            display_text = colored_prefix
        
        # Try to render as image if Pillow is available
        if Image is not None:
            try:
                base = (level // 100) * 100
                raw = PRESTIGE_RAW_PATTERNS.get(base)
                
                if raw:
                    # Parse the pattern and replace the level number
                    parts = _parse_raw_pattern(raw)
                    
                    # Build segments with the actual level
                    concat = ''.join(t for (_, t) in parts)
                    m = re.search(r"\d+", concat)
                    
                    segments = []
                    if m:
                        num_start, num_end = m.start(), m.end()
                        pos = 0
                        replaced = False
                        
                        for code, text in parts:
                            part_start = pos
                            part_end = pos + len(text)
                            pos = part_end
                            hexcol = MINECRAFT_CODE_TO_HEX.get(code.lower(), '#FFFFFF')
                            
                            if part_end <= num_start or part_start >= num_end:
                                segments.append((hexcol, text))
                                continue
                            
                            # Prefix before number
                            prefix_len = max(0, num_start - part_start)
                            if prefix_len > 0:
                                segments.append((hexcol, text[:prefix_len]))
                            
                            # Replace with actual level
                            if not replaced:
                                # Check if this is a rainbow prestige
                                rainbow_bases = {k for k, v in PRESTIGE_COLORS.items() if v is None}
                                if base in rainbow_bases:
                                    # Build rainbow colors
                                    colors_in_span = []
                                    pos2 = 0
                                    for c_code, c_text in parts:
                                        part_s = pos2
                                        part_e = pos2 + len(c_text)
                                        pos2 = part_e
                                        overlap_s = max(part_s, num_start)
                                        overlap_e = min(part_e, num_end)
                                        if overlap_e > overlap_s:
                                            hexcol_span = MINECRAFT_CODE_TO_HEX.get(c_code.lower(), '#FFFFFF')
                                            for _ in range(overlap_e - overlap_s):
                                                colors_in_span.append(hexcol_span)
                                    
                                    if not colors_in_span:
                                        RAINBOW_CODES = ['c', '6', 'e', 'a', 'b', 'd', '9', '3']
                                        colors_in_span = [MINECRAFT_CODE_TO_HEX.get(c, '#FFFFFF') for c in RAINBOW_CODES]
                                    
                                    # Apply colors to level digits
                                    for i, ch in enumerate(str(level)):
                                        col = colors_in_span[i % len(colors_in_span)]
                                        segments.append((col, ch))
                                else:
                                    segments.append((hexcol, str(level)))
                                replaced = True
                            
                            # Suffix after number
                            suffix_start_in_part = max(0, num_end - part_start)
                            if suffix_start_in_part < len(text):
                                segments.append((hexcol, text[suffix_start_in_part:]))
                    else:
                        # No number found, just use the pattern as-is with level prepended
                        segments = [(MINECRAFT_CODE_TO_HEX.get(parts[0][0], '#FFFFFF'), f"[{level}")]
                        segments.extend([(MINECRAFT_CODE_TO_HEX.get(code, '#FFFFFF'), text) for code, text in parts[1:]])
                    
                    # Add IGN if provided
                    if ign:
                        segments.append((MINECRAFT_CODE_TO_HEX.get('f', '#FFFFFF'), f" {ign}"))
                    
                    # Render to image
                    img_io = _render_text_segments_to_image(segments)
                    filename = f"prestige_{level}" + (f"_{ign}" if ign else "") + ".png"
                    await interaction.followup.send(file=discord.File(img_io, filename=filename))
                    return
            except Exception as e:
                # Fall back to ANSI if image rendering fails
                print(f"[WARNING] Image rendering failed: {e}")
        
        # Fallback: send as ANSI text
        await interaction.followup.send(f"```ansi\n{display_text}\n```")
        
    except Exception as e:
        await interaction.followup.send(f"[ERROR] {str(e)}")


@bot.tree.command(name="instructions", description="Display bot usage instructions")
async def instructions(interaction: discord.Interaction):
    if not interaction.response.is_done():
        try:
            await interaction.response.defer()
        except (discord.errors.NotFound, discord.errors.HTTPException):
            return
    
    try:
        instructions_file = BOT_DIR / "instructions.txt"
        if not instructions_file.exists():
            await interaction.followup.send("[ERROR] Instructions file not found")
            return
        
        with open(instructions_file, "r", encoding="utf-8") as f:
            content = f.read()
        
        # Discord has a 2000 character limit for messages
        if len(content) > 1900:
            # Split into chunks if needed
            chunks = [content[i:i+1900] for i in range(0, len(content), 1900)]
            for chunk in chunks:
                await interaction.followup.send(chunk)
        else:
            await interaction.followup.send(content)
    except Exception as e:
        await interaction.followup.send(f"[ERROR] {str(e)}")


@bot.tree.command(name="refresh", description="Manually run batch snapshot update for all tracked users")
@discord.app_commands.describe(mode="One of: session, daily, yesterday, monthly, all, or all+session")
@discord.app_commands.choices(mode=[
    discord.app_commands.Choice(name="session", value="session"),
    discord.app_commands.Choice(name="daily", value="daily"),
    discord.app_commands.Choice(name="yesterday", value="yesterday"),
    discord.app_commands.Choice(name="monthly", value="monthly"),
    discord.app_commands.Choice(name="all (daily + yesterday + monthly)", value="all"),
    discord.app_commands.Choice(name="all+session (session + daily + yesterday + monthly)", value="all-session"),
])
async def refresh(interaction: discord.Interaction, mode: discord.app_commands.Choice[str]):
    if not interaction.response.is_done():
        try:
            await interaction.response.defer(ephemeral=True)
        except (discord.errors.NotFound, discord.errors.HTTPException):
            return
    try:
        # Run batch_update.py with selected schedule (use extended timeout)
        def run_batch():
            return run_script_batch("batch_update.py", ["-schedule", mode.value])
        
        result = await asyncio.to_thread(run_batch)
        
        if result.returncode == 0:
            msg = f"Batch snapshot update completed for schedule: {mode.name}"
        else:
            error_msg = result.stderr or result.stdout or "Unknown error"
            msg = f"Batch update failed: {error_msg[:300]}"
        
        # Try to DM the invoking user directly
        try:
            await interaction.user.send(msg)
            await interaction.followup.send("Sent you a DM with the results.", ephemeral=True)
        except Exception:
            # Fallback to ephemeral if DMs are closed
            await interaction.followup.send(msg, ephemeral=True)
    except subprocess.TimeoutExpired:
        await interaction.followup.send(f"[ERROR] Batch update timed out after 5 minutes. Try a smaller schedule (e.g., just 'daily' or 'session').", ephemeral=True)
    except Exception as e:
        await interaction.followup.send(f"[ERROR] {str(e)}", ephemeral=True)

@bot.tree.command(name="sheepwars", description="Get player stats with deltas")
@discord.app_commands.describe(ign="Minecraft IGN")
async def sheepwars(interaction: discord.Interaction, ign: str):
    print(f"[DEBUG] /sheepwars triggered for IGN: {ign} by user: {interaction.user.name} in guild: {interaction.guild.name if interaction.guild else 'DM'}")
    
    # Defer FIRST, before any long operations
    if not interaction.response.is_done():
        try:
            print(f"[DEBUG] Deferring interaction for {ign}")
            await interaction.response.defer()
            print(f"[DEBUG] Defer successful for {ign}")
        except (discord.errors.NotFound, discord.errors.HTTPException) as e:
            # Interaction expired or already acknowledged - nothing we can do
            print(f"[DEBUG] Defer failed for {ign}: {e}")
            return
    
    try:
        # Fetch fresh stats using api_get.py (updates all-time only, no snapshots)
        print(f"[DEBUG] Running api_get.py for IGN: {ign}")
        result = run_script("api_get.py", ["-ign", ign])
        print(f"[DEBUG] api_get.py returncode: {result.returncode}")
        print(f"[DEBUG] api_get.py stdout: {result.stdout if result.stdout else 'None'}")
        print(f"[DEBUG] api_get.py stderr: {result.stderr if result.stderr else 'None'}")
        
        # Check if we got rate limited (429)
        if result.returncode == 0 and result.stdout:
            try:
                output = json.loads(result.stdout)
                if output.get("skipped") and output.get("reason") == "rate_limited":
                    # Rate limited - use existing data from Excel instead of failing
                    print(f"[DEBUG] Rate limited for {ign}, using existing data")
            except (json.JSONDecodeError, ValueError):
                # Not JSON output, continue normally
                pass
        
        if result.returncode != 0:
            # Check if it's a rate limit error
            if result.stderr and "429" in result.stderr:
                # Rate limited - try to use existing data
                print(f"[DEBUG] Rate limited for {ign}, attempting to use existing data")
            else:
                error_msg = result.stderr or result.stdout or "Unknown error"
                await interaction.followup.send(f"[ERROR] Failed to fetch stats:\n```{error_msg[:500]}```")
                return
        
        # Read Excel file and get stats
        EXCEL_FILE = BOT_DIR / "stats.xlsx"
        if not EXCEL_FILE.exists():
            await interaction.followup.send("[ERROR] Excel file not found")
            return
        
        wb = None
        try:
            wb = load_workbook(EXCEL_FILE)
            
            # Find sheet case-insensitively
            key = ign.casefold()
            found_sheet = None
            actual_ign = ign  # Store the actual sheet name (proper case)
            for sheet_name in wb.sheetnames:
                if sheet_name.casefold() == key:
                    found_sheet = wb[sheet_name]
                    actual_ign = sheet_name  # Get the properly cased username
                    break
            
            if found_sheet is None:
                await interaction.followup.send(f"[ERROR] Player sheet '{ign}' not found")
                return
            
            # Pull level and prestige icon for title decoration
            try:
                # Prefer explicit 'level' row if present; fallback to deriving from 'experience'
                level_value = 0
                level_row = None
                exp_row = None
                for i in range(1, 100):
                    name = found_sheet[f'A{i}'].value
                    if not name:
                        continue
                    key = str(name).lower()
                    if key == 'level' and level_row is None:
                        level_row = i
                    elif key == 'experience' and exp_row is None:
                        exp_row = i
                if level_row is not None:
                    level_value = int(found_sheet[f'B{level_row}'].value or 0)
                elif exp_row is not None:
                    exp = found_sheet[f'B{exp_row}'].value or 0
                    level_value = int((exp or 0) / 5000)
            except Exception:
                level_value = 0
            prestige_icon = get_prestige_icon(level_value)

            # Check if user is being tracked
            tracked_users = load_tracked_users()
            is_tracked = any(u.casefold() == actual_ign.casefold() for u in tracked_users)
            print(f"[SHEEPWARS] User '{actual_ign}' is_tracked={is_tracked}")
            print(f"[SHEEPWARS] Tracked users: {tracked_users}")
            
            # Create view with tabs using actual_ign (properly cased)
            view = StatsTabView(found_sheet, actual_ign, level_value, prestige_icon)
            
            # Reload color after API fetch (api_get.py may have just saved rank/color)
            view._load_color()
            
            embed, file = view.generate_composite_image("all-time")
            
            if file:
                if is_tracked:
                    # Show view with buttons for tracked users
                    await interaction.followup.send(view=view, file=file)
                else:
                    # No view for untracked users, show message instead
                    message = f"{actual_ign} is not being tracked. Use `/track ign:{actual_ign}` first to see delta-based stats."
                    await interaction.followup.send(content=message, file=file)
                    # Schedule cleanup for untracked user after 1 minute
                    print(f"[SHEEPWARS] Scheduling cleanup for untracked user '{actual_ign}' (has file)")
                    bot.loop.create_task(cleanup_untracked_user_delayed(actual_ign, delay_seconds=60))
                    return
            else:
                if is_tracked:
                    await interaction.followup.send(embed=embed, view=view)
                else:
                    message = f"{actual_ign} is not being tracked. Use `/track ign:{actual_ign}` first to see delta-based stats."
                    await interaction.followup.send(content=message, embed=embed)
                    # Schedule cleanup for untracked user after 1 minute
                    print(f"[SHEEPWARS] Scheduling cleanup for untracked user '{actual_ign}' (no file)")
                    bot.loop.create_task(cleanup_untracked_user_delayed(actual_ign, delay_seconds=60))
                    return
        finally:
            if wb is not None:
                try:
                    wb.close()
                except Exception:
                    pass
        
    except subprocess.TimeoutExpired:
        await interaction.followup.send("[ERROR] Command timed out (30s limit)")
    except Exception as e:
        await interaction.followup.send(f"[ERROR] {str(e)}")

# Standalone leaderboard commands
@bot.tree.command(name="leaderboard", description="View player leaderboards")
@discord.app_commands.describe(metric="Choose a stat to rank players by")
@discord.app_commands.choices(metric=[
    discord.app_commands.Choice(name="Kills", value="kills"),
    discord.app_commands.Choice(name="Deaths", value="deaths"),
    discord.app_commands.Choice(name="K/D Ratio", value="kdr"),
    discord.app_commands.Choice(name="Wins", value="wins"),
    discord.app_commands.Choice(name="Losses", value="losses"),
    discord.app_commands.Choice(name="W/L Ratio", value="wlr"),
    discord.app_commands.Choice(name="Experience", value="experience"),
    discord.app_commands.Choice(name="Level", value="level"),
    discord.app_commands.Choice(name="Coins", value="coins"),
    discord.app_commands.Choice(name="Damage Dealt", value="damage_dealt"),
    discord.app_commands.Choice(name="Games Played", value="games_played"),
    discord.app_commands.Choice(name="Sheep Thrown", value="sheep_thrown"),
    discord.app_commands.Choice(name="Magic Wool Hit", value="magic_wool_hit"),
    discord.app_commands.Choice(name="Playtime", value="playtime"),
])
async def leaderboard(interaction: discord.Interaction, metric: discord.app_commands.Choice[str]):
    if not interaction.response.is_done():
        try:
            await interaction.response.defer()
        except (discord.errors.NotFound, discord.errors.HTTPException):
            return
    wb = None
    try:
        EXCEL_FILE = "stats.xlsx"
        if not os.path.exists(EXCEL_FILE):
            await interaction.followup.send("[ERROR] Excel file not found")
            return
        wb = load_workbook(EXCEL_FILE)
        view = LeaderboardView(metric.value, wb)
        embed, file = view.generate_leaderboard_image("lifetime")
        if file:
            await interaction.followup.send(view=view, file=file)
        else:
            await interaction.followup.send(embed=embed, view=view)
    except Exception as e:
        await interaction.followup.send(f"[ERROR] {str(e)}")
    finally:
        if wb is not None:
            try:
                wb.close()
            except Exception:
                pass


@bot.tree.command(name="kill-leaderboard", description="View kills leaderboard by type")
@discord.app_commands.describe(metric="Choose which kill type to rank by")
@discord.app_commands.choices(metric=[
    discord.app_commands.Choice(name="Total Kills", value="kills"),
    discord.app_commands.Choice(name="Void Kills", value="kills_void"),
    discord.app_commands.Choice(name="Explosive Kills", value="kills_explosive"),
    discord.app_commands.Choice(name="Melee Kills", value="kills_melee"),
    discord.app_commands.Choice(name="Bow Kills", value="kills_bow"),
])
async def kill_leaderboard(interaction: discord.Interaction, metric: discord.app_commands.Choice[str]):
    if not interaction.response.is_done():
        try:
            await interaction.response.defer()
        except (discord.errors.NotFound, discord.errors.HTTPException):
            return
    wb = None
    try:
        EXCEL_FILE = "stats.xlsx"
        if not os.path.exists(EXCEL_FILE):
            await interaction.followup.send("[ERROR] Excel file not found")
            return
        wb = load_workbook(EXCEL_FILE)
        view = LeaderboardView(metric.value, wb)
        embed = view.get_leaderboard_embed("lifetime")
        await interaction.followup.send(embed=embed, view=view)
    except Exception as e:
        await interaction.followup.send(f"[ERROR] {str(e)}")
    finally:
        if wb is not None:
            try:
                wb.close()
            except Exception:
                pass


@bot.tree.command(name="death-leaderboard", description="View deaths leaderboard by type")
@discord.app_commands.describe(metric="Choose which death type to rank by")
@discord.app_commands.choices(metric=[
    discord.app_commands.Choice(name="Total Deaths", value="deaths"),
    discord.app_commands.Choice(name="Void Deaths", value="deaths_void"),
    discord.app_commands.Choice(name="Explosive Deaths", value="deaths_explosive"),
    discord.app_commands.Choice(name="Melee Deaths", value="deaths_melee"),
    discord.app_commands.Choice(name="Bow Deaths", value="deaths_bow"),
])
async def death_leaderboard(interaction: discord.Interaction, metric: discord.app_commands.Choice[str]):
    if not interaction.response.is_done():
        try:
            await interaction.response.defer()
        except (discord.errors.NotFound, discord.errors.HTTPException):
            return
    wb = None
    try:
        EXCEL_FILE = "stats.xlsx"
        if not os.path.exists(EXCEL_FILE):
            await interaction.followup.send("[ERROR] Excel file not found")
            return
        wb = load_workbook(EXCEL_FILE)
        view = LeaderboardView(metric.value, wb)
        embed = view.get_leaderboard_embed("lifetime")
        await interaction.followup.send(embed=embed, view=view)
    except Exception as e:
        await interaction.followup.send(f"[ERROR] {str(e)}")
    finally:
        if wb is not None:
            try:
                wb.close()
            except Exception:
                pass


@bot.tree.command(name="prestiges", description="List all prestige prefixes with their colors")
async def prestiges(interaction: discord.Interaction):
    # Defer in case composing takes a moment
    if not interaction.response.is_done():
        try:
            await interaction.response.defer()
        except (discord.errors.NotFound, discord.errors.HTTPException):
            return
    try:
        # Use image rendering if Pillow is available
        if Image is not None:
            try:
                combined = render_all_prestiges_combined()
                await interaction.followup.send(file=discord.File(combined, filename="Wool Games prestiges 0-4000.png"))
            except Exception:
                # If combining fails, fall back to sending individual images
                for base in sorted(PRESTIGE_RAW_PATTERNS.keys()):
                    end_display = base + 99
                    try:
                        imgio = render_prestige_range_image(base, end_display)
                        fname = f"prestige_{base}.png"
                        await interaction.followup.send(file=discord.File(imgio, filename=fname))
                    except Exception:
                        start_str = format_prestige_ansi(base, '')
                        end_str = format_prestige_ansi(end_display, '')
                        await interaction.followup.send(f"{start_str} - {end_str}")
        else:
            # Pillow not installed; fallback to ANSI list
            lines = []
            for base in sorted(PRESTIGE_RAW_PATTERNS.keys()):
                start_str = format_prestige_ansi(base, '')
                end_str = format_prestige_ansi(base + 99, '')
                lines.append(f"{start_str} - {end_str}")
            await _send_paged_ansi_followups(interaction, lines, block='ansi')
    except Exception as e:
        await interaction.followup.send(f"[ERROR] {str(e)}")

# Run bot
if __name__ == "__main__":
    bot.run(DISCORD_TOKEN)
