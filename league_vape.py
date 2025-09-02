# <full file content — updated with cancel + auto-posting at 17:00 CEST>
"""Discord League Bot

This file is the same bot you provided, but with English comments and short explanations
for each section. Keep the token secret and follow the "Next steps" in the chat to get
the bot running.

Mini-summary:
- Manages signups, forced matches, reporting and a simple leaderboard persisted to
  a local JSON file (`league_data.json`).
- Uses a small IO_LOCK to reduce the chance of concurrent writes corrupting the JSON.

Notes:
- Do not commit your bot token to source control.
- Make regular backups of `league_data.json`.
"""

import discord
from discord.ext import commands, tasks
import random
import os
import json
import asyncio
from datetime import datetime, time, timedelta, date
from zoneinfo import ZoneInfo
import csv
import uuid
import threading

# ---------------- CONFIG ----------------
# Replace ORGANISER_IDS with the Discord user IDs of your tournament organisers.
# Use strings in the JSON but keep them as integers here for convenience.
ORGANISER_IDS = [432328301155975200, 1077197583992234074, 725557525357002812, 1065961258173616228]  # <-- replace with your Discord user id(s)
# Name of the role that will be treated as moderators in permission checks.
MOD_ROLE_NAME = "Moderator"
# File used for local persistence. This keeps everything in a single JSON file.
DATA_FILE = "league_data.json"

# Auto-post config — set POST_CHANNEL_ID to the Discord channel id where you want pairs posted
AUTO_POST_CHANNEL_ID = 1405957655633203445  # <-- replace with the channel id (int) where the bot should post auto-created matches
SHUTTER_POST_CHANNEL_ID = 1406328371234734140  # <-- replace with your #setsforshutter channel id (int)
# Auto-post time (CEST zone handled by TIMEZONE)
AUTO_POST_HOUR = 18   # 17 = 5pm
AUTO_POST_MINUTE = 00  # minute
SHUTTER_POST_HOUR = 17
SHUTTER_POST_MINUTE = 00
REMATCH_BLOCK_DAYS = 20
SHOW_AUTO_CREATED_MATCHES = False


# Timezone handling: try to load system tzdata via zoneinfo. This will give correct
# behavior with DST (CEST). If zoneinfo is unavailable, fall back to pytz or a
# fixed offset timezone (the fixed fallback does NOT handle DST and is only a last resort).
try:
    from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
    TIMEZONE = ZoneInfo("Europe/Berlin")  # CEST (Central European Summer Time / CET+DST)
except Exception:
    try:
        import pytz
        TIMEZONE = pytz.timezone("Europe/Berlin")
        print("zoneinfo not available; falling back to pytz timezone('Europe/Berlin')")
    except Exception:
        # final fallback: fixed offset (UTC+2) — note: this will NOT track DST changes.
        from datetime import timezone, timedelta
        TIMEZONE = timezone(timedelta(hours=2))
        print("Warning: zoneinfo and pytz not available. Falling back to fixed UTC+2 timezone. DST will not be handled.")

# Allowed timeframe for match reporting (local time in TIMEZONE)
REPORT_START = time(18, 0)  # 18:00 local
REPORT_END = time(22, 30)    # 22:30 local
# Signup cutoff for each day: 16:00 local time on the target day
SIGNUP_CUTOFF = time(16, 0)
# Days we allow qualifiers on (lowercase strings used throughout)
VALID_DAYS = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
MAX_QUALIFIERS_PER_WEEK = 3


SIGNUP_CHANNEL_NAME = "✅league-signup✅"
SIGNUP_CHANNEL_ID = 1405956937450918018  # e.g. 123456789012345678  (set to your #signup channel id if you prefer)
# ----------------------------------------

# Bot intents: we need members and message_content to read user mentions and command messages.
intents = discord.Intents.default()
intents.members = True
intents.message_content = True

# Disable the library's default help command so our custom `!help` won't conflict.
bot = commands.Bot(command_prefix="!", intents=intents, help_command=None)

# Simple threading lock to protect file I/O. This reduces the risk of concurrent
# commands in different threads/processes corrupting the JSON file.
IO_LOCK = threading.RLock()


# ---------------- Persistence ----------------

def load_data():
    """Load JSON data from disk safely.

    The function takes the IO_LOCK to avoid concurrent reads/writes from different
    command handlers. If the file doesn't exist yet it creates a base structure and
    writes it using save_data (which does atomic write via a .tmp file + os.replace).
    """
    with IO_LOCK:
        if not os.path.exists(DATA_FILE):
            base = {
                "users": {},           # user_id -> {name, region, registered_on, stats, week_counts}
                "signups": {},         # week_key -> day -> [user_id]
                "matches": {},         # match_id -> match dict
                "suggestions": {},     # day -> list of suggestions
                "week_key": current_week_key(),
                "canceled_days": {},   # week:day -> {timestamp, reason, cancelled_by}
                "auto_posted": {},     # week:day -> timestamp (prevents duplicate auto-posts)
            }
            # Use save_data to write atomically
            save_data(base)
            return base
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            return json.load(f)

def match_line_compact(m: dict, data: dict) -> str:
    """
    Compact single-line match formatter that only mentions the players and shows their region.
    Example output:
    - [6a2a9572] <@123456789> [EU] vs <@987654321> [USA] [forced]
    """
    mid = m.get("id") or "?"
    p1 = m.get("p1") or "?"
    p2 = m.get("p2") or "?"
    # lookup region from data; fallback to '?'
    p1_region = (data.get("users", {}).get(p1, {}) or {}).get("region") or "?"
    p2_region = (data.get("users", {}).get(p2, {}) or {}).get("region") or "?"
    status = "confirmed" if m.get("confirmed") else ("forced" if m.get("forced") else "scheduled")
    score = f" => {m.get('score')}" if m.get("score") else ""
    return f"- [{mid}] <@{p1}> [{p1_region}] vs <@{p2}> [{p2_region}]{score} [{status}]"        
        

# Allowed winner range (change if you want best-of-3/5/7 behavior)
# - For best-of-3 set WIN_MIN=2, WIN_MAX=2
# - For best-of-5 set WIN_MIN=3, WIN_MAX=3
# - For best-of-7 set WIN_MIN=4, WIN_MAX=4
# I default to allowing winners between 2 and 4 inclusive (matches your examples).
WIN_MIN = 3
WIN_MAX = 3

def validate_score_best_of_5(a: int, b: int):
    """
    Enforce best-of-5 rules:
      - winner must have exactly 3 sets
      - loser must be 0, 1 or 2
      - no ties
    Returns (True, "") if valid, else (False, "reason")
    """
    if not (isinstance(a, int) and isinstance(b, int)):
        return False, "Scores must be integers."
    if a == b:
        return False, "Draws are not allowed."
    winner = max(a, b)
    loser = min(a, b)
    if winner != 3:
        return False, "Winner must have exactly 3 sets (best-of-5)."
    if loser < 0 or loser > 2:
        return False, "Loser must have 0, 1 or 2 sets."
    return True, ""



def save_data(data):
    """Save JSON data atomically: write to temp file then replace the real file.

    This reduces the risk of leaving a half-written JSON if the process is killed
    while saving. Filesystem `os.replace` is used since it is atomic on most OSes.
    """
    temp_path = DATA_FILE + ".tmp"
    with IO_LOCK:
        with open(temp_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
            f.flush()
            try:
                os.fsync(f.fileno())
            except Exception:
                # Not critical if fsync fails (e.g. on network filesystems), continue.
                pass
        # Atomic replace
        os.replace(temp_path, DATA_FILE)

def recompute_all_stats(data):
    """
    Rebuild every user's stats from scratch using only entries that have a recorded score
    in data['matches'].
    """
    # reset all stats for existing users
    for uid, u in data.get("users", {}).items():
        u["stats"] = {"wins": 0, "losses": 0, "points": 0, "played": 0, "sets_played": 0, "sets_won": 0}

    # ensure matches exists
    for mid, m in data.get("matches", {}).items():
        if "score" not in m:
            continue
        try:
            a, b = map(int, m["score"].split("-"))
        except Exception:
            # skip malformed scores
            continue

        p1 = m.get("p1"); p2 = m.get("p2")
        if not (p1 and p2):
            continue

        # ensure user entries exist
        for pid in (p1, p2):
            data.setdefault("users", {}).setdefault(pid, {})
            data["users"][pid].setdefault("stats", {"wins": 0, "losses": 0,
                                                   "points": 0, "played": 0,
                                                   "sets_played": 0, "sets_won": 0})

        # apply played / sets_played
        data["users"][p1]["stats"]["played"] = data["users"][p1]["stats"].get("played", 0) + 1
        data["users"][p2]["stats"]["played"] = data["users"][p2]["stats"].get("played", 0) + 1

        data["users"][p1]["stats"]["sets_played"] = data["users"][p1]["stats"].get("sets_played", 0) + (a + b)
        data["users"][p2]["stats"]["sets_played"] = data["users"][p2]["stats"].get("sets_played", 0) + (a + b)

        # sets won
        data["users"][p1]["stats"]["sets_won"] = data["users"][p1]["stats"].get("sets_won", 0) + a
        data["users"][p2]["stats"]["sets_won"] = data["users"][p2]["stats"].get("sets_won", 0) + b

        # wins / losses
        if a > b:
            data["users"][p1]["stats"]["wins"] = data["users"][p1]["stats"].get("wins", 0) + 1
            data["users"][p2]["stats"]["losses"] = data["users"][p2]["stats"].get("losses", 0) + 1
        else:
            data["users"][p2]["stats"]["wins"] = data["users"][p2]["stats"].get("wins", 0) + 1
            data["users"][p1]["stats"]["losses"] = data["users"][p1]["stats"].get("losses", 0) + 1

    # points = sets_won (keeps behaviour consistent with your bot)
    for uid, u in data.get("users", {}).items():
        u["stats"]["points"] = u["stats"].get("sets_won", 0)

    return data


def player_label(uid: str, data: dict) -> str:
    """Return a display label with nickname, mention and region for a user id."""
    user = data.get("users", {}).get(uid, {})
    name = user.get("name") or f"<@{uid}>"
    region = user.get("region") or "unknown"
    return f"{name} (<@{uid}>) [{region}]"


# improved DM helper: returns (True, None) on success, (False, reason_str) on failure
async def dm_user_safe(user_id: str, text: str) -> tuple:
    try:
        uid_int = int(user_id)
    except Exception:
        return False, "invalid_user_id"
    try:
        user = bot.get_user(uid_int) or await bot.fetch_user(uid_int)
        if user is None:
            return False, "user_not_found"
        await user.send(text)
        return True, None
    except discord.Forbidden:
        return False, "forbidden_dm_blocked_or_privacy"
    except Exception as e:
        return False, f"error:{type(e).__name__}:{str(e)}"






def current_week_key_for_date(dt: date):
    wk = dt.isocalendar()
    return f"{wk[0]}-W{wk[1]}"


def current_week_key():
    now = datetime.now(TIMEZONE).date()
    return current_week_key_for_date(now)

# ---------------- Helpers ----------------

def is_organiser(user: discord.User):
    """Return True if the given user is in the ORGANISER_IDS list."""
    return user.id in ORGANISER_IDS


def is_mod(member: discord.Member):
    """Return True if the member is an organiser or has the moderator role.

    Note: member may be a User in some contexts; the code assumes a Member with roles
    when called from a guild command.
    """
    if is_organiser(member):
        return True
    for role in member.roles:
        if role.name == MOD_ROLE_NAME:
            return True
    return False

# helpers (place near top of file or above these functions)
def _parse_week_key(wk):
    try:
        y, w = wk.split("-W")
        return int(y), int(w)
    except Exception:
        return None

def _date_for_weekday(wk, day):
    p = _parse_week_key(wk)
    if not p or not day:
        return None
    year, weeknum = p
    try:
        wd_index = VALID_DAYS.index(day.lower()) + 1  # 1..7
        return date.fromisocalendar(year, weeknum, wd_index)
    except Exception:
        return None

def _group_matches_for_display(matches_iterable):
    """
    Collapse duplicates: group by (week, day, players_set, score).
    Returns list of tuples (representative_match, count) sorted by rep timestamp.
    Representative chosen as earliest timestamp (or first if none).
    """
    groups = {}
    for mid, m in matches_iterable:
        if not m.get("score"):
            continue
        wk = m.get("week"); day = m.get("day"); score = m.get("score")
        p1 = m.get("p1"); p2 = m.get("p2")
        if not (wk and day and p1 and p2 and score):
            # keep as unique group using mid to avoid accidental collapsing of incomplete entries
            key = ("__UNIQUE__", mid)
        else:
            players_set = frozenset((p1, p2))
            key = (wk, day, players_set, score)
        groups.setdefault(key, []).append((mid, m))

    # pick representative (earliest timestamp) for each group
    reps = []
    for key, lst in groups.items():
        def _ts_key(item):
            _, mm = item
            ts = mm.get("timestamp")
            if not ts:
                return ""
            try:
                return datetime.fromisoformat(ts)
            except Exception:
                return ts
        lst_sorted = sorted(lst, key=_ts_key)
        rep_mid, rep_m = lst_sorted[0]
        count = len(lst_sorted)
        reps.append((rep_mid, rep_m, count))
    # sort reps by their timestamp (stable)
    def _rep_sort_key(t):
        _, mm, _ = t
        ts = mm.get("timestamp") or ""
        try:
            return datetime.fromisoformat(ts)
        except Exception:
            return ts
    reps_sorted = sorted(reps, key=_rep_sort_key)
    return reps_sorted



def within_report_window():
    """Check if the current local time falls inside the allowed report window.

    This function respects the REPORT_START and REPORT_END times configured above.
    If REPORT_START <= REPORT_END it is a single contiguous window; otherwise it
    allows wrapping around midnight.
    """
    now = datetime.now(TIMEZONE).time()
    if REPORT_START <= REPORT_END:
        return REPORT_START <= now <= REPORT_END
    else:
        # Window spans midnight (e.g. 22:00 - 03:00)
        return now >= REPORT_START or now <= REPORT_END


def week_reset_if_needed(data):
    # Ensure the data's `week_key` points to the current week. This does not wipe
    # signups; it only updates the pointer and persists the new value.
    wk = current_week_key()
    if data.get("week_key") != wk:
        data["week_key"] = wk
        save_data(data)


def next_occurrence_of_weekday(target_weekday_index: int, from_date: date = None) -> date:
    """Return the next date for the given weekday index (Monday=0..Friday=4).

    If `from_date` is None the calculation uses today's date in the configured timezone.
    The function returns `from_date` itself if it has the matching weekday.
    """
    if from_date is None:
        from_date = datetime.now(TIMEZONE).date()
    today_idx = from_date.weekday()  # Monday=0
    days_ahead = (target_weekday_index - today_idx) % 7
    return from_date + timedelta(days=days_ahead)


def week_key_for_date(d: date):
    return current_week_key_for_date(d)


def ensure_signups_week(data, week_key: str):
    """Ensure the signups structure exists for the given week_key and all valid days."""
    data.setdefault("signups", {})
    data["signups"].setdefault(week_key, {})
    for d in VALID_DAYS:
        data["signups"][week_key].setdefault(d, [])


def normalize_region(r: str):
    """Normalize region strings provided by users (case-insensitive)."""
    if not r:
        return None
    r = r.strip().upper()
    if r in ("EU", "USA"):
        return r
    if r == "NA":
        return "USA"
    return None





def user_stats_summary(user):
    """Return a small tuple summarizing a user's stats for display or ranking logic."""
    s = user.get("stats", {})
    wins = s.get("wins", 0)
    losses = s.get("losses", 0)
    points = s.get("points", 0)
    played = s.get("played", 0)
    sets_played = s.get("sets_played", 0)
    sets_won = s.get("sets_won", 0)
    win_ratio = (wins / played * 100) if played > 0 else 0.0
    return wins, losses, points, played, sets_played, sets_won, win_ratio

def find_duplicate_matches(data):
    """
    Returns a dict mapping key -> list of (mid, match) where key = (week, day, frozenset({p1,p2}))
    Only matches that have both p1 and p2 will be considered.
    """
    from collections import defaultdict
    groups = defaultdict(list)
    for mid, m in list(data.get("matches", {}).items()):
        wk = m.get("week"); dy = m.get("day"); p1 = m.get("p1"); p2 = m.get("p2")
        if not (wk and dy and p1 and p2):
            continue
        key = (wk, dy, frozenset({p1, p2}))
        groups[key].append((mid, m))
    # keep only groups with more than 1 entry
    return {k: v for k, v in groups.items() if len(v) > 1}


def dedupe_matches_keep_latest(data):
    """
    For any duplicate group (same week, day, pair), keep only the entry with the
    newest ISO timestamp (m['timestamp']). Remove other matches from data['matches'].
    Returns a tuple (kept_count, removed_count, removed_mids_list).
    """
    duplicates = find_duplicate_matches(data)
    removed = []
    kept = 0

    for key, entries in duplicates.items():
        # pick the entry with the greatest timestamp
        def _ts_val(item):
            mid, m = item
            ts = m.get("timestamp") or ""
            return ts
        entries_sorted = sorted(entries, key=_ts_val, reverse=True)  # newest first
        keep_mid, keep_m = entries_sorted[0]
        kept += 1
        # remove the rest
        for mid, m in entries_sorted[1:]:
            if mid in data.get("matches", {}):
                del data["matches"][mid]
                removed.append(mid)

    return kept, len(removed), removed


# ---------------- Bot Events ----------------

@bot.event
async def on_message(message):
    """
    Auto-delete messages (both user and bot) in the signup channel after 45 seconds.
    - Uses SIGNUP_CHANNEL_ID if set, otherwise matches channel name against SIGNUP_CHANNEL_NAME.
    - Make sure the bot has 'Manage Messages' permission and its role is above the targets in role order.
    """
    # allow commands to run as usual
    await bot.process_commands(message)

    # ignore DMs / messages outside guilds
    if not message.guild:
        return

    # Resolve target channel by ID (preferred) or by name
    try:
        in_signup = False
        if SIGNUP_CHANNEL_ID:
            in_signup = (message.channel.id == SIGNUP_CHANNEL_ID)
        else:
            in_signup = (message.channel.name.lower() == SIGNUP_CHANNEL_NAME.lower())
    except Exception:
        # if something odd happens (missing constants etc.), don't crash
        return

    if not in_signup:
        return

    # schedule deletion after 45 seconds (best-effort)
    try:
        # message.delete supports a `delay` parameter in discord.py; it's simple and works for most versions
        await message.delete(delay=45)
    except TypeError:
        # if your discord.py doesn't accept delay, fallback to asyncio sleep + delete
        try:
            await asyncio.sleep(45)
            await message.delete()
        except Exception as e:
            print(f"[signup-auto-delete] failed to delete message {message.id}: {e}")
    except Exception as e:
        # other errors (permissions, missing message, rate-limit etc.)
        print(f"[signup-auto-delete] failed to delete message {message.id}: {e}")


@bot.event
async def on_ready():
    # Called once the bot has connected and is ready.
    print(f"Logged in as {bot.user} (ID: {bot.user.id})")
    weekly_reset_task.start()
    auto_pair_and_post.start()

@bot.check
async def restrict_non_admin_commands(ctx):
    """Allow non-admin commands only in the signup channel."""
    # Mods and organisers can run commands anywhere
    if is_mod(ctx.author):
        return True

    # Check if the command is being run in the correct channel
    in_signup_channel = (
        ctx.channel.id == SIGNUP_CHANNEL_ID
        or ctx.channel.name.lower() == SIGNUP_CHANNEL_NAME.lower()
    )

    if not in_signup_channel:
        await ctx.reply(f"You can only use league commands in {SIGNUP_CHANNEL_NAME}.")
        return False

    return True


# ---------------- Background Tasks ----------------

@tasks.loop(minutes=30)
async def weekly_reset_task():
    # Periodically check and update the week_key stored in the JSON file.
    data = load_data()
    week_reset_if_needed(data)


@tasks.loop(minutes=1)
async def auto_pair_and_post():
    """Auto-pair unmatched signups (by region) and post:
       - All scheduled matches for the day (existing + newly created)
       - Unmatched players (grouped by region / unknown)
       Additionally, DM participants for all matches posted that haven't been notified yet.
    """
    # if both post channels are unset, nothing to do
    if AUTO_POST_CHANNEL_ID is None and SHUTTER_POST_CHANNEL_ID is None:
        return

    now = datetime.now(TIMEZONE)
    # run only at one of the configured times (either shutter or auto time)
    is_auto_time = (now.hour == AUTO_POST_HOUR and now.minute == AUTO_POST_MINUTE)
    is_shutter_time = (now.hour == SHUTTER_POST_HOUR and now.minute == SHUTTER_POST_MINUTE)
    if not (is_auto_time or is_shutter_time):
        return

    today_day = now.strftime('%A').lower()
    if today_day not in VALID_DAYS:
        return

    data = load_data()
    week_reset_if_needed(data)
    wk = data.get("week_key")
    ensure_signups_week(data, wk)

    # day-level key and per-post-time key (so 17:30 and 18:00 are tracked independently)
    day_key = f"{wk}:{today_day}"
    post_key = f"{day_key}:{now.hour:02d}{now.minute:02d}"

    # decide target channel id for this run
    if is_shutter_time and SHUTTER_POST_CHANNEL_ID is not None:
        target_channel_id = SHUTTER_POST_CHANNEL_ID
    else:
        target_channel_id = AUTO_POST_CHANNEL_ID


    # If cancelled, post cancellation notice and mark as posted
    # If cancelled, post cancellation notice and mark as posted for this post-time
    if data.get("canceled_days", {}).get(day_key):
        if data.get("auto_posted", {}).get(post_key):
            return
        channel = bot.get_channel(target_channel_id)
        if not channel:
            try:
                channel = await bot.fetch_channel(target_channel_id)
            except Exception:
                channel = None
        msg = f"Qualifiers for {today_day.capitalize()} (week {wk}) have been cancelled."
        cancel_info = data["canceled_days"].get(day_key, {})
        if cancel_info.get("reason"):
            msg += f" Reason: {cancel_info.get('reason')}"
        if channel:
            try:
                await channel.send(msg)
            except Exception:
                pass
        data.setdefault("auto_posted", {})[post_key] = datetime.now(TIMEZONE).isoformat()
        save_data(data)
        return


    # prevent duplicate runs for same day/week/post-time
    if data.get("auto_posted", {}).get(post_key):
        return



    # Gather signups and already scheduled matches for the day/week
    ensure_signups_week(data, wk)
    signed = list(data["signups"][wk].get(today_day, []))

    matched = set()
    existing_matches = []
    for mid, m in data.get("matches", {}).items():
        if m.get("week") == wk and m.get("day") == today_day:
            existing_matches.append(m)
            p1 = m.get("p1"); p2 = m.get("p2")
            if p1:
                matched.add(p1)
            if p2:
                matched.add(p2)

    # Unmatched signups (not present in any scheduled match)
    unmatched = [u for u in signed if u not in matched]

    # Group unmatched by region
    by_region = {"EU": [], "USA": []}
    unknown_region = []
    for uid in unmatched:
        user = data.get("users", {}).get(uid)
        if not user:
            unknown_region.append(uid)
            continue
        reg = user.get("region")
        if reg in by_region:
            by_region[reg].append(uid)
        else:
            unknown_region.append(uid)

    # Auto-create matches within each region
    # -------------------------
    # Auto-create matches within each region (avoid recent rematches if possible)
    # -------------------------
    # uses REMATCH_BLOCK_DAYS from config
    def played_recently(a_id: str, b_id: str, days: int, data: dict) -> bool:
        """Return True if players a and b have a recorded match within the last `days` days."""
        cutoff_date = (datetime.now(TIMEZONE) - timedelta(days=days)).date()
        for mm in data.get("matches", {}).values():
            p1 = mm.get("p1"); p2 = mm.get("p2")
            if not (p1 and p2):
                continue
            if set((p1, p2)) != set((a_id, b_id)):
                continue
            ts = mm.get("timestamp")
            if ts:
                try:
                    mdate = datetime.fromisoformat(ts).date()
                except Exception:
                    continue
            else:
                wk = mm.get("week"); day = mm.get("day")
                if wk and day:
                    d = _date_for_weekday(wk, day)
                    if d:
                        mdate = d
                    else:
                        continue
                else:
                    continue
            if mdate >= cutoff_date:
                return True
        return False

    suggestions = []  # list of (a,b,mid) for matches created in this run

    for reg, lst in by_region.items():
        candidates = lst[:]  # copy of signups for this region
        random.shuffle(candidates)    # <-- shuffle to randomize pairing order
        used = set()

        # Pass 1: try to pair avoiding recent rematches
        for a in candidates:
            if a in used:
                continue
            partner = None
            for b in candidates:
                if b == a or b in used:
                    continue
                try:
                    if not played_recently(a, b, REMATCH_BLOCK_DAYS, data):
                        partner = b
                        break
                except Exception:
                    # on unexpected error in check, allow pairing
                    partner = b
                    break
            if partner:
                mid = str(uuid.uuid4())[:8]
                match = {
                    "id": mid,
                    "p1": a,
                    "p2": partner,
                    "day": today_day,
                    "week": wk,
                    "reported_by": None,
                    "score": None,
                    "confirmed": False,
                    "timestamp": datetime.now(TIMEZONE).isoformat(),
                    "forced": True
                }
                data.setdefault("matches", {})[mid] = match
                suggestions.append((a, partner, mid))
                used.add(a); used.add(partner)

        # Pass 2: pair leftovers sequentially (allow rematches only if unavoidable)
        leftovers = [u for u in candidates if u not in used]
        for i in range(0, len(leftovers) - 1, 2):
            a = leftovers[i]; b = leftovers[i + 1]
            if a == b:
                continue
            mid = str(uuid.uuid4())[:8]
            match = {
                "id": mid,
                "p1": a,
                "p2": b,
                "day": today_day,
                "week": wk,
                "reported_by": None,
                "score": None,
                "confirmed": False,
                "timestamp": datetime.now(TIMEZONE).isoformat(),
                "forced": True
            }
            data.setdefault("matches", {})[mid] = match
            suggestions.append((a, b, mid))
            used.add(a); used.add(b)


    # After creating auto matches, recompute "all matches" for posting
    # --- Cross-region pairing (EU <-> USA) ---
# Build "matched_today" after the within-region pairing above
matched_today = set()
for m in data.get("matches", {}).values():
    if m.get("week") == wk and m.get("day") == today_day:
        if m.get("p1"):
            matched_today.add(m["p1"])
        if m.get("p2"):
            matched_today.add(m["p2"])

# Who is still unmatched today?
still_unmatched = [u for u in signed if u not in matched_today]

# Split leftovers by region (NA is normalized to "USA")
eu_left = [u for u in still_unmatched if (data.get("users", {}).get(u) or {}).get("region") == "EU"]
us_left = [u for u in still_unmatched if (data.get("users", {}).get(u) or {}).get("region") == "USA"]

import random
random.shuffle(eu_left)
random.shuffle(us_left)

def pick_partner(a, pool):
    """Prefer someone you haven't played recently; else take any."""
    for b in pool:
        if a == b:
            continue
        try:
            if not played_recently(a, b, REMATCH_BLOCK_DAYS, data):
                return b
        except Exception:
            return b
    # No fresh opponent? allow a rematch
    return pool[0] if pool else None

def create_pair(a, b):
    if not a or not b or a == b:
        return
    mid = str(uuid.uuid4())[:8]
    match = {
        "id": mid,
        "p1": a,
        "p2": b,
        "day": today_day,
        "week": wk,
        "reported_by": None,
        "score": None,
        "confirmed": False,
        "timestamp": datetime.now(TIMEZONE).isoformat(),
        "forced": True
    }
    data.setdefault("matches", {})[mid] = match
    suggestions.append((a, b, mid))

# Cross-pair to fix imbalances (both directions)
used = set()
for primary, secondary in ((eu_left, us_left), (us_left, eu_left)):
    for a in list(primary):
        if a in used:
            continue
        candidates = [b for b in secondary if b not in used]
        if not candidates:
            continue
        partner = pick_partner(a, candidates)
        if partner:
            create_pair(a, partner)
            used.add(a); used.add(partner)

# -------------------------
# AFTER creating auto matches, recompute posting lists and leftovers cleanly
# -------------------------
# Recompute all matches (existing + newly created) and build the matched set
    all_matches = []
    matched = set()
    for mid, m in data.get("matches", {}).items():
        if m.get("week") == wk and m.get("day") == today_day:
            all_matches.append(m)
            p1 = m.get("p1"); p2 = m.get("p2")
            if p1:
                matched.add(p1)
            if p2:
                matched.add(p2)

    # Deduplicate the signups list (preserve order) to avoid duplicate signup entries
    seen = set()
    dedup_signed = []
    for u in signed:
        if u not in seen:
            dedup_signed.append(u)
            seen.add(u)

    # Recompute unmatched *after* matches were created
    unmatched_after = [u for u in dedup_signed if u not in matched]

    # Group unmatched by region (fresh)
    by_region = {"EU": [], "USA": []}
    unknown_region = []
    for uid in unmatched_after:
        user = data.get("users", {}).get(uid)
        if not user:
            unknown_region.append(uid)
            continue
        reg = user.get("region")
        if reg in by_region:
            by_region[reg].append(uid)
        else:
            unknown_region.append(uid)

    # Leftovers: singletons and unknown-region (now derived from the post-pairing unmatched set)
    leftovers = []
    for reg, lst in by_region.items():
        if len(lst) % 2 == 1:
            leftovers.append(("region", reg, lst[-1]))
    for u in unknown_region:
        leftovers.append(("unknown", None, u))

    # persist changes (we created matches earlier, still safe to save here)
    save_data(data)



    # Build message: show all scheduled matches + unmatched players
    channel = None
    if target_channel_id is not None:
        channel = bot.get_channel(target_channel_id)
        if not channel:
            try:
                channel = await bot.fetch_channel(target_channel_id)
            except Exception:
                channel = None
    # fallback to ctx.channel is not available in the scheduled task,
    # so if channel is None we'll skip sending (existing behavior was fail-silent)


    lines = [f"Qualifiers for {today_day.capitalize()} (week {wk}):"]

    # List all scheduled matches (existing + auto-created)
    if all_matches:
        lines.append("\n**All scheduled matches:**")
        sorted_matches = sorted(all_matches, key=lambda m: (m.get("timestamp") or "", m.get("id")))
        for m in sorted_matches:
            lines.append(match_line_compact(m, data))
    else:
        lines.append("No scheduled matches for today.")

# Optionally include which matches were auto-created (disabled by default)
    if SHOW_AUTO_CREATED_MATCHES and suggestions:
        lines.append("\nAuto-created matches (paired within-region):")
        for a, b, mid in suggestions:
            lines.append(f"- <@{a}> vs <@{b}>  — match id: `{mid}`")


    # Show unmatched players
    if leftovers:
        lines.append("\nPlayers left unmatched (not auto-paired):")
        for typ, reg, uid in leftovers:
            if typ == "region":
                lines.append(f"- <@{uid}> (region: {reg})")
            else:
                lines.append(f"- <@{uid}> (region: unknown / not registered)")
    else:
        lines.append("\nNo unmatched players (all signups paired).")

    # Post the message to the configured channel (or fail silently)
    if channel:
        try:
            await channel.send("\n".join(lines))
        except Exception:
            # channel send failed; we already persisted auto_posted and matches, so continue
            pass

    # -------------------------------
    # DM participants for matches that are part of this post.
    # Behavior change: attempt to DM every match listed in `all_matches`
    if is_auto_time:
        dm_failures = []
        dm_successes = []

        # Build list of matches to notify: all matches from today's post (all_matches)
        matches_to_notify = []
        for m in all_matches:
            # keep only well-formed matches
            if not (m.get("id") and m.get("p1") and m.get("p2")):
                continue
            matches_to_notify.append(m)

        # De-duplicate by id while preserving order
        seen = set()
        matches_ordered = []
        for m in matches_to_notify:
            mid = m.get("id")
            if mid and mid not in seen:
                seen.add(mid)
                matches_ordered.append(m)

        print(f"[autopair DM] will attempt to DM {len(matches_ordered)} matches (all_matches={len(all_matches)}, suggestions={len(suggestions) if 'suggestions' in locals() else 0})")

        # Attempt DMs for each match
        for m in matches_ordered:
            mid = m.get("id")
            a = m.get("p1"); b = m.get("p2")
            if not (mid and a and b):
                continue

            p1label = data.get("users", {}).get(a, {}).get("name") or f"<@{a}>"
            p2label = data.get("users", {}).get(b, {}).get("name") or f"<@{b}>"
            status = "confirmed" if m.get("confirmed") else ("forced" if m.get("forced") else "scheduled")
            score_text = f" Score: {m.get('score')}" if m.get("score") else ""

            text_for_a = (
                f"Hi {p1label},\n"
                f"You have a qualifier scheduled for {today_day.capitalize()} (week {wk}).\n"
                f"Opponent: {p2label}\n"
                f"Match ID: {mid}\n"
                f"Status: {status}.{score_text}\n"
                "Report the result using the usual command in the server."
            )
            text_for_b = (
                f"Hi {p2label},\n"
                f"You have a qualifier scheduled for {today_day.capitalize()} (week {wk}).\n"
                f"Opponent: {p1label}\n"
                f"Match ID: {mid}\n"
                f"Status: {status}.{score_text}\n"
                "Report the result using the usual command in the server."
            )

            ok_a, reason_a = await dm_user_safe(a, text_for_a)
            await asyncio.sleep(0.35)
            ok_b, reason_b = await dm_user_safe(b, text_for_b)
            await asyncio.sleep(0.35)

            print(f"[autopair DM] match={mid} a={a} ok_a={ok_a} reason_a={reason_a} b={b} ok_b={ok_b} reason_b={reason_b}")

            if ok_a or ok_b:
                dm_successes.append(mid)
                if mid in data.get("matches", {}):
                    data["matches"][mid]["notified"] = True
            else:
                dm_failures.append((mid, a, reason_a, b, reason_b))

        # Persist notified flags if anything changed
        if dm_successes or dm_failures:
            save_data(data)

        # Post a short failure summary for moderators if there were failures
        if dm_failures and channel:
            try:
                fail_lines = [f"DM failures for today's qualifiers ({today_day.capitalize()}, week {wk}):"]
                for mid, a, ra, b, rb in dm_failures:
                    fail_lines.append(f"- match `{mid}`: <@{a}> => `{ra}`, <@{b}> => `{rb}`")
                await channel.send("\n".join(fail_lines))
            except Exception:
                print("[autopair DM] failed to post DM-failure summary to channel.")
    else:
        # Skipping DMs because this is a SHUTTER_POST run (we will let the AUTO_POST run handle DMs)
        print("[autopair DM] skipping DMs for SHUTTER_POST to avoid revealing pairings early.")    






@bot.command(name="testdm")
@commands.check(lambda ctx: is_mod(ctx.author))
async def testdm(ctx, member: discord.Member = None):
    """
    Test whether the bot can DM the target member (or the command invoker if omitted).
    Usage: !testdm @user
    """
    target = member or ctx.author
    uid = str(target.id)
    text = (
        f"Hello {target.display_name}, this is a DM test from the league bot. "
        "If you don't get this, please enable DMs from server members or check your block list."
    )
    ok, reason = await dm_user_safe(uid, text)
    if ok:
        await ctx.reply(f"DM sent to {target.display_name}.")
    else:
        await ctx.reply(f"Failed to DM {target.display_name}. Reason: `{reason}`. Ask them to enable server DMs or check block list.")



# ---------------- Commands ----------------

@bot.command(name="register")
async def register(ctx, name: str = None, region: str = None):
    """!register nickname EU/USA - register for the league (region required)

    This command stores the player's chosen nickname and region and initializes
    their stats structure in the JSON store.
    """
    data = load_data()
    if not name or not region:
        await ctx.reply("Usage: `!register <nickname> <EU|USA>` — region is required (EU or USA).")
        return
    reg = normalize_region(region)
    if not reg:
        await ctx.reply("Region invalid. Use `EU` or `USA`.")
        return
    uid = str(ctx.author.id)
    if uid in data.get("users", {}):
        await ctx.reply("You are already registered.")
        return
    data.setdefault("users", {})
    data["users"][uid] = {
        "name": name,
        "region": reg,
        "registered_on": datetime.now(TIMEZONE).isoformat(),
        "stats": {"wins": 0, "losses": 0, "points": 0, "played": 0, "sets_played": 0, "sets_won": 0},
        "week_counts": {}
    }
    save_data(data)
    await ctx.reply(f"Registered {name} ({reg}). Now !signup for the days you want. Good luck!")


@bot.command(name="signup")
async def signup(ctx, day: str):
    """
    !signup [day] - sign up for a qualifier day (mon-fri).
    Only registered, non-bot users may sign up.
    """
    data = load_data()
    week_reset_if_needed(data)

    # Safety: disallow bots and webhooks
    if getattr(ctx.author, "bot", False):
        await ctx.reply("Bots cannot sign up.")
        return

    # Normalize and validate day
    day_l = day.lower() if isinstance(day, str) else ""
    if day_l not in VALID_DAYS:
        await ctx.reply(f"Invalid day. Pick one of: {', '.join(VALID_DAYS)}")
        return

    uid = str(ctx.author.id)

    # Strong registration check: require user record + name + region
    user = data.get("users", {}).get(uid)
    if not user or not user.get("name") or not user.get("region"):
        await ctx.reply("You must register first with `!register <nickname> <EU|USA>` before signing up.")
        return

    target_weekday_index = VALID_DAYS.index(day_l)
    today = datetime.now(TIMEZONE).date()
    target_date = next_occurrence_of_weekday(target_weekday_index, from_date=today)
    cutoff_dt = datetime.combine(target_date, SIGNUP_CUTOFF).replace(tzinfo=TIMEZONE)
    now_dt = datetime.now(TIMEZONE)

    # If cutoff has already passed for that day, move to the next week's occurrence
    if now_dt >= cutoff_dt:
        target_date = target_date + timedelta(days=7)

    target_week_key = week_key_for_date(target_date)
    ensure_signups_week(data, target_week_key)

    # check duplicates
    if uid in data["signups"][target_week_key][day_l]:
        await ctx.reply(f"You are already signed up for {day_l.capitalize()} (week {target_week_key}).")
        return

    # check weekly cap
    user.setdefault("week_counts", {})
    curr_count = user["week_counts"].get(target_week_key, 0)
    if curr_count >= MAX_QUALIFIERS_PER_WEEK:
        await ctx.reply(f"You have reached your {MAX_QUALIFIERS_PER_WEEK} qualifiers/week limit for week {target_week_key}.")
        return

    # All good — add signup
    data["signups"][target_week_key][day_l].append(uid)
    user["week_counts"][target_week_key] = curr_count + 1
    save_data(data)
    await ctx.reply(f"Signed up for {day_l.capitalize()} (week {target_week_key}).")



@bot.command(name="mysignups")
async def mysignups(ctx):
    data = load_data()
    week_reset_if_needed(data)
    uid = str(ctx.author.id)
    if uid not in data.get("users", {}):
        await ctx.reply("You are not registered.")
        return

    from datetime import datetime, timedelta
    import re

    def week_key_to_range_str(wk_key):
        try:
            m = re.match(r"^(\d{4})-W(\d{1,2})$", wk_key)
            if not m:
                return "week range unknown"
            year = int(m.group(1)); week = int(m.group(2))
            monday = datetime.fromisocalendar(year, week, 1).date()
            sunday = monday + timedelta(days=6)
            if monday.year == sunday.year:
                return f"{monday.strftime('%b %d')} — {sunday.strftime('%b %d, %Y')}"
            else:
                return f"{monday.strftime('%b %d, %Y')} — {sunday.strftime('%b %d, %Y')}"
        except Exception:
            return "week range unknown"

    now = datetime.now(TIMEZONE).date()
    lines = []

    # Current week (always shown)
    cur_key = week_key_for_date(now)
    ensure_signups_week(data, cur_key)
    cur_days = [d.capitalize() for d in VALID_DAYS if uid in data["signups"][cur_key][d]]
    if cur_days:
        lines.append(f"Week {cur_key} ({week_key_to_range_str(cur_key)}): " + ", ".join(cur_days))
    else:
        lines.append(f"Week {cur_key} ({week_key_to_range_str(cur_key)}): You have no signups this week.")

    # Next week (shown only if it has signups)
    next_date = now + timedelta(days=7)
    next_key = week_key_for_date(next_date)
    ensure_signups_week(data, next_key)
    next_days = [d.capitalize() for d in VALID_DAYS if uid in data["signups"][next_key][d]]
    if next_days:
        lines.append(f"Week {next_key} ({week_key_to_range_str(next_key)}): " + ", ".join(next_days))

    await ctx.reply("Your signups:\n" + "\n".join(lines))




@bot.command(name="signuplist")
@commands.check(lambda ctx: is_mod(ctx.author))
async def signuplist(ctx, day: str, week_key: str = None):
    """!signuplist <day> [week_key] - show signups for a single day (one name per line),
    include region for each user. If no week_key given, picks the upcoming day: if that day's
    occurrence this week already passed, select next week's occurrence."""
    data = load_data()
    week_reset_if_needed(data)

    day_l = day.lower()
    if day_l not in VALID_DAYS:
        await ctx.reply(f"Invalid day. Pick one of: {', '.join(VALID_DAYS)}")
        return

    now = datetime.now(TIMEZONE).date()
    if not week_key:
        # map day names explicitly to ISO weekday numbers (Monday=1 .. Sunday=7)
        day_to_iso = {
            "monday": 1, "tuesday": 2, "wednesday": 3, "thursday": 4,
            "friday": 5, "saturday": 6, "sunday": 7
        }
        target_wd = day_to_iso.get(day_l)
        today_wd = now.isoweekday()
        # if this week's target already passed (today_wd > target_wd) -> choose next week's
        if today_wd > target_wd:
            days_ahead = 7 - (today_wd - target_wd)
        else:
            days_ahead = target_wd - today_wd
        target_date = now + timedelta(days=days_ahead)
        week_key = week_key_for_date(target_date)

    ensure_signups_week(data, week_key)

    uids = data.get("signups", {}).get(week_key, {}).get(day_l, [])
    if not uids:
        await ctx.reply(f"No signups for {day_l.capitalize()} (week {week_key}).")
        return

    # Group by region and order by EU, USA, unknown. Each signup on its own line.
    lines = [f"Signups for {day_l.capitalize()} (week {week_key}):"]
    buckets = {"EU": [], "USA": [], "unknown": []}
    for uid in uids:
        user = data.get("users", {}).get(uid, {})
        region = user.get("region") or "unknown"
        key = region if region in ("EU", "USA") else "unknown"
        buckets[key].append(uid)

    def sort_key(u):
        return (data.get("users", {}).get(u, {}).get("name") or "").lower() or u

    for reg_key in ("EU", "USA", "unknown"):
        lst = buckets.get(reg_key, [])
        if not lst:
            continue
        lines.append(f"{reg_key}:")
        for uid in sorted(lst, key=sort_key):
            user = data.get("users", {}).get(uid, {})
            display_name = user.get("name")
            if display_name:
                lines.append(f"- <@{uid}> [{reg_key}] — {display_name}")
            else:
                lines.append(f"- <@{uid}> [{reg_key}]")

    content = "\n".join(lines)
    if len(content) > 1900:
        path = f"signuplist_{day_l}_{week_key}.txt"
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)
        await ctx.reply(file=discord.File(path))
    else:
        await ctx.reply(content)

@bot.command(name="signups")
@commands.check(lambda ctx: is_mod(ctx.author))
async def signups(ctx, member: discord.Member = None, week: str = None):
    """
    Usage:
      !signups @player        -> show that player's signups (all weeks)
      !signups @player 2025-W34 -> show that player's signups for a specific week
    Admin-only (uses is_mod).
    """
    from collections import defaultdict
    import io

    if member is None:
        await ctx.send("Usage: `!signups @player [week]` — you must mention a player.")
        return

    # Load data: prefer load_data() if available, otherwise fall back to the module-level 'data' var.
    try:
        data = load_data()
    except Exception:
        # don't use `global data` here (that causes the SyntaxError if `data` was assigned earlier)
        data = globals().get("data")

    if not data:
        await ctx.send("No data loaded.")
        return

    signups = data.get("signups", None)
    if signups is None:
        await ctx.send("No signup data present.")
        return

    uid_str = str(member.id)
    found = defaultdict(list)  # week -> list of days (or tuples (day,info))

    # Most common shape in your bot: signups[week][day] = [user entries]
    if isinstance(signups, dict):
        for wk, days in signups.items():
            if not isinstance(days, dict):
                continue
            for day, users_list in days.items():
                if not isinstance(users_list, list):
                    continue
                for u in users_list:
                    matched = False
                    info = None
                    # user stored as plain int/str id
                    if isinstance(u, (int, str)) and str(u) == uid_str:
                        matched = True
                    # user stored as mention string "<@123>"
                    elif isinstance(u, str) and f"<@{uid_str}>" in u:
                        matched = True
                    # user stored as dict with id/user fields
                    elif isinstance(u, dict):
                        for key in ("id", "user", "uid", "player"):
                            if key in u and str(u.get(key)) == uid_str:
                                matched = True
                                info = {k: v for k, v in u.items() if k not in ("id", "user", "uid", "player")}
                                break
                    if matched:
                        found[wk].append((day, info))
                        break  # don't double-add the same day if multiple entries

    # If nothing found yet, try alternate shapes:
    if not found and isinstance(signups, dict):
        if uid_str in signups or member.id in signups:
            entries = signups.get(uid_str) or signups.get(member.id)
            if isinstance(entries, list):
                for e in entries:
                    if isinstance(e, dict):
                        wk = e.get("week") or "unknown-week"
                        day = e.get("day") or "unknown-day"
                        info = {k: v for k, v in e.items() if k not in ("week", "day")}
                        found[wk].append((day, info))
                    else:
                        found["unknown-week"].append((str(e), None))

    # shape: flat list of dicts
    if not found and isinstance(signups, list):
        for e in signups:
            if not isinstance(e, dict):
                continue
            candidates = [e.get(k) for k in ("user", "id", "uid", "player")]
            if any(str(x) == uid_str for x in candidates if x is not None):
                wk = e.get("week") or "unknown-week"
                day = e.get("day") or "unknown-day"
                info = {k: v for k, v in e.items() if k not in ("week", "day", "user", "id", "uid", "player")}
                found[wk].append((day, info))

    # fallback: deep scan for occurrences
    if not found:
        def scan(obj, path=""):
            if isinstance(obj, dict):
                for k, v in obj.items():
                    scan(v, f"{path}/{k}")
            elif isinstance(obj, list):
                for item in obj:
                    if isinstance(item, (str, int)) and str(item) == uid_str:
                        found["unknown"].append((f"found at {path}", None))
                    elif isinstance(item, dict):
                        for key in ("id", "user", "uid", "player"):
                            if key in item and str(item[key]) == uid_str:
                                wk = item.get("week") or "unknown-week"
                                day = item.get("day") or f"found at {path}"
                                info = {k: v for k, v in item.items() if k not in ("week","day","id","user","uid","player")}
                                found[wk].append((day, info))
                                break
        scan(signups)

    # apply optional week filter
    if week:
        filtered = defaultdict(list)
        for wk, days in found.items():
            if wk == week:
                filtered[wk] = days
        found = filtered

    # prepare output
    if not found:
        await ctx.send(f"No signups found for {member.mention}.")
        return

    lines = [f"Signups for {member.display_name} ({member.mention}):"]
    for wk in sorted(found.keys()):
        lines.append(f"\nWeek {wk}:")
        for day, info in found[wk]:
            if info:
                lines.append(f"- {day} — {info}")
            else:
                lines.append(f"- {day}")

    out = "\n".join(lines)
    if len(out) <= 1900:
        await ctx.send(out)
    else:
        # send as file if too long
        buf = io.StringIO(out)
        buf.seek(0)
        await ctx.send(file=discord.File(fp=buf, filename=f"signups_{member.id}.txt"))

@bot.command(name="unplayed")
@commands.check(lambda ctx: is_mod(ctx.author))
async def unplayed(ctx, day: str, week_key: str = None):
    """!unplayed <day> [week_key] - list scheduled matches for that day/week that have no score yet."""
    data = load_data()
    week_reset_if_needed(data)

    day_l = day.lower()
    if day_l not in VALID_DAYS:
        await ctx.reply(f"Invalid day. Pick one of: {', '.join(VALID_DAYS)}")
        return

    if not week_key:
        week_key = data.get("week_key")
    ensure_signups_week(data, week_key)

    # collect matches for that day/week that have no score
    matches = [(mid, m) for mid, m in data.get("matches", {}).items()
               if m.get("week") == week_key and m.get("day") == day_l and not m.get("score")]

    if not matches:
        await ctx.reply(f"No unplayed (scheduled) matches for {day_l.capitalize()} (week {week_key}).")
        return

    # reuse existing compact formatter
    lines = [f"Unplayed matches for {day_l.capitalize()} (week {week_key}):"]
    # sort for deterministic output
    for mid, m in sorted(matches, key=lambda it: (it[1].get("timestamp") or "", it[0])):
        lines.append(match_line_compact(m, data))

    content = "\n".join(lines)
    if len(content) > 1900:
        path = f"unplayed_{day_l}_{week_key}.txt"
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)
        await ctx.reply(file=discord.File(path))
    else:
        await ctx.reply(content)


@bot.command(name="report")
async def report(ctx, score: str):
    """
    !report 3-0
    Both players can report. If both reports agree (after converting to p1-p2)
    the result is confirmed and stats updated. If they differ, both players
    receive the 'discussion' role and the match is flagged for staff.
    """
    import discord
    import re
    from datetime import datetime as _dt

    data = load_data()
    week_reset_if_needed(data)

    author_id = str(ctx.author.id)
    if author_id not in data.get("users", {}):
        await ctx.reply("You must be registered in the league to report results.")
        return

    # parse score token (reporter-perspective)
    try:
        r_str, o_str = score.split("-")
        r_score = int(r_str); o_score = int(o_str)
    except Exception:
        await ctx.reply("Score format invalid. Use e.g. `3-1`.")
        return

    ok, reason = validate_score_best_of_5(r_score, o_score)
    if not ok:
        await ctx.reply(f"Invalid score: {reason}")
        return

    # time window check (mods bypass)
    if not within_report_window() and not is_mod(ctx.author):
        await ctx.reply("Reporting is only allowed during the report window unless a moderator/organiser uses `!forcereport`.")
        return

    now = datetime.now(TIMEZONE)
    today_day = now.strftime('%A').lower()
    wk = data.get("week_key")

    # 1) Find candidate matches where reporter is a participant and week/day match
    candidates = []
    for mid, m in data.get("matches", {}).items():
        if m.get("week") != wk:
            continue
        if (m.get("day") or "").lower() != today_day:
            continue
        if author_id in (m.get("p1"), m.get("p2")):
            candidates.append((mid, m))

    if not candidates:
        await ctx.reply(
            "No scheduled/autopaired match found for you today/week. "
            "You can only use `!report` for matches that were autopaired or scheduled by staff. "
            "If you believe this is an error, ask a moderator to use `!forcereport`."
        )
        return

    # prefer unscored/unreported/unconfirmed matches first
    unscored = [(mid, m) for (mid, m) in candidates if not m.get("score") and not m.get("confirmed")]
    chosen_candidates = unscored if unscored else candidates

    # prefer forced/autopaired if present
    prefer_pref = []
    for mid, m in chosen_candidates:
        if m.get("forced") or m.get("autopaired") or m.get("autopair"):
            prefer_pref.append((mid, m))
    if prefer_pref:
        chosen_candidates = prefer_pref

    # final tie-break: earliest timestamp
    def _ts_key(item):
        _, mm = item
        ts = mm.get("timestamp") or ""
        try:
            return datetime.fromisoformat(ts)
        except Exception:
            return datetime.max
    chosen_candidates.sort(key=_ts_key)
    mid, m = chosen_candidates[0]

    # Reject if already confirmed historically
    if m.get("confirmed") and m.get("score"):
        await ctx.reply(f"That match (id `{mid}`) is already recorded as {m.get('score')}. Ask a mod to fix it if needed.")
        return

    # prepare reports container
    if "reports" not in m:
        m["reports"] = {}  # stores per-author reports as p1/p2 canonical values

    # compute canonical p1-p2 for this reporter
    p1 = m.get("p1"); p2 = m.get("p2")
    if author_id == p1:
        p1_score = r_score
        p2_score = o_score
    else:
        p1_score = o_score
        p2_score = r_score

    # store/update this reporter's report
    with IO_LOCK:
        data = load_data()  # reload under lock
        # re-fetch match reference under locked data
        m_locked = data.get("matches", {}).get(mid)
        if m_locked is None:
            await ctx.reply("Match vanished from data (race). Ask a moderator to check.")
            return
        if m_locked.get("confirmed"):
            await ctx.reply(f"That match (id `{mid}`) was just confirmed by someone else as {m_locked.get('score')}.")
            return

        # write/update reporter's entry
        m_locked.setdefault("reports", {})
        m_locked["reports"][author_id] = {
            "p1_score": p1_score,
            "p2_score": p2_score,
            "raw": score,
            "timestamp": now.isoformat()
        }

        # check how many distinct player reports we have (only consider p1/p2 players)
        reports_for_match = {k: v for k, v in m_locked["reports"].items() if k in (m_locked.get("p1"), m_locked.get("p2"))}
        # if only one report so far, persist and ask for opponent report
        if len(reports_for_match) == 1:
            save_data(data)
            await ctx.reply(
                f"Your report `{score}` has been recorded for match `{mid}`. "
                "Waiting for the opponent to also report to auto-confirm. If they don't report, a moderator can resolve it."
            )
            return

        # There are reports from both players — compare them
        # get the two canonical reports (they might be stored in any order)
        reps = list(reports_for_match.values())
        rep0 = reps[0]; rep1 = reps[1]
        # compare canonical p1-p2
        if rep0["p1_score"] == rep1["p1_score"] and rep0["p2_score"] == rep1["p2_score"]:
            # both agree -> confirm and update stats
            confirmed_p1 = rep0["p1_score"]
            confirmed_p2 = rep0["p2_score"]
            m_locked["score"] = f"{confirmed_p1}-{confirmed_p2}"
            m_locked["confirmed"] = True
            m_locked["reported_by"] = list(reports_for_match.keys())
            m_locked["timestamp"] = now.isoformat()

            # update stats (same logic as previous)
            for pid in (m_locked.get("p1"), m_locked.get("p2")):
                u = data["users"].setdefault(pid, {})
                u.setdefault("stats", {"wins":0,"losses":0,"points":0,"played":0,"sets_played":0,"sets_won":0})
                u["stats"]["played"] = u["stats"].get("played", 0) + 1

            total_games = confirmed_p1 + confirmed_p2
            data["users"][m_locked.get("p1")]["stats"]["sets_played"] = data["users"][m_locked.get("p1")]["stats"].get("sets_played", 0) + total_games
            data["users"][m_locked.get("p2")]["stats"]["sets_played"] = data["users"][m_locked.get("p2")]["stats"].get("sets_played", 0) + total_games

            data["users"][m_locked.get("p1")]["stats"]["sets_won"] = data["users"][m_locked.get("p1")]["stats"].get("sets_won", 0) + confirmed_p1
            data["users"][m_locked.get("p2")]["stats"]["sets_won"] = data["users"][m_locked.get("p2")]["stats"].get("sets_won", 0) + confirmed_p2

            if confirmed_p1 > confirmed_p2:
                data["users"][m_locked.get("p1")]["stats"]["wins"] = data["users"][m_locked.get("p1")]["stats"].get("wins", 0) + 1
                data["users"][m_locked.get("p2")]["stats"]["losses"] = data["users"][m_locked.get("p2")]["stats"].get("losses", 0) + 1
            elif confirmed_p2 > confirmed_p1:
                data["users"][m_locked.get("p2")]["stats"]["wins"] = data["users"][m_locked.get("p2")]["stats"].get("wins", 0) + 1
                data["users"][m_locked.get("p1")]["stats"]["losses"] = data["users"][m_locked.get("p1")]["stats"].get("losses", 0) + 1

            # points reflect match wins * 10 as before
            for pid in (m_locked.get("p1"), m_locked.get("p2")):
                data["users"][pid]["stats"]["points"] = 10 * data["users"][pid]["stats"].get("wins", 0)

            save_data(data)

            await ctx.reply(
                f"Result confirmed for match `{mid}` and recorded as {m_locked['score']}. Thanks — stats updated."
            )
            return
        else:
            # conflict -> flag for discussion and assign 'discussion' role to both players
            m_locked["needs_discussion"] = True
            m_locked["reported_by"] = list(reports_for_match.keys())
            save_data(data)

            # attempt to assign role to both players (best-effort)
            guild = ctx.guild
            role = discord.utils.get(guild.roles, name="discussion")
            assigned = []
            failed = []
            if role:
                for pid in (m_locked.get("p1"), m_locked.get("p2")):
                    try:
                        member = guild.get_member(int(pid))
                        if member:
                            await member.add_roles(role, reason=f"Discussion assigned by bot for match {mid}")
                            assigned.append(member.mention)
                        else:
                            failed.append(f"<@{pid}> (not in guild)")
                    except Exception as e:
                        failed.append(f"<@{pid}> (error: {e})")
            else:
                # role not found
                failed.append("role missing: 'discussion'")

            # notify channel and suggest staff action
            msg_lines = [
                f"Conflict detected for match `{mid}`: players reported different scores.",
                f"Reports: { {k: v['raw'] for k, v in reports_for_match.items()} }",
                "I flagged this match for discussion and asked staff to resolve it."
            ]
            if assigned:
                msg_lines.append("Assigned `discussion` role to: " + ", ".join(assigned))
            if failed:
                msg_lines.append("Could not assign role to: " + ", ".join(failed) + ". Please ensure the role exists and I have Manage Roles permission.")

            # ping organisers if you want (optional) — here we simply reply in channel
            await ctx.reply("\n".join(msg_lines))
            return






@bot.command(name="forcereport")
@commands.check(lambda ctx: is_mod(ctx.author))
async def forcereport(ctx, *args):
    """
    Robust forcereport:
      - preferred usage: !forcereport @player1 @player2 3-1
      - optional: !forcereport monday 2025-W33 @player1 @player2 3-1
      - fallback: if players were not mentioned, try to match names from the remaining text
    """
    import re

    data = load_data()
    week_reset_if_needed(data)

    # tokens and mentions
    raw = ctx.message.content or ""
    tokens = [t for t in raw.split()]
    # strip the command prefix token
    if tokens and tokens[0].startswith(bot.command_prefix):
        tokens = tokens[1:]

    # find the score token (prefer last match like "3-1")
    score_token = None
    for t in reversed(tokens):
        if re.match(r'^\d+-\d+$', t):
            score_token = t
            break
    if not score_token:
        await ctx.reply("Score not found. Use format like `3-1`.")
        return

    # list of explicit mentions (discord.Member objects)
    mentions = ctx.message.mentions or []
    guild = ctx.guild

    # Build a list of tokens excluding the score token and any explicit mention tokens
    remaining = []
    # remove explicit mention substrings (<@...>) and the score token
    for t in tokens:
        if t == score_token:
            continue
        # skip tokens that look like mention forms
        if re.match(r'^<@!?\d+>$', t):
            continue
        remaining.append(t)

    # Try to determine day/week if provided (first tokens in remaining may be day/week)
    day_l = None
    week_key = None

    def parse_week_token(tok):
        """Return normalized 'YYYY-W<week>' or None.
           Accepts: 'w32', 'W32', 'w32-2025', '32-2025', '2025-W32', '2025-w32', etc.
        """
        tok = tok.strip()
        # 1) full ISO-ish: 2025-W32 or 2025-w32
        m = re.match(r'^(?P<year>\d{4})-W(?P<week>\d{1,2})$', tok, re.IGNORECASE)
        if m:
            y = int(m.group('year')); w = int(m.group('week'))
            if 1 <= w <= 53:
                return f"{y}-W{w}"
            return None

        # 2) forms like 'w32', 'W32', '32', optionally with -YYYY: 'w32-2025' or '32-2025'
        m = re.match(r'^[wW]?(?P<week>\d{1,2})(?:-(?P<year>\d{4}))?$', tok)
        if m:
            w = int(m.group('week'))
            if not (1 <= w <= 53):
                return None
            year = int(m.group('year')) if m.group('year') else datetime.now(TIMEZONE).year
            return f"{year}-W{w}"

        return None

    if remaining:
        first = remaining[0].lower()
        # if first is a weekday keyword
        if first in VALID_DAYS:
            day_l = first
            # if there's a following token, try to parse it as week
            if len(remaining) >= 2:
                candidate = remaining[1]
                wk = parse_week_token(candidate)
                if wk:
                    week_key = wk
                    # consume day + week token
                    remaining = remaining[2:]
                else:
                    # consume only day token
                    remaining = remaining[1:]
        else:
            # first might itself be a week token
            wk = parse_week_token(remaining[0])
            if wk:
                week_key = wk
                remaining = remaining[1:]

    if not week_key:
        week_key = data.get("week_key")



    if not week_key:
        week_key = data.get("week_key")

    # If the message included at least two mentions, use them
    members = []
    if len(mentions) >= 2:
        members = [mentions[0], mentions[1]]
    else:
        # fallback: try to match remaining text to guild members' display_name or name
        # join remaining into a single string and try substring match (handles 'Name | Extra' cases)
        remaining_text = " ".join(remaining).strip()
        if remaining_text and guild:
            remaining_search = remaining_text.replace("|", " ").strip().lower()
            candidates = []
            for m in guild.members:
                dn = (m.display_name or "").lower()
                un = (m.name or "").lower()
                # try: exact match, substring, or token containment
                if remaining_search == dn or remaining_search == un:
                    candidates.append(m)
                elif remaining_search in dn or remaining_search in un:
                    candidates.append(m)
                else:
                    # also try splitting by '|' or ',' and match any piece
                    for piece in re.split(r'[|,]', remaining_search):
                        piece = piece.strip()
                        if piece and (piece in dn or piece in un):
                            candidates.append(m)
                            break
            # remove duplicates while preserving order
            seen = set(); uniq = []
            for c in candidates:
                if c.id not in seen:
                    uniq.append(c); seen.add(c.id)
            if len(uniq) >= 2:
                members = uniq[:2]
            elif len(uniq) == 1 and mentions:
                # combine one found member with one mention
                members = [mentions[0], uniq[0]]
            # else: not enough fallback matches
    if not members or len(members) < 2:
        await ctx.reply("Could not determine two players. Please mention both players (use Discord mention, or provide exact display names). Example:\n`!forcereport @player1 @player2 3-1`")
        return

    p1_member = members[0]; p2_member = members[1]
    p1 = str(p1_member.id); p2 = str(p2_member.id)

    # parse the score token we detected
    try:
        a_str, b_str = score_token.split("-")
        a = int(a_str); b = int(b_str)
    except Exception:
        await ctx.reply("Score format invalid. Use e.g. `3-1`.")
        return

    # validate with your best-of-5 validator (change function name if you used another)
    ok, reason = validate_score_best_of_5(a, b)
    if not ok:
        await ctx.reply(f"Invalid score: {reason} (moderators: use the command anyway if you must)")
        return

    # defaults: day/week if not already set
    if not day_l:
        today_day = datetime.now(TIMEZONE).strftime('%A').lower()
        if today_day in VALID_DAYS:
            day_l = today_day
        else:
            await ctx.reply("Day not provided and today is not a valid qualifier day (Mon–Fri). Please include the day (e.g. `monday`).")
            return

        # Create or overwrite forced, confirmed match and update stats (overwrite if same players/day/week)
        # Create or overwrite forced, confirmed match and deduplicate same-day duplicates
    def ensure_user_stats(pid):
        data.setdefault("users", {}).setdefault(pid, {})
        data["users"][pid].setdefault("stats", {"wins": 0, "losses": 0, "points": 0, "played": 0, "sets_played": 0, "sets_won": 0})

    def apply_match_stats(p1id, p2id, sa, sb):
        for pid in (p1id, p2id):
            ensure_user_stats(pid)
        data["users"][p1id]["stats"]["played"] = data["users"][p1id]["stats"].get("played", 0) + 1
        data["users"][p2id]["stats"]["played"] = data["users"][p2id]["stats"].get("played", 0) + 1

        data["users"][p1id]["stats"]["sets_played"] = data["users"][p1id]["stats"].get("sets_played", 0) + (sa + sb)
        data["users"][p2id]["stats"]["sets_played"] = data["users"][p2id]["stats"].get("sets_played", 0) + (sa + sb)

        data["users"][p1id]["stats"]["sets_won"] = data["users"][p1id]["stats"].get("sets_won", 0) + sa
        data["users"][p2id]["stats"]["sets_won"] = data["users"][p2id]["stats"].get("sets_won", 0) + sb

        if sa > sb:
            data["users"][p1id]["stats"]["wins"] = data["users"][p1id]["stats"].get("wins", 0) + 1
            data["users"][p2id]["stats"]["losses"] = data["users"][p2id]["stats"].get("losses", 0) + 1
        else:
            data["users"][p2id]["stats"]["wins"] = data["users"][p2id]["stats"].get("wins", 0) + 1
            data["users"][p1id]["stats"]["losses"] = data["users"][p1id]["stats"].get("losses", 0) + 1

        for pid in (p1id, p2id):
            data["users"][pid]["stats"]["points"] = data["users"][pid]["stats"].get("sets_won", 0)

    def _dec_field(pid, field, val):
        cur = data["users"][pid]["stats"].get(field, 0) - val
        data["users"][pid]["stats"][field] = cur if cur >= 0 else 0

    def undo_match_stats(m):
        """Reverse effects of stored match m (used when deduping/overwriting)."""
        if "score" not in m:
            return
        try:
            old_a, old_b = map(int, m["score"].split("-"))
        except Exception:
            return

        sp1 = m.get("p1"); sp2 = m.get("p2")
        if not (sp1 and sp2):
            return

        ensure_user_stats(sp1); ensure_user_stats(sp2)

        _dec_field(sp1, "played", 1)
        _dec_field(sp2, "played", 1)
        _dec_field(sp1, "sets_played", old_a + old_b)
        _dec_field(sp2, "sets_played", old_a + old_b)

        # sets_won: stored p1 -> old_a; stored p2 -> old_b
        _dec_field(sp1, "sets_won", old_a)
        _dec_field(sp2, "sets_won", old_b)

        if old_a > old_b:
            _dec_field(sp1, "wins", 1)
            _dec_field(sp2, "losses", 1)
        else:
            _dec_field(sp2, "wins", 1)
            _dec_field(sp1, "losses", 1)

        data["users"][sp1]["stats"]["points"] = data["users"][sp1]["stats"].get("sets_won", 0)
        data["users"][sp2]["stats"]["points"] = data["users"][sp2]["stats"].get("sets_won", 0)

    # collect all existing matches that match same day/week and the same player pair (order-independent)
    matches_map = data.get("matches", {})
    matching = []
    for mid, m in list(matches_map.items()):
        if m.get("day") == day_l and m.get("week") == week_key:
            if {m.get("p1"), m.get("p2")} == {p1, p2}:
                matching.append((mid, m))

    # If multiple matches found, undo stats for all and remove them (we'll create one canonical match)
    if matching:
        # sort by timestamp if present (oldest -> newest), fallback to insertion order (list order)
        def _ts_key(item):
            mid, m = item
            ts = m.get("timestamp")
            if ts:
                try:
                    # ISO string compare is fine for ordering here
                    return ts
                except Exception:
                    return ""
            return ""
        matching.sort(key=_ts_key)

        # undo stats for every existing match
        for mid, m in matching:
            undo_match_stats(m)
            # remove the old match entry
            if mid in data.get("matches", {}):
                del data["matches"][mid]

    # create a single canonical match id and store the new score
    mid = str(uuid.uuid4())[:8]
    match = {
        "id": mid,
        "p1": p1,
        "p2": p2,
        "day": day_l,
        "week": week_key,
        "reported_by": str(ctx.author.id),
        "score": f"{a}-{b}",
        "confirmed": True,
        "timestamp": datetime.now(TIMEZONE).isoformat(),
        "forced": True
    }
    data.setdefault("matches", {})[mid] = match

    # apply stats for the new (single) match
    apply_match_stats(p1, p2, a, b)

    save_data(data)
    await ctx.reply(f"Recorded (and deduplicated) match {mid} between <@{p1}> and <@{p2}> on {day_l.capitalize()} (week {week_key}) as {a}-{b} and updated stats.")






@bot.command(name="dedupe_and_recompute")
@commands.check(lambda ctx: is_mod(ctx.author))
async def dedupe_and_recompute_cmd(ctx):
    """
    Admin command: deduplicate matches (keep newest per week/day/pair) and rebuild stats.
    """
    import json, time
    data = load_data()

    # optional external file backup (recommended). Adjust the filename/path to your setup.
    try:
        with open("data_backup_before_dedupe.json", "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    except Exception as e:
        await ctx.reply(f"Could not write external backup file: {e}. Aborting.")
        return

    kept, removed_count, removed_mids = dedupe_matches_keep_latest(data)

    # recompute stats (uses your recompute_all_stats function)
    recompute_all_stats(data)
    save_data(data)

    msg = (f"Deduplication complete. Groups kept: {kept}. Matches removed: {removed_count}.\n"
           f"Removed IDs: {', '.join(removed_mids) if removed_mids else 'None'}.\n"
           "Recomputed stats and saved data. External backup: data_backup_before_dedupe.json")
    await ctx.reply(msg)



@bot.command(name="unmatched")
@commands.check(lambda ctx: is_mod(ctx.author))
async def unmatched(ctx, day: str, week_key: str = None):
    # List players who signed up but don't currently have a match scheduled, grouped by region
    data = load_data()
    week_reset_if_needed(data)
    day_l = day.lower()
    if day_l not in VALID_DAYS:
        await ctx.reply(f"Invalid day. Pick one of: {', '.join(VALID_DAYS)}")
        return
    if not week_key:
        week_key = data.get("week_key")
    ensure_signups_week(data, week_key)
    signed = data["signups"][week_key].get(day_l, [])
    matched = set()
    for mid, m in data.get("matches", {}).items():
        if m.get("week") == week_key and m.get("day") == day_l:
            matched.add(m.get("p1")); matched.add(m.get("p2"))
    unmatched_list = [u for u in signed if u not in matched]
    if not unmatched_list:
        await ctx.reply("No unmatched players for that day/week.")
        return

    by_region = {"EU": [], "USA": [], "UNKNOWN": []}
    for uid in unmatched_list:
        user = data.get("users", {}).get(uid)
        if not user:
            by_region["UNKNOWN"].append(uid)
            continue
        reg = user.get("region")
        if reg in ("EU", "USA"):
            by_region[reg].append(uid)
        else:
            by_region["UNKNOWN"].append(uid)

    lines = [f"Unmatched players for {day_l.capitalize()} (week {week_key}):"]
    for r in ("EU", "USA", "UNKNOWN"):
        lst = by_region[r]
        if lst:
            header = r if r != "UNKNOWN" else "Unregistered/Unknown"
            lines.append(f"- {header} ({len(lst)}): " + ", ".join([f"<@{u}>" for u in lst]))
    await ctx.reply("\n".join(lines))



@bot.command(name="deleteset")
@commands.check(lambda ctx: is_mod(ctx.author))
async def deleteset(ctx, mid: str):
    # Delete a match and revert stats if it was confirmed
    data = load_data()
    m = data.get("matches", {}).get(mid)
    if not m:
        await ctx.reply("Match not found.")
        return
    if m.get("confirmed"):
        p1 = m["p1"]; p2 = m["p2"]
        a,b = map(int, m["score"].split("-"))
        for pid in (p1,p2):
            data["users"][pid]["stats"]["played"] = max(0, data["users"][pid]["stats"].get("played",0)-1)
            data["users"][pid]["stats"]["sets_played"] = max(0, data["users"][pid]["stats"].get("sets_played",0)-(a+b))
        data["users"][p1]["stats"]["sets_won"] = max(0, data["users"][p1]["stats"].get("sets_won",0)-a)
        data["users"][p2]["stats"]["sets_won"] = max(0, data["users"][p2]["stats"].get("sets_won",0)-b)
        if a > b:
            data["users"][p1]["stats"]["wins"] = max(0, data["users"][p1]["stats"].get("wins",0)-1)
            data["users"][p2]["stats"]["losses"] = max(0, data["users"][p2]["stats"].get("losses",0)-1)
        elif b > a:
            data["users"][p2]["stats"]["wins"] = max(0, data["users"][p2]["stats"].get("wins",0)-1)
            data["users"][p1]["stats"]["losses"] = max(0, data["users"][p1]["stats"].get("losses",0)-1)
        for pid in (p1,p2):
            data["users"][pid]["stats"]["points"] = data["users"][pid]["stats"].get("sets_won", 0)
    del data["matches"][mid]
    save_data(data)
    await ctx.reply(f"Deleted match {mid}.")



@bot.command(name="forcesignup")
@commands.check(lambda ctx: is_mod(ctx.author))
async def forcesignup(ctx, day: str, week_key_or_player: str, player: discord.Member = None):
    """
    Mod-only:
    !forcesignup <day> <week_key> <@player>
    or
    !forcesignup <day> <@player>         (uses current week)
    Week key formats accepted: YYYY-Www, Www, ww (week number)
    """
    import re
    from datetime import datetime as _dt

    # normalize day
    day_l = (day or "").lower()
    if day_l not in VALID_DAYS:
        await ctx.reply(f"Invalid day. Pick one of: {', '.join(VALID_DAYS)}")
        return

    # load data & ensure week pointer is current
    data = load_data()
    week_reset_if_needed(data)

    # Helper to resolve a member from a mention/id/string
    async def _resolve_member(ctx_guild, s):
        if not s:
            return None
        m = re.match(r'^<@!?(\d+)>$', s.strip())
        if m:
            uid = int(m.group(1))
            # try to get Member from guild (preferred)
            member = ctx_guild.get_member(uid)
            if member:
                return member
            try:
                member = await ctx_guild.fetch_member(uid)
                return member
            except Exception:
                # last resort: try fetch user (not a Member)
                try:
                    return await bot.fetch_user(uid)
                except Exception:
                    return None
        # numeric id
        if s.isdigit():
            uid = int(s)
            member = ctx_guild.get_member(uid)
            if member:
                return member
            try:
                member = await ctx_guild.fetch_member(uid)
                return member
            except Exception:
                try:
                    return await bot.fetch_user(uid)
                except Exception:
                    return None
        # try to match by display name or username
        member = discord.utils.find(lambda mm: (mm.display_name == s or mm.name == s), ctx_guild.members)
        return member

    # Determine which arg is week and which is player
    if player is None:
        # week_key_or_player is probably the player; use current week_key
        raw_week_key = None
        player_candidate = week_key_or_player
        week_key = data.get("week_key") or current_week_key()
        member = await _resolve_member(ctx.guild, player_candidate)
    else:
        # explicit week and player passed
        raw_week_key = week_key_or_player.strip()
        member = player

        # normalize week_key input (accept YYYY-Wnn, Wnn, nn)
        if re.match(r'^\d{4}-W\d{1,2}$', raw_week_key):
            week_key = raw_week_key
        elif re.match(r'^W\d{1,2}$', raw_week_key, re.IGNORECASE):
            year = _dt.now(TIMEZONE).year
            week_key = f"{year}-{raw_week_key.upper()}"
        elif re.match(r'^\d{1,2}$', raw_week_key):
            year = _dt.now(TIMEZONE).year
            week_key = f"{year}-W{int(raw_week_key):02d}"
        else:
            # fallback to current week and inform
            week_key = data.get("week_key") or current_week_key()
            await ctx.reply("Week key format not recognized; using current week instead.")

    # final checks
    if member is None:
        await ctx.reply("Could not resolve the player. Mention them or pass their ID/username.")
        return

    # ensure signups structure exists for the target week
    ensure_signups_week(data, week_key)

    uid = str(getattr(member, "id", None) or getattr(member, "id", None))
    if not uid:
        await ctx.reply("Could not determine user id for that player.")
        return

    # don't double-signup
    if uid in data["signups"][week_key].get(day_l, []):
        await ctx.reply(f"{member.display_name} is already signed up for {day_l.capitalize()} (week {week_key}).")
        return

    # create user record if missing (best-effort placeholder)
    data.setdefault("users", {})
    if uid not in data["users"]:
        display_name = getattr(member, "display_name", None) or getattr(member, "name", None) or f"<@{uid}>"
        data["users"][uid] = {
            "name": display_name,
            "region": None,
            "registered_on": datetime.now(TIMEZONE).isoformat(),
            "stats": {"wins": 0, "losses": 0, "points": 0, "played": 0, "sets_played": 0, "sets_won": 0},
            "week_counts": {}
        }

    # increment week_counts (respecting existing counts)
    data["users"][uid].setdefault("week_counts", {})
    prev = data["users"][uid]["week_counts"].get(week_key, 0)
    data["users"][uid]["week_counts"][week_key] = prev + 1

    # add to signups
    data["signups"][week_key].setdefault(day_l, [])
    data["signups"][week_key][day_l].append(uid)

    save_data(data)

    await ctx.reply(f"Forced signup: {member.display_name} -> {day_l.capitalize()} (week {week_key}).")


@bot.command(name="table")
@commands.check(lambda ctx: is_organiser(ctx.author))
async def table(ctx, top: int = 100):
    """
    Generate a centered, bordered leaderboard as paginated images and send them.
    - top: how many players to show (default 50).
    Requires Pillow: pip install pillow
    Optional (for avatars): pip install aiohttp
    """
    try:
        from PIL import Image, ImageDraw, ImageFont, ImageFilter, ImageOps
    except Exception:
        await ctx.reply("Image leaderboard requires the Pillow library. Install it with `pip install pillow` and restart the bot to enable image generation. Falling back to text output.")
        # fallback unchanged except using Discord cache for username (cache-only)
        data = load_data()
        users = data.get("users", {})

        def sort_key(kv):
            u = kv[1]
            stats = u.get("stats", {})
            games_won = stats.get("sets_won", 0)
            games_played = stats.get("sets_played", 0)
            set_ratio = (games_won / games_played) if games_played > 0 else 0
            return (games_won, set_ratio, stats.get("wins", 0))

        leaderboard = sorted(users.items(), key=sort_key, reverse=True)[:top]
        lines = []
        for pos, (uid, u) in enumerate(leaderboard, start=1):
            # try cache-only Discord username first
            try:
                member = None
                try:
                    member = ctx.guild.get_member(int(uid))
                except Exception:
                    member = None
                if member:
                    name = getattr(member, "name", member.display_name)
                else:
                    name = u.get("name", "?")
            except Exception:
                name = u.get("name", "?")

            region = u.get("region", "")
            stats = u.get("stats", {})
            sets_played = stats.get("played", 0)
            set_wins = stats.get("wins", 0)
            games_played = stats.get("sets_played", 0)
            games_won = stats.get("sets_won", 0)
            pct = (games_won / games_played * 100) if games_played > 0 else 0.0
            pts = 10 * set_wins
            lines.append(f"{pos}. {name} ({region}) - Pts:{pts} SetsW:{set_wins} SetsP:{games_played} Games%:{pct:.1f}")
        if not lines:
            await ctx.reply("No players in leaderboard.")
        else:
            content = "Leaderboard:\n" + "\n".join(lines)
            if len(content) > 1900:
                path = "leaderboard.txt"
                with open(path, "w", encoding="utf-8") as f:
                    f.write(content)
                await ctx.reply(file=discord.File(path))
            else:
                await ctx.reply(content)
        return

    try:
        import aiohttp
        HAVE_AIOHTTP = True
    except Exception:
        HAVE_AIOHTTP = False

    from datetime import datetime
    import io
    import math
    import os
    import zipfile

    data = load_data()
    users = data.get("users", {})

    def sort_key(kv):
        u = kv[1]
        s = u.get("stats", {})
        games_won = s.get("sets_won", 0)
        games_played_local = s.get("sets_played", 0)
        set_ratio = (games_won / games_played_local) if games_played_local > 0 else 0
        return (games_won, set_ratio, s.get("wins", 0))

    # caps & pagination
    max_top = 500
    top = max(1, min(top, max_top))
    leaderboard = sorted(users.items(), key=sort_key, reverse=True)[:top]

    if not leaderboard:
        await ctx.reply("No players in leaderboard.")
        return

    # pagination settings
    page_size = 100          # rows per image (change if you want smaller pages)
    avatar_rows = 100         # fetch avatars for first N rows per page to avoid storms
    pages = [leaderboard[i:i+page_size] for i in range(0, len(leaderboard), page_size)]

    # if True, will call API to fetch missing members (can be slow / rate-limit risk)
    force_fetch_missing = False

    def load_font(size, bold=False):
        candidates = [
            ("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", False),
            ("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", True),
            ("/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf", False),
            ("/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf", True),
            ("C:\\Windows\\Fonts\\arial.ttf", False),
            ("C:\\Windows\\Fonts\\arialbd.ttf", True),
            ("/Library/Fonts/Arial.ttf", False),
            ("/Library/Fonts/Arial Bold.ttf", True),
        ]
        for p, is_b in candidates:
            try:
                if bold and is_b:
                    return ImageFont.truetype(p, size=size)
                if (not bold) and (not is_b):
                    return ImageFont.truetype(p, size=size)
            except Exception:
                continue
        return ImageFont.load_default()

    font_header = load_font(28, bold=True)
    font_col = load_font(16, bold=True)
    font_row = load_font(18)
    font_small = load_font(14)

    cols = [
        ("Rk", 0.05),
        ("", 0.08),         # avatar
        ("Player", 0.34),
        ("Reg", 0.06),
        ("SetsP", 0.06),
        ("SetW", 0.06),
        ("GamesP", 0.06),
        ("GamesW", 0.06),
        ("Set%", 0.10),
        ("Pts", 0.13),
    ]

    def measure_text(drawobj, text, fontobj):
        try:
            bbox = drawobj.textbbox((0, 0), text, font=fontobj)
            return (bbox[2] - bbox[0], bbox[3] - bbox[1])
        except Exception:
            try:
                return fontobj.getsize(text)
            except Exception:
                return (len(text) * 6, getattr(fontobj, "size", 12))

    def draw_truncated_text(drawobj, text, x, y, fontobj, max_width, fill=(0,0,0)):
        if not text:
            return
        w, h = measure_text(drawobj, text, fontobj)
        if w <= max_width:
            drawobj.text((x,y), text, font=fontobj, fill=fill)
            return
        ell = "…"
        lo, hi = 0, len(text)
        while lo < hi:
            mid = (lo + hi) // 2
            tst = text[:mid] + ell
            tw, _ = measure_text(drawobj, tst, font=fontobj)
            if tw <= max_width:
                lo = mid + 1
            else:
                hi = mid
        final = text[:max(0, lo-1)] + ell
        drawobj.text((x,y), final, font=fontobj, fill=fill)

    async def get_avatar_image(uid, size):
        if not HAVE_AIOHTTP:
            return None
        try:
            member = None
            try:
                member = ctx.guild.get_member(int(uid))
                if member is None and force_fetch_missing:
                    member = await ctx.guild.fetch_member(int(uid))
            except Exception:
                member = None
            if not member:
                return None
            avatar_url = None
            try:
                avatar_url = member.display_avatar.url
            except Exception:
                try:
                    avatar_url = member.avatar_url
                except Exception:
                    avatar_url = None
            if not avatar_url:
                return None
            async with aiohttp.ClientSession() as session:
                async with session.get(str(avatar_url)) as resp:
                    if resp.status != 200:
                        return None
                    data = await resp.read()
                    av = Image.open(io.BytesIO(data)).convert("RGBA")
                    av = ImageOps.fit(av, (size, size), centering=(0.5,0.5))
                    mask = Image.new("L", (size, size), 0)
                    md = ImageDraw.Draw(mask)
                    md.ellipse((0,0,size,size), fill=255)
                    out = Image.new("RGBA", (size,size), (0,0,0,0))
                    out.paste(av, (0,0), mask=mask)
                    return out
        except Exception:
            return None

    saved_files = []
    zip_name = None
    for page_index, page in enumerate(pages, start=1):
        image_width = 1400
        row_height = 62
        header_height = 110
        padding = 26
        rows = len(page)
        image_height = padding*2 + header_height + rows * row_height + 30

        col_px = [int(image_width * frac) for (_, frac) in cols]
        diff = image_width - sum(col_px)
        if diff != 0:
            col_px[-1] += diff

        inner_margin = 10
        col_bounds = []
        cur_x = padding
        separators_x = []
        for w in col_px:
            left = cur_x + inner_margin
            right = cur_x + w - inner_margin
            col_bounds.append((left, right))
            separators_x.append(cur_x + w)
            cur_x += w

        table_top = padding + header_height + 6
        table_bottom = table_top + rows * row_height - 6

        img = Image.new("RGB", (image_width, image_height), color=(248,249,250))
        draw = ImageDraw.Draw(img)

        header_box = (padding, padding, image_width - padding, padding + header_height)
        shadow = Image.new("RGBA", img.size, (0,0,0,0))
        sd = ImageDraw.Draw(shadow)
        sd.rounded_rectangle([header_box[0]+4, header_box[1]+6, header_box[2]+4, header_box[3]+6], radius=12, fill=(0,0,0,60))
        img = Image.alpha_composite(img.convert("RGBA"), shadow).convert("RGB")
        draw = ImageDraw.Draw(img)

        header_bg = (34, 43, 69)
        draw.rounded_rectangle(header_box, radius=12, fill=header_bg)
        title = "Leaderboard"
        subtitle = f"Top {len(leaderboard)} — Page {page_index}/{len(pages)} — Generated {datetime.now().astimezone().strftime('%Y-%m-%d %H:%M:%S %Z')}"
        draw.text((padding + 18, padding + 18), title, font=font_header, fill=(255,255,255))
        draw.text((padding + 18, padding + 18 + 36), subtitle, font=font_small, fill=(210,210,235))

        col_header_y = padding + header_height - 36
        for i, (name_col, _) in enumerate(cols):
            if not name_col:
                continue
            left, right = col_bounds[i]
            maxw = right - left
            w, h = measure_text(draw, name_col, font_col)
            draw.text((left + (maxw - w)/2, col_header_y), name_col, font=font_col, fill=(220,220,230))

        sep_y = padding + header_height + 6
        draw.line([(padding, sep_y), (image_width - padding, sep_y)], fill=(220,220,230))

        sep_color = (220,220,230)
        for sx in separators_x[:-1]:
            draw.line([(sx - inner_margin/2, table_top - 6), (sx - inner_margin/2, table_bottom + 6)], fill=sep_color)

        table_left = padding
        table_right = image_width - padding
        table_rect = [table_left, table_top - 6, table_right, table_bottom + 6]
        draw.rounded_rectangle(table_rect, radius=8, outline=(200,200,205), width=1)

        y0 = padding + header_height + 12
        # loop rows for this page
        for idx, (uid, u) in enumerate(page, start=(1 + (page_index-1)*page_size)):
            row_index = idx - (page_index-1)*page_size
            row_y_top = y0 + (row_index-1) * row_height
            row_y_text = row_y_top + 14

            if idx == 1:
                row_bg = (255, 248, 230)
            elif idx == 2:
                row_bg = (245, 247, 252)
            elif idx == 3:
                row_bg = (250, 245, 245)
            else:
                row_bg = (255,255,255) if row_index % 2 == 1 else (249,250,252)

            draw.rectangle([padding, row_y_top, image_width - padding, row_y_top + row_height - 6], fill=row_bg)
            h_sep_y = row_y_top + row_height - 6
            draw.line([(padding + 2, h_sep_y), (image_width - padding - 2, h_sep_y)], fill=(236,236,240))

            # get discord username (cache-first). optionally fetch if allowed.
            try:
                member = None
                try:
                    member = ctx.guild.get_member(int(uid))
                except Exception:
                    member = None
                if member is None and force_fetch_missing:
                    try:
                        member = await ctx.guild.fetch_member(int(uid))
                    except Exception:
                        member = None
                if member:
                    name = getattr(member, "name", member.display_name)
                else:
                    name = u.get("name", "?")
            except Exception:
                name = u.get("name", "?")

            region = u.get("region", "")
            stats = u.get("stats", {})
            sets_played = stats.get("played", 0)
            set_wins = stats.get("wins", 0)
            games_played = stats.get("sets_played", 0)
            games_won = stats.get("sets_won", 0)
            set_pct = (games_won / games_played * 100) if games_played > 0 else 0.0
            pct_text = f"{set_pct:.1f}%"
            pts = 10 * set_wins

            col_values = [
                str(idx),
                None,
                name,
                region or "",
                str(sets_played),
                str(set_wins),
                str(games_played),
                str(games_won),
                pct_text,
                str(pts)
            ]

            # avatar centered inside its column
            avatar_col_index = 1
            left, right = col_bounds[avatar_col_index]
            avatar_w = min(48, right - left - 8)
            av_img = None
            if row_index <= avatar_rows:
                try:
                    av_img = await get_avatar_image(uid, avatar_w)
                except Exception:
                    av_img = None
            if av_img:
                av_x = left + ((right - left) - av_img.width) // 2
                av_y = row_y_top + ((row_height - av_img.height) // 2)
                border = 3
                bx = av_x - border
                by = av_y - border
                draw.ellipse([bx, by, bx + av_img.width + border*2, by + av_img.height + border*2], fill=(255,255,255))
                img.paste(av_img, (av_x, av_y), av_img)
            else:
                ph_size = min(44, avatar_w)
                cx = left + ((right - left) - ph_size) // 2
                cy = row_y_top + ((row_height - ph_size) // 2)
                draw.ellipse([cx, cy, cx + ph_size, cy + ph_size], fill=(230,230,235))
                initials = "".join([part[0].upper() for part in name.split()[:2] if part])[:2]
                iw, ih = measure_text(draw, initials or "?", font_row)
                draw.text((cx + (ph_size - iw) / 2, cy + (ph_size - ih)/2 - 2), initials or "?", font=font_row, fill=(70,70,80))

            # DRAW: center every value inside its column
            for i, text in enumerate(col_values):
                if i == avatar_col_index:
                    continue
                left, right = col_bounds[i]
                maxw = right - left
                if text is None:
                    continue
                tw, th = measure_text(draw, text, font_row)
                tx = left + max(0, (maxw - tw) // 2)
                draw.text((tx, row_y_text), text, font=font_row, fill=(30,30,40))

            # medals centered in rank column
            if idx == 1:
                medal = "🥇"
            elif idx == 2:
                medal = "🥈"
            elif idx == 3:
                medal = "🥉"
            else:
                medal = None
            if medal:
                left, right = col_bounds[0]
                maxw = right - left
                mw, mh = measure_text(draw, medal, font_row)
                draw.text((left + (maxw - mw)/2, row_y_text - 2), medal, font=font_row, fill=(30,30,30))

        footer = f"Generated: {datetime.now().astimezone().strftime('%Y-%m-%d %H:%M:%S %Z')}"
        draw.text((padding, image_height - padding - 18), footer, font=font_small, fill=(120,120,130))

        out_path = f"leaderboard_page_{page_index}.png"
        try:
            img.convert("RGB").save(out_path, format="PNG", optimize=True)
            saved_files.append(out_path)
        except Exception as e:
            await ctx.reply(f"Failed to save leaderboard image: {e}")
            return

    try:
        if len(saved_files) == 1:
            await ctx.reply(file=discord.File(saved_files[0]))
        else:
            zip_name = "leaderboard_pages.zip"
            with zipfile.ZipFile(zip_name, "w", zipfile.ZIP_DEFLATED) as zf:
                for f in saved_files:
                    zf.write(f)
            await ctx.reply(file=discord.File(zip_name))
    except Exception as e:
        await ctx.reply(f"Failed to upload leaderboard files: {e}")
    finally:
        for f in saved_files:
            try: os.remove(f)
            except: pass
        if zip_name:
            try: os.remove(zip_name)
            except: pass











@bot.command(name="exporttable")
@commands.check(lambda ctx: is_mod(ctx.author))
async def exporttable(ctx):
    # Export the leaderboard as CSV and upload it to the channel
    data = load_data()
    users = data.get("users", {})
    csv_path = "standings_export.csv"
    with open(csv_path, "w", newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(["Position","UserID","Name","Region","Games Played","Games Won","Sets Played","Sets Won","Win Ratio","Points"])
        leaderboard = sorted(users.items(), key=lambda kv: (
            kv[1].get("stats",{}).get("sets_won",0),
            (kv[1].get("stats",{}).get("wins",0) / kv[1].get("stats",{}).get("played",1)) if kv[1].get("stats",{}).get("played",0) > 0 else 0,
            kv[1].get("stats",{}).get("wins",0)
        ), reverse=True)
        for pos, (uid,u) in enumerate(leaderboard, start=1):
            stats = u.get("stats", {})
            played = stats.get("played", 0)
            wins = stats.get("wins", 0)
            sets_played = stats.get("sets_played", 0)
            sets_won = stats.get("sets_won", 0)
            win_ratio = (wins / played * 100) if played > 0 else 0.0
            pts = sets_won
            writer.writerow([pos, uid, u.get("name",""), u.get("region",""), played, wins, sets_played, sets_won, f"{win_ratio:.2f}%", pts])
    await ctx.reply(file=discord.File(csv_path))


@bot.command(name="recompute_stats")
@commands.check(lambda ctx: is_mod(ctx.author))
async def recompute_stats_cmd(ctx):
    """Admin-only: rebuild all user stats from recorded match scores."""
    data = load_data()
    # optional: keep a quick backup inside the program (still make a file backup externally)
    try:
        data_backup = json.dumps(data)  # only keeps in-memory copy
    except Exception:
        data_backup = None

    recompute_all_stats(data)
    save_data(data)
    await ctx.reply("Recomputed all user stats from matches and saved the data. Make sure you have an external backup.")


@bot.command(name="warn")
@commands.check(lambda ctx: is_mod(ctx.author))
async def warn(ctx, member: discord.Member, *, reason: str = ""):
    # Send a DM to the user informing them they were warned by staff
    try:
        await member.send(f"You have been warned by staff: {reason}")
        await ctx.reply(f"Warned {member.display_name}.")
    except Exception:
        await ctx.reply("Could not DM that user.")



@bot.command(name="changename")
@commands.check(lambda ctx: is_mod(ctx.author))
async def changename(ctx, *, new_name: str):
    """Change your name in the leaderboard (available to everyone)."""
    data = load_data()
    users = data.get("users", {})

    user_id = str(ctx.author.id)

    if user_id not in users:
        await ctx.reply("You are not registered in the leaderboard.")
        return

    # Update name
    old_name = users[user_id].get("name", "?")
    users[user_id]["name"] = new_name

    save_data(data)  # Make sure this function exists in your code

    await ctx.reply(f"✅ Name changed from **{old_name}** to **{new_name}**.")


    # clear signups and decrement week_counts
    signed = list(data["signups"].get(week_key, {}).get(day_l, []))
    data["signups"].setdefault(week_key, {})[day_l] = []
    for uid in signed:
        u = data["users"].get(uid)
        if u:
            wcounts = u.setdefault("week_counts", {})
            wcounts[week_key] = max(0, wcounts.get(week_key, 0) - 1)

    # delete matches for that day/week
    to_delete = []
    for mid, m in list(data.get("matches", {}).items()):
        if m.get("week") == week_key and m.get("day") == day_l:
            to_delete.append(mid)
    for mid in to_delete:
        del data["matches"][mid]

    save_data(data)

    # attempt to notify in channel and DM participants
    mentions = ", ".join([f"<@{u}>" for u in signed]) if signed else "No signups to notify."
    msg_lines = [f"Qualifiers for {day_l.capitalize()} (week {week_key}) have been cancelled by <@{ctx.author.id}>."]
    if reason:
        msg_lines.append(f"Reason: {reason}")
    msg_lines.append("Affected players: " + mentions)
    await ctx.reply("\n".join(msg_lines))

    # post to AUTO_POST_CHANNEL_ID as well (if configured)
    if AUTO_POST_CHANNEL_ID is not None:
        channel = bot.get_channel(AUTO_POST_CHANNEL_ID)
        if not channel:
            try:
                channel = await bot.fetch_channel(AUTO_POST_CHANNEL_ID)
            except Exception:
                channel = None
        if channel:
            try:
                await channel.send("\n".join(msg_lines))
            except Exception:
                pass

    # try DMing users (best-effort)
    for uid in signed:
        try:
            user = await bot.fetch_user(int(uid))
            await user.send(f"Qualifiers for {day_l.capitalize()} (week {week_key}) were cancelled. Reason: {reason or 'No reason provided.'}")
        except Exception:
            pass


@bot.command(name="help")
async def help_cmd(ctx):
    """!help - show this message"""
    lines = [
        "!help - show this message",
        "!register <nickname> <EU/USA> - register for the league (region required)",
        "!signup [day] - sign up for a qualifier day (monday-friday). Signup for a target day closes at 16:00 CEST on that day.",
        "!cancelsignup <day> - cancel your signup.",
        "!mysignups - show your signups for the upcoming weeks",
        "!report <score> - report your match score (example: `!report 3-1`). The bot will match it to your opponent automatically.",
        "!myscores - shows your current scores.",
    ]
    await ctx.send("\n".join(lines))

@bot.command(name="helpadmin")
@commands.check(lambda ctx: is_mod(ctx.author))
async def helpadmin_cmd(ctx):
    """!helpadmin - show admin/moderator commands"""
    lines = [
        "!helpadmin - show this admin help message",
        "!forcereport <player1> <player2> <score> - force a reported result into the system",
        "!forcesignup <day> <week_key> <@player> - force-signup a player for a day/week",
        "!forceregister [@user] <name> <region-USA or EU>"
        "!autopair - run automated pairing for the current signups (if implemented)",
        "!deleteset <match_id> - delete a single match/set from records",
        "!unmatched <day>- list unmatched signups awaiting pairing",
        "!warn <@user> <reason> - issue a warning to a player",
        "!cancelsignupadmin <day> <week_key> [@user] - force cancels the signup of someone(admin version).",
        "!deleteregister <@user> - remove a player's registration",
        "!signuplist [day] [Week]- show the signup list for a given day",        
        "!allsignups - list all signups",
        "!pastsignups - show past signups",
        "!table - show standings (admin view)",
        "!exporttable - export standings as CSV/JSON",
        "!allscores - show all recorded scores",
        "!scores <player> - show scores for a specific player",
        "!testdm [@user] - test whether the bot can DM the specified user (or you if omitted)",
        "!dedupe_and_recompute - deduplicate duplicate matches and recompute all stats",
        "!recompute_stats - rebuild all player stats from recorded matches",
        "!unplayed <day> <week_key> - shows matched which don't have a result yet",
        "!signups [@user] - shows the signups of the respective user",
        "The format for <Day> <Week_Key> <@user> is ,,thursday 2025-W33 @shutter"
    ]
    await ctx.send("\n".join(lines))



@bot.command(name="deleteregister")
@commands.check(lambda ctx: is_mod(ctx.author))
async def delete_register(ctx, target: str):
    """Delete a registered user from the league (mods/organisers only).
    Usage: !deleteregister @User  OR  !deleteregister 123456789012345678  OR !deleteregister username
    """
    # Resolve target to a user id string
    uid = None
    # mention formats: <@123...> or <@!123...>
    if target.startswith("<@") and target.endswith(">"):
        inner = target.strip("<@!>")
        if inner.isdigit():
            uid = inner
    elif target.isdigit():
        uid = target
    else:
        # fallback: try to find guild member by name or display name
        if ctx.guild:
            m = discord.utils.find(lambda mm: mm.name == target or mm.display_name == target, ctx.guild.members)
            if m:
                uid = str(m.id)

    if not uid:
        await ctx.reply("Could not resolve target. Use an @mention or a numeric user ID or exact username/display name.")
        return

    with IO_LOCK:
        data = load_data()
        removed_any = False
        removed_from_signups = False
        removed_matches = []

        # 1) Remove from users (registered list)
        if uid in data.get("users", {}):
            del data["users"][uid]
            removed_any = True

        # 2) Remove from all signups and adjust week_counts
        for week_key, days in data.get("signups", {}).items():
            for day_name, lst in days.items():
                if uid in lst:
                    lst.remove(uid)
                    removed_any = True
                    removed_from_signups = True
                    # decrement week_counts if present
                    u = data.get("users", {}).get(uid)
                    if u:
                        wcounts = u.setdefault("week_counts", {})
                        wcounts[week_key] = max(0, wcounts.get(week_key, 0) - 1)

        # 3) Remove from suggestions (pairs)
        # suggestions stored under keys like "YYYY-WN:day" as list of pairs (a,b)
        for key, pairs in list(data.get("suggestions", {}).items()):
            new_pairs = [p for p in pairs if not (str(p[0]) == uid or str(p[1]) == uid)]
            if len(new_pairs) != len(pairs):
                data["suggestions"][key] = new_pairs
                removed_any = True

        # 4) Remove all matches involving user. If match was confirmed, revert stats similar to deleteset.
        for mid, m in list(data.get("matches", {}).items()):
            if m.get("p1") == uid or m.get("p2") == uid:
                # if confirmed, revert stats first
                if m.get("confirmed"):
                    try:
                        p1 = m["p1"]; p2 = m["p2"]
                        a,b = map(int, m["score"].split("-"))
                        # decrement played and sets_played
                        for pid in (p1, p2):
                            if pid in data.get("users", {}):
                                data["users"][pid]["stats"]["played"] = max(0, data["users"][pid]["stats"].get("played",0)-1)
                                data["users"][pid]["stats"]["sets_played"] = max(0, data["users"][pid]["stats"].get("sets_played",0)-(a+b))
                        # revert sets_won
                        if p1 in data.get("users", {}):
                            data["users"][p1]["stats"]["sets_won"] = max(0, data["users"][p1]["stats"].get("sets_won",0)-a)
                        if p2 in data.get("users", {}):
                            data["users"][p2]["stats"]["sets_won"] = max(0, data["users"][p2]["stats"].get("sets_won",0)-b)
                        # revert wins/losses
                        if a > b:
                            if p1 in data.get("users", {}):
                                data["users"][p1]["stats"]["wins"] = max(0, data["users"][p1]["stats"].get("wins",0)-1)
                            if p2 in data.get("users", {}):
                                data["users"][p2]["stats"]["losses"] = max(0, data["users"][p2]["stats"].get("losses",0)-1)
                        elif b > a:
                            if p2 in data.get("users", {}):
                                data["users"][p2]["stats"]["wins"] = max(0, data["users"][p2]["stats"].get("wins",0)-1)
                            if p1 in data.get("users", {}):
                                data["users"][p1]["stats"]["losses"] = max(0, data["users"][p1]["stats"].get("losses",0)-1)
                        # update points = sets_won
                        for pid in (p1, p2):
                            if pid in data.get("users", {}):
                                data["users"][pid]["stats"]["points"] = data["users"][pid]["stats"].get("sets_won", 0)
                    except Exception:
                        # if anything odd in score format, just proceed to delete match
                        pass
                # delete match
                del data["matches"][mid]
                removed_matches.append(mid)
                removed_any = True

        save_data(data)

    # report result
    if not removed_any:
        await ctx.reply(f"⚠️ No registration/signups/matches found for <@{uid}>.")
        return

    msg = [f"✅ Removed registration for <@{uid}>."]
    if removed_from_signups:
        msg.append("They were removed from signups.")
    if removed_matches:
        msg.append(f"Removed matches: {', '.join(removed_matches)}")
    await ctx.reply("\n".join(msg))


# ----------------- Score commands (English) -----------------


@bot.command(name="forceregister")
async def forceregister(ctx, member: discord.Member = None, name: str = None, region: str = None):
    """
    !forceregister <@member> [name] [region]
    Admin/mod-only: register (or update) a user on behalf of someone else.

    - If name is omitted, the member's display name will be used.
    - If the member is not yet registered, region is required (EU or USA).
    - If the member is already registered and no name/region are provided, command will notify and do nothing.
    """
    # permission check (uses your existing helper)
    if not is_mod(ctx.author):
        await ctx.reply("You don't have permission to use this command.")
        return

    if member is None:
        await ctx.reply("Usage: `!forceregister <@member> [name] [region]`")
        return

    data = load_data()
    users = data.setdefault("users", {})
    uid = str(member.id)

    # default name to display name if not provided
    final_name = name if name else getattr(member, "display_name", None)

    # handle new registration
    if uid not in users:
        if not region:
            await ctx.reply("Region is required when registering a new user. Use `EU` or `USA`.")
            return
        reg = normalize_region(region)
        if not reg:
            await ctx.reply("Region invalid. Use `EU` or `USA` (or `NA` -> `USA`).")
            return

        users[uid] = {
            "name": final_name,
            "region": reg,
            "registered_on": datetime.now(TIMEZONE).isoformat(),
            "stats": {"wins": 0, "losses": 0, "points": 0, "played": 0, "sets_played": 0, "sets_won": 0},
            "week_counts": {}
        }
        save_data(data)
        await ctx.reply(f"Force-registered {member.mention} as **{final_name}** ({reg}).")
        return

    # user already exists -> update only the provided fields
    changed = []
    if name:
        users[uid]["name"] = final_name
        changed.append(f"name -> {final_name}")
    if region:
        reg = normalize_region(region)
        if not reg:
            await ctx.reply("Region invalid. Use `EU` or `USA` (or `NA` -> `USA`).")
            return
        users[uid]["region"] = reg
        changed.append(f"region -> {reg}")

    if changed:
        save_data(data)
        await ctx.reply(f"Updated {member.mention}: " + ", ".join(changed))
    else:
        await ctx.reply(f"{member.mention} is already registered as **{users[uid].get('name')}** ({users[uid].get('region')}). "
                        "Provide `name` and/or `region` to update their registration.")


@bot.command(name="myscores")
async def myscores(ctx):
    """!myscores - show your sets & games stats + list of sets (recorded scores) and rank.
       Terminology: 'matches' -> 'sets' and 'sets' -> 'games'. Points = 10 * set_wins.
    """
    data = load_data()
    uid = str(ctx.author.id)
    if uid not in data.get("users", {}):
        await ctx.reply("You are not registered in the leaderboard.")
        return

    # --- stats header (changed naming) ---
    stats = data["users"][uid].get("stats", {})
    # 'sets' are the old 'matches' fields:
    sets_played = stats.get("played", 0)
    set_wins = stats.get("wins", 0)
    set_losses = stats.get("losses", 0)
    # 'games' are the old 'sets' fields:
    games_played = stats.get("sets_played", 0)
    games_won = stats.get("sets_won", 0)

    pct = (games_won / games_played * 100) if games_played > 0 else 0.0
    pts = 10 * set_wins

    # --- compute rank using same sort logic as table() but using games_*
    users = data.get("users", {})
    def sort_key(kv):
        u = kv[1]
        s = u.get("stats", {})
        g_won = s.get("sets_won", 0)           # games_won
        g_played_local = s.get("sets_played", 0)  # games_played
        set_ratio = (g_won / g_played_local) if g_played_local > 0 else 0
        return (g_won, set_ratio, s.get("wins", 0))

    leaderboard = sorted(users.items(), key=sort_key, reverse=True)
    total_players = len(leaderboard)
    rank = None
    for pos, (user_id, _) in enumerate(leaderboard, start=1):
        if user_id == uid:
            rank = pos
            break
    if rank is None:
        rank = total_players  # fallback

    # prefer guild display_name if possible
    try:
        gm = ctx.guild.get_member(int(uid)) if ctx.guild else None
        gm_name = gm.display_name if (gm and getattr(gm, "display_name", None)) else None
    except Exception:
        gm_name = None
    display_name = gm_name or data['users'][uid].get('name') or f"<@{uid}>"

    lines = [
        f"Scores for {display_name} (<@{uid}> / {data['users'][uid].get('name','?')}):",
        f"- Sets played: {sets_played}",
        f"- Set wins: {set_wins}",
        f"- Set losses: {set_losses}",
        f"- Games played: {games_played}",
        f"- Games won: {games_won}",
        f"- Points: {pts}",
        f"- Win%: {pct:.1f}%",
        f"- Rank: {rank} / {total_players}",
        "",
        "Sets (only those with recorded scores):"
    ]

    # --- helpers (same as before) ---
    def _parse_week_key(wk):
        try:
            y, w = wk.split("-W")
            return int(y), int(w)
        except Exception:
            return None

    def _date_for_weekday(wk, day):
        p = _parse_week_key(wk)
        if not p or not day:
            return None
        year, weeknum = p
        try:
            wd_index = VALID_DAYS.index(day.lower()) + 1  # 1..7
            return date.fromisocalendar(year, weeknum, wd_index)
        except Exception:
            return None

    # collect sets (previously 'matches') involving this user that have recorded scores
    matches_for_user = [(mid, m) for mid, m in data.get("matches", {}).items()
                        if m.get("score") and (m.get("p1") == uid or m.get("p2") == uid)]

    if not matches_for_user:
        lines.append("No sets with recorded scores yet for this user.")
        await ctx.reply("\n".join(lines))
        return

    # collapse duplicates silently by grouping on (week, day, players_set, score)
    groups = {}
    for mid, m in matches_for_user:
        wk = m.get("week"); day = m.get("day"); score = m.get("score")
        p1 = m.get("p1"); p2 = m.get("p2")
        if wk and day and p1 and p2 and score:
            key = (wk, day, frozenset((p1, p2)), score)
        else:
            key = ("__UNIQUE__", mid)
        groups.setdefault(key, []).append((mid, m))

    # pick representative per group (earliest timestamp if present)
    reps = []
    for key, lst in groups.items():
        def _ts_key(item):
            _, mm = item
            ts = mm.get("timestamp") or ""
            try:
                return datetime.fromisoformat(ts)
            except Exception:
                return ts
        lst_sorted = sorted(lst, key=_ts_key)
        rep_mid, rep_m = lst_sorted[0]
        reps.append((rep_mid, rep_m))

    # chronological sort key: prefer week+day -> date, else ISO timestamp, else week string
    def _chron_key(item):
        _, mm = item
        match_date = _date_for_weekday(mm.get("week"), mm.get("day"))
        if match_date:
            return (0, match_date)
        ts = mm.get("timestamp") or ""
        try:
            dt = datetime.fromisoformat(ts)
            return (1, dt)
        except Exception:
            wk = mm.get("week") or ""
            return (2, wk)

    reps.sort(key=_chron_key)

    # format each representative set line (same style as !scores)
    for rep_mid, rep_m in reps:
        p1 = rep_m.get("p1"); p2 = rep_m.get("p2")

        def disp_and_mention(pid):
            user_data = data.get("users", {}).get(pid, {})
            name = user_data.get("name") or user_data.get("display_name")
            try:
                gm = ctx.guild.get_member(int(pid)) if ctx.guild else None
                gm_name = gm.display_name if (gm and getattr(gm, "display_name", None)) else None
            except Exception:
                gm_name = None
            display = name or gm_name or f"<@{pid}>"
            mention = f"<@{pid}>"
            return f"{display} ({mention})"

        p1_str = disp_and_mention(p1)
        p2_str = disp_and_mention(p2)

        weekday = (rep_m.get("day") or "?").capitalize()

        match_date = _date_for_weekday(rep_m.get("week"), rep_m.get("day"))
        if match_date:
            weeknum = rep_m.get("week").split("-W")[-1] if rep_m.get("week") else "?"
            date_s = match_date.strftime("%d.%m.%Y")
            date_part = f"W{weeknum} {date_s}"
        else:
            date_part = rep_m.get("week") or (rep_m.get("timestamp") or "unknown")

        status = "confirmed" if rep_m.get("confirmed") else ("forced" if rep_m.get("forced") else "reported")
        score = rep_m.get("score", "?:?")
        lines.append(f"[{rep_mid}] {weekday} {date_part} {p1_str} vs {p2_str} — {score}  [{status}]")

    # reply (file fallback if long)
    content = "\n".join(lines)
    if len(content) > 1900:
        path = f"scores_{uid}.txt"
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)
        await ctx.reply(file=discord.File(path))
    else:
        await ctx.reply(content)




@bot.command(name="allscores")
@commands.check(lambda ctx: is_mod(ctx.author))
async def allscores(ctx, day: str = None, week_key: str = None):
    """
    !allscores [day] [week_key]
    If week_key omitted, shows all weeks found in matches. Day (mon..fri) filters across weeks.
    Output example per match:
    [6a9d3f86] fckyouspawn vs fluid  => 3-1  [confirmed] (W33 · 14.08.2025 · Thursday) ×2
    """
    data = load_data()
    week_reset_if_needed(data)

    # local helpers (kept small and self-contained)
    def _parse_week_key(wk):
        try:
            y, w = wk.split("-W")
            return int(y), int(w)
        except Exception:
            return None

    def _date_for_weekday(wk, day):
        p = _parse_week_key(wk)
        if not p or not day:
            return None
        year, weeknum = p
        try:
            wd_index = VALID_DAYS.index(day.lower()) + 1  # 1..7
            return date.fromisocalendar(year, weeknum, wd_index)
        except Exception:
            return None

    def _group_matches_for_display(matches_iterable):
        """
        Collapse duplicates: group by (week, day, players_set, score).
        Returns list of tuples (rep_mid, rep_m, count) sorted by rep timestamp.
        """
        groups = {}
        for mid, m in matches_iterable:
            if not m.get("score"):
                continue
            wk = m.get("week"); daym = m.get("day"); score = m.get("score")
            p1 = m.get("p1"); p2 = m.get("p2")
            if wk and daym and p1 and p2 and score:
                key = (wk, daym, frozenset((p1, p2)), score)
            else:
                key = ("__UNIQUE__", mid)
            groups.setdefault(key, []).append((mid, m))

        reps = []
        for key, lst in groups.items():
            def _ts_key(item):
                _, mm = item
                ts = mm.get("timestamp") or ""
                try:
                    return datetime.fromisoformat(ts)
                except Exception:
                    return ts
            lst_sorted = sorted(lst, key=_ts_key)
            rep_mid, rep_m = lst_sorted[0]
            reps.append((rep_mid, rep_m, len(lst_sorted)))
        # sort reps by timestamp (stable)
        def _rep_sort_key(t):
            _, mm, _ = t
            ts = mm.get("timestamp") or ""
            try:
                return datetime.fromisoformat(ts)
            except Exception:
                return ts
        reps_sorted = sorted(reps, key=_rep_sort_key)
        return reps_sorted

    # validate day filter
    day_l = None
    if day:
        day_l = day.lower()
        if day_l not in VALID_DAYS:
            await ctx.reply(f"Invalid day. Pick one of: {', '.join(VALID_DAYS)}")
            return

    # collect week_keys from matches if week_key not provided
    all_weeks = {m.get("week") for m in data.get("matches", {}).values() if m.get("week")}
    if week_key:
        weeks_to_show = [week_key]
    else:
        parsed = []
        for wk in all_weeks:
            p = _parse_week_key(wk)
            if p:
                parsed.append((p[0], p[1], wk))
        parsed.sort()
        weeks_to_show = [t[2] for t in parsed]

    if not weeks_to_show:
        await ctx.reply("No week keys found in matches.")
        return

    lines = []
    found_any = False

    for wk in weeks_to_show:
        # compute Monday..Sunday for header
        parsed = _parse_week_key(wk)
        if parsed:
            try:
                monday_dt = date.fromisocalendar(parsed[0], parsed[1], 1)
                sunday_dt = monday_dt + timedelta(days=6)
                header_suffix = f" — Mon {monday_dt.strftime('%d.%m.%Y')} - Sun {sunday_dt.strftime('%d.%m.%Y')}"
            except Exception:
                header_suffix = ""
        else:
            header_suffix = ""

        lines.append(f"\n=== Week {wk}{header_suffix} ===")

        # collect matches for this week
        matches_for_week = [(mid, mm) for mid, mm in data.get("matches", {}).items() if mm.get("week") == wk]
        if day_l:
            matches_for_week = [(mid, mm) for mid, mm in matches_for_week if mm.get("day") == day_l]

        # group duplicates (we keep a visible ×N badge here)
        reps = _group_matches_for_display(matches_for_week)

        for rep_mid, rep_m, count in reps:
            if not rep_m.get("score"):
                continue

            # display names (prefer stored name, else mention)
            def name_for(pid):
                u = data.get("users", {}).get(pid, {})
                return u.get("name") or u.get("display_name") or f"<@{pid}>"

            p1name = name_for(rep_m.get("p1"))
            p2name = name_for(rep_m.get("p2"))
            score = rep_m.get("score")
            status = "confirmed" if rep_m.get("confirmed") else ("forced" if rep_m.get("forced") else "reported")

            # compute numeric date + weekday for this match
            match_date = _date_for_weekday(rep_m.get("week"), rep_m.get("day"))
            if match_date:
                weeknum = rep_m.get("week").split("-W")[-1] if rep_m.get("week") else "?"
                date_s = match_date.strftime("%d.%m.%Y")
                weekday = match_date.strftime("%A")
                reported_label = f"W{weeknum} · {date_s} · {weekday}"
            else:
                # fallback to stored week / timestamp
                ts = rep_m.get("timestamp")
                reported_label = rep_m.get("week") or (ts or "unknown")

            dup_suffix = f" ×{count}" if count > 1 else ""
            # final compact line (no leading weekday before players)
            lines.append(f"[{rep_mid}] {p1name} vs {p2name}  => {score}  [{status}] ({reported_label}){dup_suffix}")
            found_any = True

    if not found_any:
        await ctx.reply("No matches with scores found for the given criteria.")
        return

    content = "\n".join(lines).lstrip("\n")
    if len(content) > 1900:
        safe_wk = (week_key or "all").replace(":", "-")
        path = f"allscores_{safe_wk}.txt"
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)
        await ctx.reply(file=discord.File(path))
    else:
        await ctx.reply(content)




@bot.command(name="scores")
@commands.check(lambda ctx: is_mod(ctx.author))
async def scores(ctx, member: discord.Member = None):
    """!scores @user - (mods only) show the sets/games stats & chronologically ordered set list for the mentioned user.
       Terminology: 'matches' -> 'sets' and 'sets' -> 'games'. Points = 10 * set_wins.
       Also shows Rank (same logic as !myscores / table).
    """
    if not member:
        await ctx.reply("Usage: `!scores @User`")
        return

    data = load_data()
    uid = str(member.id)
    if uid not in data.get("users", {}):
        await ctx.reply("The user is not registered in the leaderboard.")
        return

    # --- stats header (changed naming) ---
    stats = data["users"][uid].get("stats", {})
    sets_played = stats.get("played", 0)
    set_wins = stats.get("wins", 0)
    set_losses = stats.get("losses", 0)
    games_played = stats.get("sets_played", 0)
    games_won = stats.get("sets_won", 0)

    pct = (games_won / games_played * 100) if games_played > 0 else 0.0
    pts = 10 * set_wins

    # --- compute rank using same sort logic as table() ---
    users = data.get("users", {})
    def sort_key(kv):
        u = kv[1]
        s = u.get("stats", {})
        g_won = s.get("sets_won", 0)           # games_won
        g_played_local = s.get("sets_played", 0)  # games_played
        set_ratio = (g_won / g_played_local) if g_played_local > 0 else 0
        return (g_won, set_ratio, s.get("wins", 0))

    leaderboard = sorted(users.items(), key=sort_key, reverse=True)
    total_players = len(leaderboard)
    rank = None
    for pos, (user_id, _) in enumerate(leaderboard, start=1):
        if user_id == uid:
            rank = pos
            break
    if rank is None:
        rank = total_players  # fallback

    lines = [
        f"Scores for {member.display_name} (<@{uid}> / {data['users'][uid].get('name','?')}):",
        f"- Sets played: {sets_played}",
        f"- Set wins: {set_wins}",
        f"- Set losses: {set_losses}",
        f"- Games played: {games_played}",
        f"- Games won: {games_won}",
        f"- Points: {pts}",
        f"- Win%: {pct:.1f}%",
        f"- Rank: {rank} / {total_players}",
        "",
        "Sets (only those with recorded scores):"
    ]

    # --- helpers ---
    def _parse_week_key(wk):
        try:
            y, w = wk.split("-W")
            return int(y), int(w)
        except Exception:
            return None

    def _date_for_weekday(wk, day):
        p = _parse_week_key(wk)
        if not p or not day:
            return None
        year, weeknum = p
        try:
            wd_index = VALID_DAYS.index(day.lower()) + 1  # 1..7
            return date.fromisocalendar(year, weeknum, wd_index)
        except Exception:
            return None

    # collect sets (previously 'matches') involving this user
    matches_for_user = [(mid, m) for mid, m in data.get("matches", {}).items()
                        if m.get("score") and (m.get("p1") == uid or m.get("p2") == uid)]

    if not matches_for_user:
        lines.append("No sets with recorded scores yet for this user.")
        await ctx.reply("\n".join(lines))
        return

    # collapse duplicates by grouping on (week, day, players_set, score)
    groups = {}
    for mid, m in matches_for_user:
        wk = m.get("week"); day = m.get("day"); score = m.get("score")
        p1 = m.get("p1"); p2 = m.get("p2")
        if wk and day and p1 and p2 and score:
            key = (wk, day, frozenset((p1, p2)), score)
        else:
            key = ("__UNIQUE__", mid)
        groups.setdefault(key, []).append((mid, m))

    # pick representative per group (earliest timestamp if present)
    reps = []
    for key, lst in groups.items():
        def _ts_key(item):
            _, mm = item
            ts = mm.get("timestamp") or ""
            try:
                return datetime.fromisoformat(ts)
            except Exception:
                return ts
        lst_sorted = sorted(lst, key=_ts_key)
        rep_mid, rep_m = lst_sorted[0]
        reps.append((rep_mid, rep_m))

    # chronological sort key: prefer week+day -> date, else ISO timestamp, else week string
    def _chron_key(item):
        _, mm = item
        match_date = _date_for_weekday(mm.get("week"), mm.get("day"))
        if match_date:
            return (0, match_date)
        ts = mm.get("timestamp") or ""
        try:
            dt = datetime.fromisoformat(ts)
            return (1, dt)
        except Exception:
            wk = mm.get("week") or ""
            return (2, wk)

    reps.sort(key=_chron_key)

    # format each representative set line
    for rep_mid, rep_m in reps:
        p1 = rep_m.get("p1"); p2 = rep_m.get("p2")

        def disp_and_mention(pid):
            user_data = data.get("users", {}).get(pid, {})
            name = user_data.get("name") or user_data.get("display_name")
            try:
                gm = ctx.guild.get_member(int(pid)) if ctx.guild else None
                gm_name = gm.display_name if (gm and getattr(gm, "display_name", None)) else None
            except Exception:
                gm_name = None
            display = name or gm_name or f"<@{pid}>"
            mention = f"<@{pid}>"
            return f"{display} ({mention})"

        p1_str = disp_and_mention(p1)
        p2_str = disp_and_mention(p2)

        weekday = (rep_m.get("day") or "?").capitalize()

        match_date = _date_for_weekday(rep_m.get("week"), rep_m.get("day"))
        if match_date:
            weeknum = rep_m.get("week").split("-W")[-1] if rep_m.get("week") else "?"
            date_s = match_date.strftime("%d.%m.%Y")
            date_part = f"W{weeknum} {date_s}"
        else:
            date_part = rep_m.get("week") or (rep_m.get("timestamp") or "unknown")

        status = "confirmed" if rep_m.get("confirmed") else ("forced" if rep_m.get("forced") else "reported")
        score = rep_m.get("score", "?:?")
        lines.append(f"[{rep_mid}] {weekday} {date_part} {p1_str} vs {p2_str} — {score}  [{status}]")

    # reply (file fallback if long)
    content = "\n".join(lines)
    if len(content) > 1900:
        path = f"scores_{uid}.txt"
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)
        await ctx.reply(file=discord.File(path))
    else:
        await ctx.reply(content)





from datetime import datetime, date, timedelta

@bot.command(name="allsignups")
@commands.check(lambda ctx: is_mod(ctx.author))
async def allsignups(ctx, *args):
    """
    !allsignups - (mods only) show all signups from today until the last week that has any signup.
    Each signup is shown on its own line, grouped by region.
    """
    data = load_data()
    week_reset_if_needed(data)

    # collect all week_keys present in data
    signup_weeks = list(data.get("signups", {}).keys())
    if not signup_weeks:
        await ctx.reply("No signups found in the data store.")
        return

    # helper to parse "YYYY-WNN" -> (year, week)
    def parse_week_key(wk):
        try:
            parts = wk.split("-W")
            return int(parts[0]), int(parts[1])
        except Exception:
            return None

    parsed = [ (parse_week_key(wk)[0], parse_week_key(wk)[1], wk)
               for wk in signup_weeks if parse_week_key(wk) is not None ]

    if not parsed:
        await ctx.reply("No valid signup weeks found in the data store.")
        return

    # determine the last week that has any signup (max by (year,week))
    last_y, last_n, last_wk = max(parsed)

    # determine current week key (based on today)
    now = datetime.now(TIMEZONE).date()
    current_wk = week_key_for_date(now)
    cur_parsed = parse_week_key(current_wk)
    if not cur_parsed:
        await ctx.reply("Couldn't determine the current week key.")
        return
    cur_y, cur_n = cur_parsed

    # build week start dates (Monday) using isocalendar -> date.fromisocalendar
    try:
        start_date = date.fromisocalendar(cur_y, cur_n, 1)
        end_date = date.fromisocalendar(last_y, last_n, 1)
    except Exception:
        await ctx.reply("Date conversion error for week keys.")
        return

    if end_date < start_date:
        await ctx.reply("No upcoming signups found (last signup is before this week).")
        return

    lines = [f"All signups from {start_date.isoformat()} through week {last_wk} (inclusive):"]
    found_any = False
    wk_date = start_date
    while wk_date <= end_date:
        wk_key = week_key_for_date(wk_date)
        ensure_signups_week(data, wk_key)
        lines.append(f"\nWeek {wk_key}:")
        any_in_week = False

        for d in VALID_DAYS:
            uids = data.get("signups", {}).get(wk_key, {}).get(d, [])
            if not uids:
                continue

            # Prepare region buckets
            buckets = {"EU": [], "USA": [], "unknown": []}
            for uid in uids:
                user = data.get("users", {}).get(uid, {})
                region = user.get("region") or "unknown"
                key = region if region in ("EU", "USA") else "unknown"
                buckets[key].append(uid)

            # If there are no entries at all, skip
            if not any(buckets.values()):
                continue

            any_in_week = True
            found_any = True
            lines.append(f"- {d.capitalize()}:")

            # order: EU, USA, unknown
            for reg_key in ("EU", "USA", "unknown"):
                lst = buckets.get(reg_key, [])
                if not lst:
                    continue

                # sort by display name (if available) for stable output
                def sort_key(u):
                    return (data.get("users", {}).get(u, {}).get("name") or "").lower() or u
                lst_sorted = sorted(lst, key=sort_key)

                lines.append(f"  {reg_key}:")
                for uid in lst_sorted:
                    user = data.get("users", {}).get(uid, {})
                    display_name = user.get("name")
                    if display_name:
                        lines.append(f"    - <@{uid}> [{reg_key}] — {display_name}")
                    else:
                        lines.append(f"    - <@{uid}> [{reg_key}]")

        if not any_in_week:
            lines.append("- (no signups)")
        # move to next week
        wk_date = wk_date + timedelta(days=7)

    if not found_any:
        await ctx.reply("No signups found in the requested range.")
        return

    content = "\n".join(lines)
    if len(content) > 1900:
        path = f"allsignups_{current_wk}_to_{last_wk}.txt"
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)
        await ctx.reply(file=discord.File(path))
    else:
        await ctx.reply(content)




@bot.command(name="pastsignups")
@commands.check(lambda ctx: is_mod(ctx.author))
async def pastsignups(ctx, *args):
    """
    !pastsignups - (mods only) show ALL past signups (all weeks older than the current week).
    Extra args are ignored so accidental words won't crash the command.
    """
    data = load_data()
    week_reset_if_needed(data)

    def parse_week_key(wk):
        try:
            parts = wk.split("-W")
            return int(parts[0]), int(parts[1])
        except Exception:
            return None

    current_wk = data.get("week_key") or week_key_for_date(datetime.now(TIMEZONE).date())
    cur = parse_week_key(current_wk)
    if not cur:
        await ctx.reply("Couldn't determine the current week key.")
        return
    cur_y, cur_n = cur

    past_entries = []
    for wk in data.get("signups", {}).keys():
        p = parse_week_key(wk)
        if not p:
            continue
        y, n = p
        if (y, n) < (cur_y, cur_n):
            past_entries.append((y, n, wk))

    if not past_entries:
        await ctx.reply("No past signups found in the data store.")
        return

    # sort most recent first
    past_entries.sort(reverse=True)

    lines = [f"All past signups (every week older than current {current_wk}):"]
    found_any = False
    for y, n, wk_key in past_entries:
        ensure_signups_week(data, wk_key)
        lines.append(f"\nWeek {wk_key}:")
        any_in_week = False
        for d in VALID_DAYS:
            uids = data.get("signups", {}).get(wk_key, {}).get(d, [])
            if not uids:
                continue
            any_in_week = True
            found_any = True
            entries = []
            for uid in uids:
                user = data.get("users", {}).get(uid, {})
                name = user.get("name")
                region = user.get("region")
                if name and region:
                    entries.append(f"<@{uid}> ({name}) [{region}]")
                elif name:
                    entries.append(f"<@{uid}> ({name})")
                elif region:
                    entries.append(f"<@{uid}> [{region}]")
                else:
                    entries.append(f"<@{uid}>")

            lines.append(f"- {d.capitalize()}: " + ", ".join(entries))
        if not any_in_week:
            lines.append("- (no signups)")

    if not found_any:
        await ctx.reply("No past signups found.")
        return

    content = "\n".join(lines)
    if len(content) > 1900:
        path = f"pastsignups_all_older_than_{current_wk}.txt"
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)
        await ctx.reply(file=discord.File(path))
    else:
        await ctx.reply(content)


@bot.command(name="cancelsignupadmin")
@commands.check(lambda ctx: is_mod(ctx.author))
async def cancelsignupadmin(ctx, day: str, week_key: str, target_user: discord.Member):
    """
    Mod-only:
    !cancelsignupadmin <day> <week_key> <@user>
    - Force-remove a single user's signup for the given day/week.
    - ONLY removes the signup entry; does NOT touch week_counts, suggestions, matches or stats.
    - Week key formats accepted: 'YYYY-Www', 'Www', or 'ww' (week number).
    """
    import re
    from datetime import datetime as _dt

    day_l = (day or "").lower()
    if day_l not in VALID_DAYS:
        await ctx.reply(f"Invalid day. Pick one of: {', '.join(VALID_DAYS)}")
        return

    # normalize week_key input (accept YYYY-Wnn, Wnn, nn)
    wk = (week_key or "").strip()
    if re.match(r'^\d{4}-W\d{1,2}$', wk):
        week_key_norm = wk
    elif re.match(r'^W\d{1,2}$', wk, re.IGNORECASE):
        year = _dt.now(TIMEZONE).year
        week_key_norm = f"{year}-{wk.upper()}"
    elif re.match(r'^\d{1,2}$', wk):
        year = _dt.now(TIMEZONE).year
        week_key_norm = f"{year}-W{int(wk):02d}"
    else:
        await ctx.reply("Week key format not recognized. Use `YYYY-Www`, `Www` or `ww` (week number).")
        return

    uid = str(target_user.id)
    with IO_LOCK:
        data = load_data()
        week_reset_if_needed(data)
        ensure_signups_week(data, week_key_norm)

        signups_list = data.get("signups", {}).get(week_key_norm, {}).get(day_l, [])
        if uid in signups_list:
            try:
                data["signups"][week_key_norm][day_l].remove(uid)
                save_data(data)
                await ctx.reply(f"Removed signup for {target_user.mention} -> {day_l.capitalize()} (week {week_key_norm}).")
                return
            except Exception as e:
                # unexpected I/O / structure problem
                await ctx.reply(f"Failed to remove signup due to an error: `{e}`")
                return
        else:
            await ctx.reply(f"{target_user.mention} was not signed up for {day_l.capitalize()} (week {week_key_norm}).")
            return


@bot.command(name="cancelsignup")
async def cancelsignup(ctx, day: str):
    """!cancelsignup <day>
    - Cancels your signup for the given day in a best-effort week (tries this week, then next, then previous).
    - Enforces cutoff: you cannot cancel a signup after 16:00 on the target day (in TIMEZONE).
    - Only removes the signup entry and decrements the user's week_counts for that week.
    """
    import re
    from datetime import datetime as _dt, date as _date, timedelta as _timedelta

    data = load_data()
    week_reset_if_needed(data)

    day_l = (day or "").lower()
    if day_l not in VALID_DAYS:
        await ctx.reply(f"Invalid day. Pick one of: {', '.join(VALID_DAYS)}")
        return

    uid = str(ctx.author.id)

    # helper: convert normalized week_key like '2025-W33' + weekday to a date
    def weekkey_and_day_to_date(wkkey, weekday_name):
        """
        wkkey expected normalized like 'YYYY-Www'
        weekday_name like 'monday'..'sunday'
        returns a datetime.date or None
        """
        if not wkkey:
            return None
        m = re.match(r'^(?P<year>\d{4})-W(?P<week>\d{1,2})$', wkkey, re.IGNORECASE)
        if not m:
            return None
        year = int(m.group("year"))
        week = int(m.group("week"))
        weekday_map = {"monday":1, "tuesday":2, "wednesday":3, "thursday":4, "friday":5, "saturday":6, "sunday":7}
        wd = weekday_map.get(weekday_name.lower())
        if not wd:
            return None
        try:
            return _date.fromisocalendar(year, week, wd)
        except Exception:
            return None

    # cutoff check: true if the candidate target date is today AND now >= 16:00 in TIMEZONE
    def is_after_cutoff_for_weekday(wkkey, weekday_name):
        target_date = weekkey_and_day_to_date(wkkey, weekday_name)
        if not target_date:
            return False
        now = _dt.now(TIMEZONE)
        cutoff_dt = _dt(target_date.year, target_date.month, target_date.day, 16, 0, 0, tzinfo=TIMEZONE)
        return now.date() == target_date and now >= cutoff_dt

    # helper to remove signup (returns True if removed)
    def remove_if_present(d, wk):
        ensure_signups_week(d, wk)
        lst = d["signups"][wk].get(day_l, [])
        if uid in lst:
            try:
                lst.remove(uid)
            except ValueError:
                return False
            # adjust week_counts on user (best-effort)
            u = d.get("users", {}).get(uid)
            if u:
                u.setdefault("week_counts", {})
                u["week_counts"][wk] = max(0, u["week_counts"].get(wk, 1) - 1)
            save_data(d)
            return True
        return False

    # Try current week, then next, then previous (so user doesn't have to give week)
    base_date = _dt.now(TIMEZONE).date()
    tried = []
    for wdelta in (0, 7, -7):  # this week, next week, previous week
        candidate_date = base_date + _timedelta(days=wdelta)
        wk_key = week_key_for_date(candidate_date)
        tried.append(wk_key)

        # if this candidate resolves to today and we're after cutoff -> disallow
        if is_after_cutoff_for_weekday(wk_key, day_l):
            await ctx.reply("You cannot cancel a signup after 16:00 CEST on the target day.")
            return

        # attempt removal under IO_LOCK to avoid races
        with IO_LOCK:
            data_locked = load_data()
            week_reset_if_needed(data_locked)
            if remove_if_present(data_locked, wk_key):
                await ctx.reply(f"Removed signup for {day_l.capitalize()} (week {wk_key}).")
                return

    # nothing found in the tried weeks
    await ctx.reply(f"No signups found for {day_l.capitalize()} in the checked weeks: {', '.join(tried)}. "
                    f"Use `!signuplist {day_l} <week>` to inspect specific week keys if needed.")




# ----------------- Run ------------------

if __name__ == "__main__":
    # Read the token from environment variable BOT_TOKEN. This avoids storing tokens
    # in the source code.
    TOKEN = os.environ.get("BOT_TOKEN")
    if not TOKEN:
        print("Please set BOT_TOKEN environment variable or edit the code to add your token.")
    else:
        bot.run(TOKEN)
