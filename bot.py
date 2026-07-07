import os
import asyncio
import discord
from discord import app_commands
from dotenv import load_dotenv
import random
import string
import math
import re
import sqlite3
from datetime import datetime, timedelta, timezone, time
from collections import defaultdict

load_dotenv()
TOKEN = os.getenv('DISCORD_TOKEN')

intents = discord.Intents.default()
intents.message_content = True
intents.members = True  # サーバーメンバーイベント用

# ---------- 簡易イベントバス ----------
class EventBus:
    def __init__(self):
        self._listeners = defaultdict(list)

    def on(self, event: str, callback):
        self._listeners[event].append(callback)

    async def emit(self, event: str, *args, **kwargs):
        for callback in self._listeners[event]:
            await callback(*args, **kwargs)

event_bus = EventBus()

# ---------- SQLite 初期化 ----------
DB_FILE = "lounge_bot.db"

def init_db():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            discord_id INTEGER PRIMARY KEY,
            dmp INTEGER NOT NULL DEFAULT 1500,
            wins INTEGER NOT NULL DEFAULT 0,
            losses INTEGER NOT NULL DEFAULT 0,
            draws INTEGER NOT NULL DEFAULT 0,
            current_win_streak INTEGER NOT NULL DEFAULT 0,
            highest_win_streak INTEGER NOT NULL DEFAULT 0,
            is_wanted INTEGER NOT NULL DEFAULT 0,
            penalty_points INTEGER NOT NULL DEFAULT 0,
            current_title TEXT
        )
    ''')
    for col, col_type in [("wins", "INTEGER NOT NULL DEFAULT 0"),
                          ("losses", "INTEGER NOT NULL DEFAULT 0"),
                          ("draws", "INTEGER NOT NULL DEFAULT 0"),
                          ("current_win_streak", "INTEGER NOT NULL DEFAULT 0"),
                          ("highest_win_streak", "INTEGER NOT NULL DEFAULT 0"),
                          ("is_wanted", "INTEGER NOT NULL DEFAULT 0"),
                          ("penalty_points", "INTEGER NOT NULL DEFAULT 0"),
                          ("current_title", "TEXT")]:
        try:
            cursor.execute(f"ALTER TABLE users ADD COLUMN {col} {col_type}")
        except:
            pass

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS matches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            room_id TEXT NOT NULL,
            player1_id INTEGER NOT NULL,
            player2_id INTEGER NOT NULL,
            condition INTEGER NOT NULL,
            score_a INTEGER NOT NULL,
            score_b INTEGER NOT NULL,
            winner_id INTEGER,
            before_dmp_a INTEGER,
            after_dmp_a INTEGER,
            before_dmp_b INTEGER,
            after_dmp_b INTEGER,
            change_a INTEGER NOT NULL,
            change_b INTEGER NOT NULL,
            confirmed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    for col, col_type in [("before_dmp_a", "INTEGER"),
                          ("after_dmp_a", "INTEGER"),
                          ("before_dmp_b", "INTEGER"),
                          ("after_dmp_b", "INTEGER")]:
        try:
            cursor.execute(f"ALTER TABLE matches ADD COLUMN {col} {col_type}")
        except:
            pass

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS rating_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            match_id INTEGER NOT NULL,
            before_dmp INTEGER NOT NULL,
            after_dmp INTEGER NOT NULL,
            dmp_change INTEGER NOT NULL,
            expected_score REAL NOT NULL,
            multiplier REAL NOT NULL,
            weight REAL NOT NULL,
            wanted_bonus REAL NOT NULL DEFAULT 0.0,
            k_factor INTEGER NOT NULL DEFAULT 60,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS match_queue (
            user_id INTEGER PRIMARY KEY,
            user_name TEXT NOT NULL,
            condition INTEGER NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            expire_at TIMESTAMP
        )
    ''')
    try:
        cursor.execute("ALTER TABLE match_queue ADD COLUMN expire_at TIMESTAMP")
    except:
        pass

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS designated_queue (
            user_id INTEGER PRIMARY KEY,
            user_name TEXT NOT NULL,
            joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS wanted_pool (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            amount INTEGER NOT NULL DEFAULT 0
        )
    ''')
    cursor.execute('INSERT OR IGNORE INTO wanted_pool (id, amount) VALUES (1, 0)')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS system_settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
    ''')
    cursor.execute('INSERT OR IGNORE INTO system_settings (key, value) VALUES (?, ?)', ('current_season', '1'))
    cursor.execute('INSERT OR IGNORE INTO system_settings (key, value) VALUES (?, ?)', ('soft_reset_ratio', '0.5'))
    cursor.execute('INSERT OR IGNORE INTO system_settings (key, value) VALUES (?, ?)', ('last_season_reset', datetime.now().strftime('%Y-%m-%d')))

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS active_rooms (
            channel_id INTEGER PRIMARY KEY,
            room_id TEXT NOT NULL,
            player1_id INTEGER NOT NULL,
            player2_id INTEGER NOT NULL,
            condition INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'ACTIVE',
            submitter_id INTEGER,
            submitter_score INTEGER DEFAULT 0,
            opponent_score INTEGER DEFAULT 0,
            opponent_id INTEGER
        )
    ''')
    conn.commit()
    conn.close()

init_db()

# ---------- ヘルパー ----------
def get_setting(key: str) -> str:
    conn = sqlite3.connect(DB_FILE)
    row = conn.execute('SELECT value FROM system_settings WHERE key = ?', (key,)).fetchone()
    conn.close()
    return row[0] if row else ""

def set_setting(key: str, value: str):
    conn = sqlite3.connect(DB_FILE)
    conn.execute('INSERT OR REPLACE INTO system_settings (key, value) VALUES (?, ?)', (key, value))
    conn.commit()
    conn.close()

def load_queue_from_db():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('SELECT user_id, user_name, condition, created_at, expire_at FROM match_queue ORDER BY created_at ASC')
    rows = cursor.fetchall()
    conn.close()
    return [{'user_id': row[0], 'user_name': row[1], 'condition': row[2], 'created_at': row[3], 'expire_at': row[4]} for row in rows]

def add_to_queue_db(user_id: int, user_name: str, condition: int):
    global match_queue
    expire_at = datetime.now() + timedelta(minutes=condition)
    match_queue.append({'user_id': user_id, 'user_name': user_name, 'condition': condition, 'expire_at': expire_at})
    conn = sqlite3.connect(DB_FILE)
    conn.execute('INSERT OR REPLACE INTO match_queue (user_id, user_name, condition, created_at, expire_at) VALUES (?, ?, ?, ?, ?)',
                 (user_id, user_name, condition, datetime.now(), expire_at))
    conn.commit()
    conn.close()

def remove_from_queue_db(user_id: int):
    global match_queue
    match_queue = [entry for entry in match_queue if entry['user_id'] != user_id]
    conn = sqlite3.connect(DB_FILE)
    conn.execute('DELETE FROM match_queue WHERE user_id = ?', (user_id,))
    conn.commit()
    conn.close()

def add_to_designated_queue(user_id: int, user_name: str):
    conn = sqlite3.connect(DB_FILE)
    conn.execute('INSERT OR IGNORE INTO designated_queue (user_id, user_name) VALUES (?, ?)', (user_id, user_name))
    conn.commit()
    conn.close()

def remove_from_designated_queue(user_id: int):
    conn = sqlite3.connect(DB_FILE)
    conn.execute('DELETE FROM designated_queue WHERE user_id = ?', (user_id,))
    conn.commit()
    conn.close()

def get_designated_queue():
    conn = sqlite3.connect(DB_FILE)
    rows = conn.execute('SELECT user_id, user_name FROM designated_queue ORDER BY joined_at ASC').fetchall()
    conn.close()
    return [{'user_id': r[0], 'user_name': r[1]} for r in rows]

def clear_designated_queue():
    conn = sqlite3.connect(DB_FILE)
    conn.execute('DELETE FROM designated_queue')
    conn.commit()
    conn.close()

def save_active_room(channel_id: int, match_info: dict):
    conn = sqlite3.connect(DB_FILE)
    conn.execute('''
        INSERT OR REPLACE INTO active_rooms (channel_id, room_id, player1_id, player2_id, condition, status, submitter_id, submitter_score, opponent_score, opponent_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (channel_id, match_info['room_id'], match_info['player1_id'], match_info['player2_id'],
          match_info['condition'], match_info['status'], match_info.get('submitter_id'),
          match_info.get('submitter_score', 0), match_info.get('opponent_score', 0),
          match_info.get('opponent_id')))
    conn.commit()
    conn.close()

def delete_active_room(channel_id: int):
    conn = sqlite3.connect(DB_FILE)
    conn.execute('DELETE FROM active_rooms WHERE channel_id = ?', (channel_id,))
    conn.commit()
    conn.close()

def load_active_rooms():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('SELECT channel_id, room_id, player1_id, player2_id, condition, status, submitter_id, submitter_score, opponent_score, opponent_id FROM active_rooms')
    rows = cursor.fetchall()
    conn.close()
    rooms = {}
    for row in rows:
        rooms[row[0]] = {
            'room_id': row[1],
            'player1_id': row[2],
            'player2_id': row[3],
            'condition': row[4],
            'status': row[5],
            'submitter_id': row[6],
            'submitter_score': row[7],
            'opponent_score': row[8],
            'opponent_id': row[9]
        }
    return rooms

def get_user_stats(discord_id: int) -> dict:
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('SELECT dmp, wins, losses, draws, current_win_streak, highest_win_streak, is_wanted, penalty_points, current_title FROM users WHERE discord_id = ?', (discord_id,))
    row = cursor.fetchone()
    conn.close()
    if row:
        return {
            'dmp': row[0], 'wins': row[1], 'losses': row[2], 'draws': row[3],
            'current_win_streak': row[4], 'highest_win_streak': row[5],
            'is_wanted': bool(row[6]), 'penalty_points': row[7],
            'current_title': row[8]
        }
    else:
        conn = sqlite3.connect(DB_FILE)
        conn.execute('INSERT INTO users (discord_id) VALUES (?)', (discord_id,))
        conn.commit()
        conn.close()
        return {'dmp': 1500, 'wins': 0, 'losses': 0, 'draws': 0,
                'current_win_streak': 0, 'highest_win_streak': 0,
                'is_wanted': False, 'penalty_points': 0, 'current_title': None}

def update_stats(discord_id: int, dmp_change: int, result: str, new_title: str = None):
    conn = sqlite3.connect(DB_FILE)
    stats = get_user_stats(discord_id)
    new_dmp = stats['dmp'] + dmp_change
    wins, losses, draws = stats['wins'], stats['losses'], stats['draws']
    streak = stats['current_win_streak']
    highest_streak = stats['highest_win_streak']
    is_wanted = stats['is_wanted']

    if result == 'win':
        wins += 1
        streak += 1
        highest_streak = max(highest_streak, streak)
        if streak >= 5:
            is_wanted = True
    elif result == 'loss':
        losses += 1
        streak = 0
        is_wanted = False
    elif result == 'draw':
        draws += 1
        streak = 0
        is_wanted = False

    title = stats['current_title']
    if new_title:
        title = new_title

    conn.execute('''
        UPDATE users SET dmp=?, wins=?, losses=?, draws=?, current_win_streak=?, highest_win_streak=?, is_wanted=?, current_title=?
        WHERE discord_id=?
    ''', (new_dmp, wins, losses, draws, streak, highest_streak, int(is_wanted), title, discord_id))
    conn.commit()
    conn.close()

def save_match_to_db(room_id, player1_id, player2_id, condition, score_a, score_b, winner_id, 
                     before_dmp_a, after_dmp_a, before_dmp_b, after_dmp_b, change_a, change_b):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO matches (room_id, player1_id, player2_id, condition, score_a, score_b, winner_id,
                             before_dmp_a, after_dmp_a, before_dmp_b, after_dmp_b, change_a, change_b)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (room_id, player1_id, player2_id, condition, score_a, score_b, winner_id,
          before_dmp_a, after_dmp_a, before_dmp_b, after_dmp_b, change_a, change_b))
    match_id = cursor.lastrowid
    conn.commit()
    conn.close()
    return match_id

def save_rating_history(user_id, match_id, before_dmp, after_dmp, dmp_change, expected_score, multiplier, weight, wanted_bonus=0.0, k_factor=60):
    conn = sqlite3.connect(DB_FILE)
    conn.execute('''
        INSERT INTO rating_history (user_id, match_id, before_dmp, after_dmp, dmp_change, expected_score, multiplier, weight, wanted_bonus, k_factor)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (user_id, match_id, before_dmp, after_dmp, dmp_change, expected_score, multiplier, weight, wanted_bonus, k_factor))
    conn.commit()
    conn.close()

def get_wanted_pool():
    conn = sqlite3.connect(DB_FILE)
    row = conn.execute('SELECT amount FROM wanted_pool WHERE id = 1').fetchone()
    conn.close()
    return row[0] if row else 0

def add_to_wanted_pool(amount: int):
    conn = sqlite3.connect(DB_FILE)
    conn.execute('UPDATE wanted_pool SET amount = amount + ? WHERE id = 1', (amount,))
    conn.commit()
    conn.close()

def take_from_wanted_pool(amount: int) -> bool:
    conn = sqlite3.connect(DB_FILE)
    cur = conn.execute('SELECT amount FROM wanted_pool WHERE id = 1')
    pool = cur.fetchone()[0]
    if pool >= amount:
        conn.execute('UPDATE wanted_pool SET amount = amount - ? WHERE id = 1', (amount,))
        conn.commit()
        conn.close()
        return True
    conn.close()
    return False

# ---------- DMP 計算 ----------
def calculate_dmp(player_a_dmp, player_b_dmp, score_a, score_b, total_races, player_a_streak=0, designated_match=False):
    expected_a = 1 / (1 + 10 ** ((player_b_dmp - player_a_dmp) / 400))
    SA = 1.0 if score_a > score_b else (0.0 if score_a < score_b else 0.5)
    scaled_diff = (abs(score_a - score_b) / total_races) * 12
    multiplier = 0.75 + 0.75 * ((scaled_diff / 24) ** 2.3)
    if total_races <= 30:
        weight = 0.6
    elif total_races <= 60:
        weight = 1.0
    else:
        weight = 1.3
    if player_a_streak <= -3:
        expected_a = max(0.0, expected_a - 0.1)
    K = 60
    base_delta_a = round(K * weight * multiplier * (SA - expected_a))
    delta_a = base_delta_a
    extra_bonus = 0
    if designated_match and SA == 1.0:
        extra_bonus = round(base_delta_a * 0.2)
        delta_a += extra_bonus
    return delta_a, -base_delta_a, extra_bonus

# ---------- Bot クラス ----------
class MyBot(discord.Client):
    def __init__(self):
        super().__init__(intents=intents)
        self.tree = app_commands.CommandTree(self)
        self.first_ready = True  # 初回起動フラグ

    async def setup_hook(self):
        # 初回のみグローバル同期、以降は必要最低限
        if self.first_ready:
            await self.tree.sync()
            print('✅ スラッシュコマンドをグローバル同期しました')
        self.first_ready = False

bot = MyBot()

match_queue = load_queue_from_db()
active_matches = load_active_rooms()
active_queue_messages = {}

# ---------- チャンネル名 ----------
CHAT_STORAGE_CATEGORY = "運営・防壁"
CHAT_STORAGE_CHANNEL = "chat-storage"
INFO_NEWS_CHANNEL = "information-news"
BOUNTY_CHANNEL = "bounty-hunters"
RANK_CHANNEL = "rank-leaderboard"
LOUNGE_TALK_CHANNEL = "lounge-talk"
SYSTEM_LOG_CHANNEL = "system-log-alerts"
ADMIN_CHANNEL = "admin-commands"
MATCH_LOBBY_CHANNEL = "match-lobby"
DESIGNATED_MATCH_CHANNEL = "designated-match"
PICTURE_READ_CHANNEL = "picture-read-validation"
RULES_CHANNEL = "rules"
WELCOME_CHANNEL = "welcome"

NG_WORDS = ["死ね", "殺す", "クソ", "fuck", "バカ", "アホ"]

def generate_room_id():
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=4))

def is_match_room(channel: discord.TextChannel) -> bool:
    if channel.id in active_matches:
        return True
    if channel.category and channel.category.name == "対戦中部屋":
        if re.match(r'^room-[A-Z0-9]{4}$', channel.name):
            return True
    return False

async def delete_queue_message(user_id: int):
    if user_id in active_queue_messages:
        channel_id, message_id = active_queue_messages[user_id]
        try:
            channel = bot.get_channel(channel_id)
            if channel:
                msg = await channel.fetch_message(message_id)
                await msg.delete()
        except:
            pass
        finally:
            del active_queue_messages[user_id]

async def ensure_channel(guild: discord.Guild, name: str, category: discord.CategoryChannel = None,
                         permissions: dict = None):
    existing = discord.utils.get(guild.text_channels, name=name)
    if existing:
        return existing
    overwrites = permissions or {}
    if category:
        return await category.create_text_channel(name, overwrites=overwrites)
    return await guild.create_text_channel(name, overwrites=overwrites)

async def send_system_log(message: str):
    for guild in bot.guilds:
        channel = discord.utils.get(guild.text_channels, name=SYSTEM_LOG_CHANNEL)
        if channel:
            await channel.send(f"🛠️ {message}")
            return

async def ensure_roles(guild: discord.Guild):
    bot_role = discord.utils.get(guild.roles, name="ボット")
    if bot_role is None:
        bot_role = await guild.create_role(name="ボット", color=discord.Color.blue())
    if bot_role not in guild.me.roles:
        await guild.me.add_roles(bot_role)

    sub_admin_role = discord.utils.get(guild.roles, name="副管理者")
    if sub_admin_role is None:
        await guild.create_role(name="副管理者", permissions=discord.Permissions(administrator=True))

    unverified_role = discord.utils.get(guild.roles, name="未認証")
    if unverified_role is None:
        unverified_role = await guild.create_role(name="未認証", mentionable=False)
        for ch in guild.channels:
            if ch.name != WELCOME_CHANNEL:
                await ch.set_permissions(unverified_role, view_channel=False)

    member_role = discord.utils.get(guild.roles, name="メンバー")
    if member_role is None:
        member_role = await guild.create_role(name="メンバー", mentionable=True)

# ---------- レート制限リトライ用 ----------
async def bot_login_with_retry(max_retries=5):
    for i in range(max_retries):
        try:
            await bot.start(TOKEN)
            return
        except discord.HTTPException as e:
            if e.status == 429:
                retry_after = e.retry_after or 60
                print(f"429 Too Many Requests: retry after {retry_after} seconds")
                await asyncio.sleep(retry_after)
        except Exception as e:
            print(f"Login error: {e}")
            await asyncio.sleep(10)
    print("Failed to login after retries")

# ---------- ニックネーム認証 ----------
@bot.event
async def on_member_join(member: discord.Member):
    guild = member.guild
    if member.guild_permissions.administrator or member == guild.owner or discord.utils.get(member.roles, name="管理者"):
        member_role = discord.utils.get(guild.roles, name="メンバー")
        if member_role:
            await member.add_roles(member_role)
        return

    unverified_role = discord.utils.get(guild.roles, name="未認証")
    if unverified_role:
        await member.add_roles(unverified_role)

    try:
        await member.send(
            "**ラウンジへようこそ！**\n"
            "このサーバーでは個人情報保護のため、ニックネームでの参加をお願いしています。\n"
            "以下のコマンドを **このDMで** 実行して、ニックネームを登録してください。\n\n"
            "`/nickname あなたの希望する名前`\n\n"
            "例: `/nickname タカ`\n\n"
            "登録が完了すると、すべてのチャンネルが利用可能になります。"
        )
    except:
        pass

@bot.tree.command(name="nickname", description="サーバー用のニックネームを登録する（DMで実行）")
@app_commands.describe(name="希望するニックネーム")
async def nickname(interaction: discord.Interaction, name: str):
    if not isinstance(interaction.channel, discord.DMChannel):
        await interaction.response.send_message("このコマンドはBotとのDMでのみ実行できます。", ephemeral=True)
        return

    if len(name) < 2 or len(name) > 32:
        await interaction.response.send_message("ニックネームは2〜32文字で設定してください。", ephemeral=True)
        return
    if not re.match(r'^[a-zA-Z0-9ぁ-んァ-ヶ一-龥々ー_]+$', name):
        await interaction.response.send_message("ニックネームに使えない文字が含まれています。英数字・日本語・アンダーバーが使えます。", ephemeral=True)
        return

    user_id = interaction.user.id
    guild = bot.guilds[0] if bot.guilds else None
    if guild is None:
        await interaction.response.send_message("サーバーが見つかりません。", ephemeral=True)
        return

    member = guild.get_member(user_id)
    if member is None:
        await interaction.response.send_message("サーバーに参加していないか、メンバーが見つかりません。", ephemeral=True)
        return

    try:
        await member.edit(nick=name)
    except discord.Forbidden:
        await interaction.response.send_message("権限不足でニックネームを変更できませんでした。", ephemeral=True)
        return

    unverified_role = discord.utils.get(guild.roles, name="未認証")
    member_role = discord.utils.get(guild.roles, name="メンバー")
    if unverified_role:
        await member.remove_roles(unverified_role)
    if member_role:
        await member.add_roles(member_role)

    await interaction.response.send_message(f"✅ ニックネームを **{name}** に設定しました！サーバー内のチャンネルが利用可能になりました。")

# ---------- 管理者用ニックネーム設定コマンド ----------
@bot.tree.command(name="setname", description="自分のニックネームを変更する（管理者専用）")
@app_commands.describe(name="設定する新しいニックネーム")
async def setname(interaction: discord.Interaction, name: str):
    if not interaction.user.guild_permissions.administrator:
        admin_role = discord.utils.get(interaction.guild.roles, name="管理者")
        if admin_role is None or admin_role not in interaction.user.roles:
            await interaction.response.send_message("あなたにはこのコマンドを実行する権限がありません。", ephemeral=True)
            return

    if len(name) < 2 or len(name) > 32:
        await interaction.response.send_message("ニックネームは2〜32文字で設定してください。", ephemeral=True)
        return
    if not re.match(r'^[a-zA-Z0-9ぁ-んァ-ヶ一-龥々ー_]+$', name):
        await interaction.response.send_message("ニックネームに使えない文字が含まれています。英数字・日本語・アンダーバーが使えます。", ephemeral=True)
        return

    try:
        await interaction.user.edit(nick=name)
        await interaction.response.send_message(f"✅ ニックネームを **{name}** に変更しました。")
    except discord.Forbidden:
        await interaction.response.send_message("権限不足でニックネームを変更できませんでした。", ephemeral=True)

# ---------- サービス ----------
async def rating_service(submitter_id, opponent_id, score_a, score_b, total_races, condition, room_id, player1_id, player2_id, interaction_guild, designated_match=False):
    dmp_a = get_user_stats(submitter_id)['dmp']
    dmp_b = get_user_stats(opponent_id)['dmp']
    delta_a, delta_b, extra_bonus = calculate_dmp(dmp_a, dmp_b, score_a, score_b, total_races, 
                                                  player_a_streak=(-3 if get_user_stats(submitter_id)['losses'] >= 3 else 0),
                                                  designated_match=designated_match)
    change_a, change_b = delta_a, delta_b
    wanted_bonus_used = 0
    if extra_bonus > 0:
        if take_from_wanted_pool(extra_bonus):
            change_a = delta_a + extra_bonus
            wanted_bonus_used = extra_bonus

    bonus_a = 0
    bonus_b = 0
    stats_a = get_user_stats(submitter_id)
    stats_b = get_user_stats(opponent_id)
    if stats_a['is_wanted'] or stats_b['is_wanted']:
        pool = get_wanted_pool()
        if pool > 0:
            if stats_a['is_wanted']:
                bonus_b = min(pool, 20)
            if stats_b['is_wanted']:
                bonus_a = min(pool, 20)
            if bonus_a > 0 and take_from_wanted_pool(bonus_a):
                change_a += bonus_a
            if bonus_b > 0 and take_from_wanted_pool(bonus_b):
                change_b += bonus_b

    if score_a > score_b:
        result_a, result_b, winner_id = 'win', 'loss', submitter_id
    elif score_a < score_b:
        result_a, result_b, winner_id = 'loss', 'win', opponent_id
    else:
        result_a, result_b, winner_id = 'draw', 'draw', None

    new_title_a = None
    new_title_b = None
    if result_a == 'win' and get_user_stats(submitter_id)['current_win_streak'] + 1 == 5:
        new_title_a = "連勝王"
    elif result_a == 'win' and get_user_stats(submitter_id)['current_win_streak'] + 1 == 10:
        new_title_a = "無双"
    if result_b == 'win' and get_user_stats(opponent_id)['current_win_streak'] + 1 == 5:
        new_title_b = "連勝王"
    elif result_b == 'win' and get_user_stats(opponent_id)['current_win_streak'] + 1 == 10:
        new_title_b = "無双"
    update_stats(submitter_id, change_a, result_a, new_title_a)
    update_stats(opponent_id, change_b, result_b, new_title_b)
    new_dmp_a = dmp_a + change_a
    new_dmp_b = dmp_b + change_b
    match_id = save_match_to_db(room_id, player1_id, player2_id, condition, score_a, score_b, winner_id,
                                dmp_a, new_dmp_a, dmp_b, new_dmp_b, change_a, change_b)
    expected_a = 1 / (1 + 10 ** ((dmp_b - dmp_a) / 400))
    scaled_diff_val = (abs(score_a - score_b) / total_races) * 12
    multiplier_val = 0.75 + 0.75 * ((scaled_diff_val / 24) ** 2.3)
    weight_val = 0.6 if total_races <= 30 else (1.0 if total_races <= 60 else 1.3)
    save_rating_history(submitter_id, match_id, dmp_a, new_dmp_a, change_a, expected_a, multiplier_val, weight_val, wanted_bonus_used + bonus_a)
    save_rating_history(opponent_id, match_id, dmp_b, new_dmp_b, change_b, expected_a, multiplier_val, weight_val, bonus_b)
    return {
        'winner_id': winner_id,
        'new_dmp_a': new_dmp_a, 'new_dmp_b': new_dmp_b,
        'change_a': change_a, 'change_b': change_b,
        'result_a': result_a, 'result_b': result_b,
        'match_id': match_id
    }

async def record_service(submitter_id, opponent_id, result_a, result_b, interaction_guild):
    stats_a = get_user_stats(submitter_id)
    stats_b = get_user_stats(opponent_id)
    news_channel = discord.utils.get(interaction_guild.text_channels, name=INFO_NEWS_CHANNEL)
    if result_a == 'win' and stats_a['current_win_streak'] >= stats_a['highest_win_streak']:
        if news_channel:
            await news_channel.send(f"🏆 <@{submitter_id}> が最高連勝記録を更新！ **{stats_a['current_win_streak']}連勝** 達成！")
    if result_b == 'win' and stats_b['current_win_streak'] >= stats_b['highest_win_streak']:
        if news_channel:
            await news_channel.send(f"🏆 <@{opponent_id}> が最高連勝記録を更新！ **{stats_b['current_win_streak']}連勝** 達成！")

async def ensure_chat_storage(guild: discord.Guild):
    category = discord.utils.get(guild.categories, name=CHAT_STORAGE_CATEGORY)
    if category is None:
        admin_role = discord.utils.get(guild.roles, permissions=discord.Permissions(administrator=True))
        overwrites_cat = {
            guild.default_role: discord.PermissionOverwrite(view_channel=False),
            guild.me: discord.PermissionOverwrite(view_channel=True)
        }
        if admin_role:
            overwrites_cat[admin_role] = discord.PermissionOverwrite(view_channel=True)
        category = await guild.create_category(CHAT_STORAGE_CATEGORY, overwrites=overwrites_cat)
    return await ensure_channel(guild, CHAT_STORAGE_CHANNEL, category, {
        guild.default_role: discord.PermissionOverwrite(view_channel=False, send_messages=False),
        guild.me: discord.PermissionOverwrite(view_channel=True, send_messages=True)
    })

async def archive_chat(channel: discord.TextChannel, match_info: dict):
    try:
        storage = await ensure_chat_storage(channel.guild)
        if storage is None:
            return
        messages = [msg async for msg in channel.history(limit=1000, oldest_first=True)]
        if not messages:
            return
        embed = discord.Embed(
            title=f"📜 対戦ログ: room-{match_info['room_id']}",
            description=f"対戦者: <@{match_info['player1_id']}> vs <@{match_info['player2_id']}>\n条件: {match_info['condition']}分\n確定: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            color=0x3498db
        )
        log_text = ""
        for msg in messages:
            ts = msg.created_at.strftime('%H:%M:%S')
            content = msg.content or "(画像/添付)"
            for att in msg.attachments:
                content += f"\n📎 {att.url}"
            log_text += f"[{ts}] {msg.author.display_name}: {content}\n"
        if len(log_text) > 3800:
            chunks = [log_text[i:i+3800] for i in range(0, len(log_text), 3800)]
            for i, chunk in enumerate(chunks):
                if i == 0:
                    embed.add_field(name="チャットログ (1)", value=f"```{chunk}```", inline=False)
                else:
                    await storage.send(embed=discord.Embed(description=f"```{chunk}```"))
        else:
            embed.add_field(name="チャットログ", value=f"```{log_text}```", inline=False)
        await storage.send(embed=embed)
    except Exception as e:
        await send_system_log(f"チャットログ退避エラー: {e}")

async def post_news(guild: discord.Guild, embed: discord.Embed):
    channel = discord.utils.get(guild.text_channels, name=INFO_NEWS_CHANNEL)
    if channel is None:
        channel = await ensure_channel(guild, INFO_NEWS_CHANNEL, permissions={
            guild.default_role: discord.PermissionOverwrite(send_messages=False, view_channel=True),
            guild.me: discord.PermissionOverwrite(send_messages=True)
        })
    await channel.send(embed=embed)

async def update_bounty_board(guild: discord.Guild):
    channel = discord.utils.get(guild.text_channels, name=BOUNTY_CHANNEL)
    if channel is None:
        channel = await ensure_channel(guild, BOUNTY_CHANNEL, permissions={
            guild.default_role: discord.PermissionOverwrite(send_messages=False, view_channel=True),
            guild.me: discord.PermissionOverwrite(send_messages=True)
        })
    conn = sqlite3.connect(DB_FILE)
    rows = conn.execute('SELECT discord_id, current_win_streak FROM users WHERE is_wanted = 1').fetchall()
    conn.close()
    async for msg in channel.history():
        await msg.delete()
    if not rows:
        await channel.send("現在賞金首はいません。")
        return
    for row in rows:
        user = bot.get_user(row[0])
        if user:
            embed = discord.Embed(title="💰 賞金首手配書", color=0xff0000)
            embed.add_field(name="プレイヤー", value=user.mention, inline=True)
            embed.add_field(name="連勝数", value=str(row[1]), inline=True)
            embed.set_thumbnail(url=user.display_avatar.url)
            await channel.send(embed=embed)

async def leaderboard_updater():
    await bot.wait_until_ready()
    while not bot.is_closed():
        try:
            for guild in bot.guilds:
                channel = discord.utils.get(guild.text_channels, name=RANK_CHANNEL)
                if channel is None:
                    channel = await ensure_channel(guild, RANK_CHANNEL, permissions={
                        guild.default_role: discord.PermissionOverwrite(send_messages=False, view_channel=True),
                        guild.me: discord.PermissionOverwrite(send_messages=True)
                    })
                conn = sqlite3.connect(DB_FILE)
                rows = conn.execute('SELECT discord_id, dmp, wins, losses, draws FROM users ORDER BY dmp DESC LIMIT 10').fetchall()
                conn.close()
                embed = discord.Embed(title="🏆 DMPランキング TOP10", color=0xf1c40f)
                if not rows:
                    embed.description = "まだデータがありません。"
                else:
                    desc = ""
                    for i, row in enumerate(rows, start=1):
                        user = guild.get_member(row[0]) or bot.get_user(row[0])
                        name = user.display_name if user else f"User {row[0]}"
                        total = row[2] + row[3] + row[4]
                        desc += f"**{i}.** {name}  `{row[1]} DMP`  ({row[2]}勝{row[3]}敗{row[4]}分 / {total}戦)\n"
                    embed.description = desc
                async for msg in channel.history(limit=1):
                    await msg.delete()
                await channel.send(embed=embed)
        except Exception as e:
            print(f"Leaderboard update error: {e}")
        await asyncio.sleep(600)

# ---------- キュー情報更新（最適化）----------
async def queue_info_updater():
    await bot.wait_until_ready()
    last_message_ids = {}
    while not bot.is_closed():
        try:
            for guild in bot.guilds:
                channel = discord.utils.get(guild.text_channels, name=MATCH_LOBBY_CHANNEL)
                if channel is None:
                    continue
                now = datetime.now()
                expired_users = []
                for entry in match_queue:
                    if entry['expire_at'] and entry['expire_at'] < now:
                        expired_users.append(entry)
                for entry in expired_users:
                    user = guild.get_member(entry['user_id']) or bot.get_user(entry['user_id'])
                    if user:
                        try:
                            await user.send(f"⏰ 対戦待機時間（{entry['condition']}分）が経過したため、自動的に募集をキャンセルしました。")
                        except:
                            pass
                    remove_from_queue_db(entry['user_id'])
                    await delete_queue_message(entry['user_id'])

                conn = sqlite3.connect(DB_FILE)
                rows = conn.execute('SELECT user_id, user_name, condition, created_at FROM match_queue ORDER BY created_at ASC').fetchall()
                conn.close()
                embed = discord.Embed(title="🔄 現在の対戦待機キュー", color=0x00ff00)
                if not rows:
                    embed.description = "待機中のプレイヤーはいません。"
                else:
                    desc = ""
                    for row in rows:
                        user = guild.get_member(row[0]) or bot.get_user(row[0])
                        name = user.display_name if user else row[1]
                        desc += f"**{name}** : {row[2]}分待機 (登録: {row[3]})\n"
                    embed.description = desc
                # 編集を試みる
                if guild.id in last_message_ids:
                    try:
                        msg = await channel.fetch_message(last_message_ids[guild.id])
                        await msg.edit(embed=embed)
                        continue
                    except:
                        pass
                # 新規投稿
                msg = await channel.send(embed=embed)
                last_message_ids[guild.id] = msg.id
        except Exception as e:
            print(f"Queue info update error: {e}")
        await asyncio.sleep(30)

# ---------- 自動シーズンリセット（3ヶ月ごと） ----------
async def season_scheduler():
    await bot.wait_until_ready()
    while not bot.is_closed():
        try:
            last_reset_str = get_setting('last_season_reset')
            if last_reset_str:
                last_reset = datetime.strptime(last_reset_str, '%Y-%m-%d')
                if (datetime.now() - last_reset).days >= 90:
                    for guild in bot.guilds:
                        await change_season(guild)
                    set_setting('last_season_reset', datetime.now().strftime('%Y-%m-%d'))
        except Exception as e:
            print(f"Season scheduler error: {e}")
        await asyncio.sleep(3600)

async def change_season(guild: discord.Guild):
    current_season = int(get_setting('current_season'))
    soft_reset_ratio = float(get_setting('soft_reset_ratio'))
    conn = sqlite3.connect(DB_FILE)
    mvp_row = conn.execute('SELECT discord_id, dmp, wins FROM users ORDER BY dmp DESC, wins DESC LIMIT 1').fetchone()
    if mvp_row:
        mvp_user = guild.get_member(mvp_row[0])
        if mvp_user:
            role_name = f"Season {current_season} MVP"
            mvp_role = discord.utils.get(guild.roles, name=role_name)
            if mvp_role is None:
                mvp_role = await guild.create_role(name=role_name, color=discord.Color.gold(), hoist=True)
            await mvp_user.add_roles(mvp_role)
            news_channel = discord.utils.get(guild.text_channels, name=INFO_NEWS_CHANNEL)
            if news_channel:
                await news_channel.send(
                    f"🏆 **シーズン {current_season} 終了！**\n"
                    f"MVP: {mvp_user.mention} (最終DMP: {mvp_row[1]}, 勝利数: {mvp_row[2]})\n"
                    f"全プレイヤーのDMPがソフトリセットされました。（次シーズン初期DMP = 1500 + (旧DMP - 1500) × {soft_reset_ratio}）"
                )
    conn.execute('UPDATE users SET dmp = 1500 + (dmp - 1500) * ?', (soft_reset_ratio,))
    conn.commit()
    conn.close()
    set_setting('current_season', str(current_season + 1))

# ---------- 指名マッチ自動開放スケジューラ ----------
JST = timezone(timedelta(hours=9))

DESIGNATED_SESSIONS = [
    {"start": time(18, 0), "end": time(19, 0), "acceptance_end": time(19, 15)},
    {"start": time(20, 0), "end": time(21, 0), "acceptance_end": time(21, 15)},
    {"start": time(22, 0), "end": time(23, 0), "acceptance_end": time(23, 15)},
]

async def designated_match_scheduler():
    await bot.wait_until_ready()
    last_state = {}
    while not bot.is_closed():
        try:
            now_jst = datetime.now(JST)
            weekday = now_jst.weekday()
            is_weekend = weekday in (4, 5, 6)

            for guild in bot.guilds:
                channel = discord.utils.get(guild.text_channels, name=DESIGNATED_MATCH_CHANNEL)
                if channel is None:
                    continue

                current_phase = None
                if is_weekend:
                    for sess in DESIGNATED_SESSIONS:
                        if sess["start"] <= now_jst.time() < sess["end"]:
                            current_phase = "recruitment"
                            break
                        elif sess["end"] <= now_jst.time() < sess["acceptance_end"]:
                            current_phase = "acceptance"
                            break

                last = last_state.get(guild.id)
                if last == current_phase:
                    continue

                if last == "recruitment" and current_phase != "recruitment":
                    await process_designated_queue(guild, channel)
                elif current_phase == "recruitment":
                    await channel.set_permissions(guild.default_role, send_messages=True)
                    await channel.send("⚔️ **週末マルチモード開放！** `/call` でエントリーしてください。8人まで自動振り分け。")
                elif current_phase == "acceptance":
                    await channel.set_permissions(guild.default_role, send_messages=False)
                    if last == "recruitment":
                        await channel.send("🔒 エントリー終了。マッチングを開始します。")
                else:
                    await channel.set_permissions(guild.default_role, send_messages=False)

                last_state[guild.id] = current_phase
        except Exception as e:
            print(f"Designated scheduler error: {e}")
        await asyncio.sleep(30)

async def process_designated_queue(guild: discord.Guild, channel: discord.TextChannel):
    queue = get_designated_queue()
    if not queue:
        return
    random.shuffle(queue)
    groups = [queue[i:i+8] for i in range(0, len(queue), 8)]
    for group in groups:
        if len(group) == 1:
            user = guild.get_member(group[0]['user_id'])
            if user:
                await channel.send(f"❌ {user.mention} 参加者が1人しかいないため、マッチはキャンセルされました。")
            remove_from_designated_queue(group[0]['user_id'])
        else:
            await create_multiplayer_room(guild, channel, group)
    clear_designated_queue()

async def create_multiplayer_room(guild: discord.Guild, channel: discord.TextChannel, participants: list):
    num = len(participants)
    room_id = generate_room_id()
    overwrites = {
        guild.default_role: discord.PermissionOverwrite(view_channel=False, send_messages=False, connect=False),
        guild.me: discord.PermissionOverwrite(view_channel=True, send_messages=True, connect=True)
    }
    player_ids = []
    for p in participants:
        member = guild.get_member(p['user_id'])
        if member:
            overwrites[member] = discord.PermissionOverwrite(view_channel=True, send_messages=True, connect=True)
            player_ids.append(member)

    category = discord.utils.get(guild.categories, name="対戦中部屋")
    if category is None:
        category = await guild.create_category("対戦中部屋")
    text_ch = await guild.create_text_channel(name=f"room-{room_id}", category=category, overwrites=overwrites)
    voice_ch = await guild.create_voice_channel(name=f"🔊 room-{room_id}", category=category, overwrites=overwrites)

    mode = get_game_mode(num)
    if mode == "vote":
        view = GameModeVoteView(num, player_ids, room_id, text_ch, voice_ch)
        await text_ch.send(f"参加者: {', '.join([m.mention for m in player_ids])}\nモードを投票で決定してください。下のボタンから選んでください。", view=view)
        return
    else:
        await text_ch.send(f"モード: {mode}\n参加者: {', '.join([m.mention for m in player_ids])}\n試合を開始してください！")
        if "v" in mode:
            teams = mode.split("v")
            team_size = int(teams[0])
            team_list = [player_ids[i:i+team_size] for i in range(0, num, team_size)]
            msg = "**チーム分け**\n"
            for idx, team in enumerate(team_list, 1):
                msg += f"チーム{idx}: {', '.join([m.mention for m in team])}\n"
            await text_ch.send(msg)

def get_game_mode(num):
    if num == 1: return None
    if num == 2: return "1v1"
    if num == 3: return "個人戦"
    if num == 4: return "vote"
    if num == 5: return "個人戦"
    if num == 6: return "vote"
    if num == 7: return "個人戦"
    if num == 8: return "vote"
    return "個人戦"

class GameModeVoteView(discord.ui.View):
    def __init__(self, num, player_ids, room_id, text_ch, voice_ch):
        super().__init__(timeout=300)
        self.num = num
        self.player_ids = player_ids
        self.room_id = room_id
        self.text_ch = text_ch
        self.voice_ch = voice_ch
        self.votes = {}
        self.options = self._get_options()
        for opt in self.options:
            self.add_item(GameModeButton(opt, self))

    def _get_options(self):
        n = self.num
        if n == 4:
            return ["個人戦", "2v2"]
        elif n == 6:
            return ["個人戦", "2v2v2", "3v3"]
        elif n == 8:
            return ["個人戦", "2v2v2v2", "4v4"]
        return []

    async def on_timeout(self):
        if not self.votes:
            mode = self.options[0]
        else:
            mode = max(set(self.votes.values()), key=list(self.votes.values()).count)
        await self.text_ch.send(f"投票タイムアウト。最も多かったモード **{mode}** に決定しました。")
        await self.finalize(mode)

    async def finalize(self, mode):
        player_mentions = [m.mention for m in self.player_ids]
        await self.text_ch.send(f"モード: {mode}\n参加者: {', '.join(player_mentions)}\n試合を開始してください！")
        if "v" in mode:
            teams = mode.split("v")
            team_size = int(teams[0])
            team_list = [self.player_ids[i:i+team_size] for i in range(0, len(self.player_ids), team_size)]
            msg = "**チーム分け**\n"
            for idx, team in enumerate(team_list, 1):
                msg += f"チーム{idx}: {', '.join([m.mention for m in team])}\n"
            await self.text_ch.send(msg)

class GameModeButton(discord.ui.Button):
    def __init__(self, mode, parent_view):
        super().__init__(label=mode, style=discord.ButtonStyle.primary)
        self.mode = mode
        self.parent_view = parent_view

    async def callback(self, interaction: discord.Interaction):
        if interaction.user.id not in [p.id for p in self.parent_view.player_ids]:
            await interaction.response.send_message("あなたは投票権がありません。", ephemeral=True)
            return
        self.parent_view.votes[interaction.user.id] = self.mode
        await interaction.response.send_message(f"**{self.mode}** に投票しました！", ephemeral=True)
        if len(self.parent_view.votes) == len(self.parent_view.player_ids):
            votes = list(self.parent_view.votes.values())
            mode = max(set(votes), key=votes.count)
            await self.parent_view.text_ch.send(f"全員投票完了。モード **{mode}** に決定しました。")
            await self.parent_view.finalize(mode)
            self.parent_view.stop()

async def fire_result_confirmed_event(match_info, submitter_id, opponent_id, score_a, score_b, condition, guild, channel, designated_match=False):
    if designated_match:
        return None
    total_score = score_a + score_b
    total_races = 3 if total_score == 54 else (6 if total_score == 108 else (12 if total_score == 216 else 3))
    result = await rating_service(submitter_id, opponent_id, score_a, score_b, total_races, condition,
                                  match_info['room_id'], match_info['player1_id'], match_info['player2_id'], guild,
                                  designated_match=designated_match)
    await record_service(submitter_id, opponent_id, result['result_a'], result['result_b'], guild)
    embed = discord.Embed(title="⚔️ 対戦結果", color=0x00ff00 if result['winner_id'] else 0xffaa00)
    if result['winner_id'] is None:
        embed.add_field(name="結果", value="引き分け 🤝", inline=False)
    else:
        w_name = f"<@{submitter_id}>" if result['winner_id'] == submitter_id else f"<@{opponent_id}>"
        l_name = f"<@{opponent_id}>" if result['winner_id'] == submitter_id else f"<@{submitter_id}>"
        w_change = result['change_a'] if result['winner_id'] == submitter_id else result['change_b']
        l_change = result['change_b'] if result['winner_id'] == submitter_id else result['change_a']
        embed.add_field(name="勝者", value=f"{w_name} ({w_change:+d})", inline=True)
        embed.add_field(name="敗者", value=f"{l_name} ({l_change:+d})", inline=True)
    embed.add_field(name="スコア", value=f"{score_a} - {score_b}", inline=False)
    embed.add_field(name=f"<@{submitter_id}> のDMP", value=f"{get_user_stats(submitter_id)['dmp'] - result['change_a']} → {get_user_stats(submitter_id)['dmp']}", inline=True)
    embed.add_field(name=f"<@{opponent_id}> のDMP", value=f"{get_user_stats(opponent_id)['dmp'] - result['change_b']} → {get_user_stats(opponent_id)['dmp']}", inline=True)
    await post_news(guild, embed)
    await update_bounty_board(guild)
    return embed

# ---------- UI ----------
class ConfirmView(discord.ui.View):
    def __init__(self, match_info, opponent_id, channel):
        super().__init__(timeout=3600)
        self.match_info = match_info
        self.opponent_id = opponent_id
        self.channel = channel

    async def on_timeout(self):
        if self.channel and self.match_info['status'] == 'SCORE_SUBMITTED':
            self.match_info['status'] = 'TIMEOUT'
            await self.channel.set_permissions(self.channel.guild.default_role, send_messages=False)
            for pid in (self.match_info['player1_id'], self.match_info['player2_id']):
                member = self.channel.guild.get_member(pid)
                if member:
                    await self.channel.set_permissions(member, send_messages=False)
            await self.channel.send("⏰ 同意のタイムアウトにより、部屋がロックされました。")
            admin_channel = discord.utils.get(self.channel.guild.text_channels, name=ADMIN_CHANNEL)
            if admin_channel:
                await admin_channel.send(f"⚠️ room-{self.match_info['room_id']} がタイムアウトしました。")

    @discord.ui.button(label="同意して結果を確定する", style=discord.ButtonStyle.success)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.opponent_id:
            await interaction.response.send_message("あなたはこのボタンを押せません。", ephemeral=True)
            return
        match_info = self.match_info
        if match_info['status'] != 'SCORE_SUBMITTED':
            await interaction.response.send_message("既に処理済みです。", ephemeral=True)
            return
        match_info['status'] = 'CONFIRMED'
        self.stop()
        channel = interaction.channel
        await channel.set_permissions(interaction.guild.default_role, send_messages=False)
        for pid in (match_info['player1_id'], match_info['player2_id']):
            member = interaction.guild.get_member(pid)
            if member:
                await channel.set_permissions(member, send_messages=False)
        await interaction.response.send_message("🔒 確定しました。ログ退避中...", ephemeral=True)
        await archive_chat(channel, match_info)
        designated = channel.name == DESIGNATED_MATCH_CHANNEL
        embed = await fire_result_confirmed_event(
            match_info,
            match_info['submitter_id'],
            match_info['opponent_id'],
            match_info['submitter_score'],
            match_info['opponent_score'],
            match_info['condition'],
            interaction.guild,
            channel,
            designated_match=designated
        )
        if embed:
            await channel.send(embed=embed)
        await channel.send("この部屋は10秒後に削除されます。")
        await asyncio.sleep(10)
        category = channel.category
        voice_name = f"🔊 room-{match_info['room_id']}"
        for vc in category.voice_channels:
            if vc.name == voice_name:
                await vc.delete()
                break
        await channel.delete()
        if channel.id in active_matches:
            del active_matches[channel.id]
            delete_active_room(channel.id)

class ChallengeView(discord.ui.View):
    def __init__(self, challenger_id, opponent_id, condition):
        super().__init__(timeout=900)
        self.challenger_id = challenger_id
        self.opponent_id = opponent_id
        self.condition = condition

    @discord.ui.button(label="👍 挑戦を受ける", style=discord.ButtonStyle.success)
    async def accept(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.opponent_id:
            await interaction.response.send_message("あなたはこのボタンを押せません。", ephemeral=True)
            return
        self.stop()
        guild = interaction.guild
        challenger = guild.get_member(self.challenger_id)
        opponent = guild.get_member(self.opponent_id)
        if not challenger or not opponent:
            await interaction.response.send_message("相手が見つかりません。", ephemeral=True)
            return
        room_id = generate_room_id()
        overwrites = {
            guild.default_role: discord.PermissionOverwrite(view_channel=False, send_messages=False, connect=False),
            challenger: discord.PermissionOverwrite(view_channel=True, send_messages=True, connect=True),
            opponent: discord.PermissionOverwrite(view_channel=True, send_messages=True, connect=True),
            guild.me: discord.PermissionOverwrite(view_channel=True, send_messages=True, connect=True)
        }
        category = discord.utils.get(guild.categories, name="対戦中部屋")
        if category is None:
            category = await guild.create_category("対戦中部屋")
        text_ch = await guild.create_text_channel(name=f"room-{room_id}", category=category, overwrites=overwrites)
        voice_ch = await guild.create_voice_channel(name=f"🔊 room-{room_id}", category=category, overwrites=overwrites)
        match_info = {
            'room_id': room_id, 'player1_id': challenger.id, 'player2_id': opponent.id,
            'condition': self.condition, 'status': 'ACTIVE', 'submitter_id': None,
            'submitter_score': 0, 'opponent_score': 0, 'opponent_id': None
        }
        active_matches[text_ch.id] = match_info
        save_active_room(text_ch.id, match_info)
        await text_ch.send(
            f"⚔️ **指名対戦成立！** ⚔️\n{challenger.mention} vs {opponent.mention}\n"
            f"募集時間: {self.condition}分\n"
            f"試合が終わったら `/result` でスコア報告。\n"
            f"※ スコア合計は 54(3レース), 108(6), 216(12) のいずれか"
        )
        await interaction.response.send_message(f"✅ 挑戦を受けました！ {text_ch.mention} / {voice_ch.mention}")

    @discord.ui.button(label="❌ 断る", style=discord.ButtonStyle.danger)
    async def deny(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.opponent_id:
            await interaction.response.send_message("あなたはこのボタンを押せません。", ephemeral=True)
            return
        self.stop()
        await interaction.response.send_message("挑戦を断りました。", ephemeral=True)

    async def on_timeout(self):
        pass

# ---------- コマンド ----------
@bot.event
async def on_ready():
    print(f'🤖 {bot.user} としてログインしました！')
    if not bot.first_ready:
        return  # 再起動時の重複処理を防止
    bot.first_ready = False

    for guild in bot.guilds:
        await ensure_roles(guild)

        old_queue_channel = discord.utils.get(guild.text_channels, name="queue-information")
        if old_queue_channel:
            try:
                await old_queue_channel.delete()
                print(f"古い #queue-information を {guild.name} から削除しました。")
            except Exception as e:
                print(f"#queue-information の削除に失敗: {e}")

        admin_category = discord.utils.get(guild.categories, name=CHAT_STORAGE_CATEGORY)
        if admin_category is None:
            admin_role = discord.utils.get(guild.roles, permissions=discord.Permissions(administrator=True))
            overwrites_cat = {
                guild.default_role: discord.PermissionOverwrite(view_channel=False),
                guild.me: discord.PermissionOverwrite(view_channel=True)
            }
            if admin_role:
                overwrites_cat[admin_role] = discord.PermissionOverwrite(view_channel=True)
            admin_category = await guild.create_category(CHAT_STORAGE_CATEGORY, overwrites=overwrites_cat)

        channels_to_create = {
            CHAT_STORAGE_CHANNEL: {'category': admin_category, 'perms': {guild.default_role: discord.PermissionOverwrite(view_channel=False, send_messages=False), guild.me: discord.PermissionOverwrite(view_channel=True, send_messages=True)}},
            INFO_NEWS_CHANNEL: {'perms': {guild.default_role: discord.PermissionOverwrite(view_channel=True, send_messages=False), guild.me: discord.PermissionOverwrite(send_messages=True)}},
            BOUNTY_CHANNEL: {'perms': {guild.default_role: discord.PermissionOverwrite(view_channel=True, send_messages=False), guild.me: discord.PermissionOverwrite(send_messages=True)}},
            SYSTEM_LOG_CHANNEL: {'perms': {guild.default_role: discord.PermissionOverwrite(view_channel=False), guild.me: discord.PermissionOverwrite(view_channel=True, send_messages=True)}},
            RANK_CHANNEL: {'perms': {guild.default_role: discord.PermissionOverwrite(view_channel=True, send_messages=False), guild.me: discord.PermissionOverwrite(send_messages=True)}},
            ADMIN_CHANNEL: {'perms': {guild.default_role: discord.PermissionOverwrite(view_channel=False), guild.me: discord.PermissionOverwrite(view_channel=True, send_messages=True)}},
            MATCH_LOBBY_CHANNEL: {'perms': {guild.default_role: discord.PermissionOverwrite(view_channel=True, send_messages=True)}},
            DESIGNATED_MATCH_CHANNEL: {'perms': {guild.default_role: discord.PermissionOverwrite(view_channel=True, send_messages=False), guild.me: discord.PermissionOverwrite(send_messages=True)}},
            PICTURE_READ_CHANNEL: {'category': admin_category, 'perms': {guild.default_role: discord.PermissionOverwrite(view_channel=False), guild.me: discord.PermissionOverwrite(view_channel=True, send_messages=True)}},
            RULES_CHANNEL: {'perms': {guild.default_role: discord.PermissionOverwrite(view_channel=True, send_messages=False), guild.me: discord.PermissionOverwrite(send_messages=True)}},
            WELCOME_CHANNEL: {'perms': {guild.default_role: discord.PermissionOverwrite(view_channel=False), guild.me: discord.PermissionOverwrite(view_channel=True, send_messages=True)}},
        }
        for name, config in channels_to_create.items():
            cat = config.get('category')
            perms = config['perms']
            if not discord.utils.get(guild.text_channels, name=name):
                try:
                    if cat:
                        await cat.create_text_channel(name, overwrites=perms)
                    else:
                        await guild.create_text_channel(name, overwrites=perms)
                    await asyncio.sleep(0.5)  # レート制限緩和
                except Exception as e:
                    print(f"チャンネル {name} 作成エラー: {e}")

        welcome_ch = discord.utils.get(guild.text_channels, name=WELCOME_CHANNEL)
        if welcome_ch:
            unverified_role = discord.utils.get(guild.roles, name="未認証")
            if unverified_role:
                await welcome_ch.set_permissions(guild.default_role, view_channel=False)
                await welcome_ch.set_permissions(unverified_role, view_channel=True)
            pins = await welcome_ch.pins()
            if not pins:
                embed = discord.Embed(title="👋 ラウンジへようこそ！", description="サーバーに参加するには、BotからのDMに従ってニックネームを登録してください。")
                embed.add_field(name="手順", value="1. 左のメンバーリストから **LoungeBot** を見つけてDMを開きます。\n2. `/nickname 希望する名前` を実行します。\n3. 認証完了！すべてのチャンネルが表示されます。")
                await welcome_ch.send(embed=embed)

        rules_channel = discord.utils.get(guild.text_channels, name=RULES_CHANNEL)
        if rules_channel:
            pins = await rules_channel.pins()
            if not pins:
                embed = discord.Embed(title="🏁 ラウンジ ルール", color=0x3498db)
                embed.add_field(name="📅 対戦形式（平日/ロビー）", value=(
                    "・1試合は **3レース / 6レース / 12レース** のいずれか\n"
                    "・勝者は1レース10点、敗者は8点\n"
                    "・スコア合計は **54点(3R), 108点(6R), 216点(12R)** となります\n"
                    "・募集時間は `/call` の `condition` に分単位で指定（例：60分）\n"
                    "・指定時間が経過すると自動で募集がキャンセルされます"
                ), inline=False)
                embed.add_field(name="🤝 マッチング", value=(
                    "・`/call` は必ず **#match-lobby** で実行してください\n"
                    "・同じ募集時間の相手がいれば自動で対戦部屋が作られます\n"
                    "・`/call opponent:@ユーザー` で指名対戦も可能です\n"
                    "・**週末のゴールデンタイム** には #designated-match で最大8人マルチモードが出現！"
                ), inline=False)
                embed.add_field(name="🎮 週末マルチモード (#designated-match)", value=(
                    "・金土日 18:00〜23:15 の間、#designated-match が解放されます\n"
                    "・`/call` するだけで参加登録、8人まで自動振り分け\n"
                    "・人数に応じて自動で個人戦・チーム戦を決定、4人以上は投票でルールを決めます\n"
                    "・このモードでは **DMPは変動しません**（純粋なバトル）"
                ), inline=False)
                embed.add_field(name="📊 レーティング (DMP)", value=(
                    "・初期値1500、平日の対戦ごとに変動\n"
                    "・5連勝で賞金首（Wanted）になり、討伐されるとボーナスが発生\n"
                    "・指名マッチではDMP変動が1.2倍になります（増加分はプールから支給）\n"
                    "・シーズン制（約3ヶ月）で自動リセット＆MVPロール付与"
                ), inline=False)
                embed.add_field(name="🚫 禁止事項", value=(
                    "・暴言、差別、不快な発言はフィルターで自動削除＆ペナルティ\n"
                    "・不正なスコア報告（合計が不適切）はシステムが拒否\n"
                    "・対戦部屋での `/call` は利用できません"
                ), inline=False)
                embed.set_footer(text="本格的な戦いを楽しみましょう！")
                await rules_channel.send(embed=embed)
                msg = await rules_channel.send(".")
                await msg.delete()
                rules_msg = await rules_channel.history(limit=1).flatten()
                if rules_msg:
                    await rules_msg[0].pin()

@bot.event
async def on_message(message: discord.Message):
    if message.author.bot:
        return
    if message.channel.name == LOUNGE_TALK_CHANNEL:
        content_lower = message.content.lower()
        for ng in NG_WORDS:
            if ng.lower() in content_lower:
                await message.delete()
                await message.channel.send(f"{message.author.mention} 不適切な発言が検出されました。", delete_after=5)
                conn = sqlite3.connect(DB_FILE)
                conn.execute('UPDATE users SET penalty_points = penalty_points + 1 WHERE discord_id = ?', (message.author.id,))
                conn.commit()
                stats = get_user_stats(message.author.id)
                if stats['penalty_points'] >= 3:
                    muted_role = discord.utils.get(message.guild.roles, name="Muted")
                    if muted_role is None:
                        muted_role = await message.guild.create_role(name="Muted")
                        for ch in message.guild.text_channels:
                            await ch.set_permissions(muted_role, send_messages=False)
                    await message.author.add_roles(muted_role)
                    await message.channel.send(f"{message.author.mention} ペナルティが累積し、ミュートされました。")
                conn.close()
                break

@bot.tree.command(name="promote", description="副管理者ロールを付与（管理者専用）")
@app_commands.describe(target="副管理者に任命するユーザー")
async def promote(interaction: discord.Interaction, target: discord.Member):
    if not interaction.user.guild_permissions.administrator:
        admin_role = discord.utils.get(interaction.guild.roles, name="管理者")
        if admin_role is None or admin_role not in interaction.user.roles:
            await interaction.response.send_message("権限がありません。", ephemeral=True)
            return
    sub_admin_role = discord.utils.get(interaction.guild.roles, name="副管理者")
    if sub_admin_role is None:
        sub_admin_role = await interaction.guild.create_role(name="副管理者", permissions=discord.Permissions(administrator=True))
    if sub_admin_role in target.roles:
        await interaction.response.send_message(f"{target.mention} は既に副管理者です。", ephemeral=True)
    else:
        await target.add_roles(sub_admin_role)
        await interaction.response.send_message(f"{target.mention} を副管理者に任命しました。")

@bot.tree.command(name="demote", description="副管理者ロールを剥奪（管理者専用）")
@app_commands.describe(target="降格させるユーザー")
async def demote(interaction: discord.Interaction, target: discord.Member):
    if not interaction.user.guild_permissions.administrator:
        admin_role = discord.utils.get(interaction.guild.roles, name="管理者")
        if admin_role is None or admin_role not in interaction.user.roles:
            await interaction.response.send_message("権限がありません。", ephemeral=True)
            return
    sub_admin_role = discord.utils.get(interaction.guild.roles, name="副管理者")
    if sub_admin_role is None or sub_admin_role not in target.roles:
        await interaction.response.send_message(f"{target.mention} は副管理者ではありません。", ephemeral=True)
    else:
        await target.remove_roles(sub_admin_role)
        await interaction.response.send_message(f"{target.mention} を降格しました。")

@bot.tree.command(name="season_change", description="シーズンを手動で切り替え（管理者専用）")
async def season_change(interaction: discord.Interaction):
    if not interaction.user.guild_permissions.administrator:
        admin_role = discord.utils.get(interaction.guild.roles, name="管理者")
        if admin_role is None or admin_role not in interaction.user.roles:
            await interaction.response.send_message("権限がありません。", ephemeral=True)
            return
    await interaction.response.defer(ephemeral=True)
    try:
        await change_season(interaction.guild)
        await interaction.followup.send("✅ シーズンが切り替わりました。MVPロールが付与され、DMPがソフトリセットされました。")
    except Exception as e:
        await interaction.followup.send(f"エラー: {e}")

@bot.tree.command(name="ping", description="動作確認")
async def ping(interaction: discord.Interaction):
    await interaction.response.send_message("Pong!")

@bot.tree.command(name="leave", description="対戦待ちをキャンセルする")
async def leave(interaction: discord.Interaction):
    user_id = interaction.user.id
    for entry in match_queue:
        if entry['user_id'] == user_id:
            await delete_queue_message(user_id)
            remove_from_queue_db(user_id)
            await interaction.response.send_message("✅ 対戦待ちをキャンセルしました。", ephemeral=True)
            return
    designated = get_designated_queue()
    for entry in designated:
        if entry['user_id'] == user_id:
            remove_from_designated_queue(user_id)
            await interaction.response.send_message("✅ 週末マルチの参加をキャンセルしました。", ephemeral=True)
            return
    await interaction.response.send_message("⚠️ 現在対戦待ちをしていません。", ephemeral=True)

@bot.tree.command(name="call", description="対戦相手を募集する（#match-lobby または #designated-match で使用可能）")
@app_commands.describe(condition="募集時間（分）", opponent="指名したい相手（省略可）")
async def call(interaction: discord.Interaction, condition: int, opponent: discord.Member = None):
    user = interaction.user
    channel = interaction.channel

    if channel.name == DESIGNATED_MATCH_CHANNEL:
        now_jst = datetime.now(JST)
        is_weekend = now_jst.weekday() in (4, 5, 6)
        if not is_weekend:
            await interaction.response.send_message("⚠️ 週末マルチモードは金土日のみ開放されます。", ephemeral=True)
            return
        in_recruitment = False
        for sess in DESIGNATED_SESSIONS:
            if sess["start"] <= now_jst.time() < sess["end"]:
                in_recruitment = True
                break
        if not in_recruitment:
            await interaction.response.send_message("⚠️ 現在はエントリー受付時間ではありません。", ephemeral=True)
            return
        designated = get_designated_queue()
        if any(e['user_id'] == user.id for e in designated):
            await interaction.response.send_message("⚠️ すでにエントリー済みです。", ephemeral=True)
            return
        add_to_designated_queue(user.id, user.display_name)
        await interaction.response.send_message(f"✅ 週末マルチにエントリーしました！（現在の参加者: {len(designated)+1}人）", ephemeral=True)
        return

    if channel.name != MATCH_LOBBY_CHANNEL:
        await interaction.response.send_message(f"⚠️ このコマンドは {MATCH_LOBBY_CHANNEL} または週末の {DESIGNATED_MATCH_CHANNEL} でのみ使用できます。", ephemeral=True)
        return

    if is_match_room(channel):
        await interaction.response.send_message("⚠️ このチャンネルは対戦部屋です。", ephemeral=True)
        return

    if opponent is not None:
        if opponent.id == user.id or opponent.bot:
            await interaction.response.send_message("⚠️ その相手は指名できません。", ephemeral=True)
            return
        for entry in match_queue:
            if entry['user_id'] == opponent.id:
                await interaction.response.send_message("⚠️ 相手はキュー待ちのため指名できません。", ephemeral=True)
                return
        for match in active_matches.values():
            if opponent.id in (match['player1_id'], match['player2_id']):
                await interaction.response.send_message("⚠️ 相手は対戦中のため指名できません。", ephemeral=True)
                return
        view = ChallengeView(user.id, opponent.id, condition)
        await interaction.response.send_message(
            f"⚔️ {user.mention} が {opponent.mention} に指名対戦を申し込みました！\n"
            f"条件: {condition}分\n返答期限: 15分", view=view
        )
        return

    for entry in match_queue:
        if entry['user_id'] == user.id:
            await interaction.response.send_message("⚠️ 既に別の条件で待機中です。`/leave` でキャンセルしてください。", ephemeral=True)
            return
    opponent_entry = None
    for entry in match_queue:
        if entry['condition'] == condition and entry['user_id'] != user.id:
            opponent_entry = entry
            break
    if opponent_entry:
        await delete_queue_message(opponent_entry['user_id'])
        remove_from_queue_db(opponent_entry['user_id'])
        player2 = bot.get_user(opponent_entry['user_id']) or await bot.fetch_user(opponent_entry['user_id'])
        if not player2:
            await interaction.response.send_message("❌ 相手情報を取得できませんでした。", ephemeral=True)
            return
        room_id = generate_room_id()
        guild = interaction.guild
        overwrites = {
            guild.default_role: discord.PermissionOverwrite(view_channel=False, send_messages=False, connect=False),
            user: discord.PermissionOverwrite(view_channel=True, send_messages=True, connect=True),
            player2: discord.PermissionOverwrite(view_channel=True, send_messages=True, connect=True),
            guild.me: discord.PermissionOverwrite(view_channel=True, send_messages=True, connect=True)
        }
        category = discord.utils.get(guild.categories, name="対戦中部屋")
        if category is None:
            category = await guild.create_category("対戦中部屋")
        text_ch = await guild.create_text_channel(name=f"room-{room_id}", category=category, overwrites=overwrites)
        voice_ch = await guild.create_voice_channel(name=f"🔊 room-{room_id}", category=category, overwrites=overwrites)
        match_info = {
            'room_id': room_id, 'player1_id': user.id, 'player2_id': player2.id,
            'condition': condition, 'status': 'ACTIVE', 'submitter_id': None,
            'submitter_score': 0, 'opponent_score': 0, 'opponent_id': None
        }
        active_matches[text_ch.id] = match_info
        save_active_room(text_ch.id, match_info)
        await text_ch.send(
            f"⚔️ **対戦成立！** ⚔️\n{user.mention} vs {player2.mention}\n"
            f"募集時間: {condition}分\n"
            f"スコア合計は 54(3レース), 108(6), 216(12) のいずれかで報告してください。"
        )
        await interaction.response.send_message(
            f"✅ マッチング成立！ {user.mention} vs {player2.mention}\n{text_ch.mention} / {voice_ch.mention}"
        )
    else:
        add_to_queue_db(user.id, user.display_name, condition)
        await interaction.response.send_message(
            f"🟢 {user.mention} が [募集時間: {condition}分] で対戦相手を募集中です！"
        )
        msg = await interaction.original_response()
        active_queue_messages[user.id] = (interaction.channel_id, msg.id)

@bot.tree.command(name="result", description="試合結果を報告する（対戦部屋の中で実行）")
@app_commands.describe(your_score="自分のスコア", opponent_score="相手のスコア")
async def result(interaction: discord.Interaction, your_score: int, opponent_score: int):
    channel = interaction.channel
    user = interaction.user
    if channel.id not in active_matches:
        await interaction.response.send_message("このチャンネルは対戦部屋ではありません。", ephemeral=True)
        return
    match_info = active_matches[channel.id]
    if user.id not in (match_info['player1_id'], match_info['player2_id']):
        await interaction.response.send_message("あなたはこの試合の参加者ではありません。", ephemeral=True)
        return
    if match_info['status'] != 'ACTIVE':
        await interaction.response.send_message("この試合は報告を受け付けていません。", ephemeral=True)
        return
    total = your_score + opponent_score
    if total not in (54, 108, 216):
        await interaction.response.send_message(
            f"⚠️ スコア合計が {total} です。54(3レース), 108(6), 216(12) のいずれかにしてください。", ephemeral=True)
        return
    opponent_id = match_info['player2_id'] if user.id == match_info['player1_id'] else match_info['player1_id']
    match_info.update({
        'status': 'SCORE_SUBMITTED', 'submitter_id': user.id,
        'submitter_score': your_score, 'opponent_score': opponent_score,
        'opponent_id': opponent_id
    })
    save_active_room(channel.id, match_info)
    view = ConfirmView(match_info, opponent_id, channel)
    await interaction.response.send_message(
        f"{user.mention} が結果を報告しました。スコア: {your_score} - {opponent_score}\n"
        f"<@{opponent_id}> さん、下のボタンで同意してください。（60分間有効）",
        view=view
    )

@bot.tree.command(name="dc", description="回線切断・トラブルを報告する（対戦部屋の中で実行）")
@app_commands.describe(description="状況の説明")
async def dc(interaction: discord.Interaction, description: str):
    channel = interaction.channel
    user = interaction.user
    if channel.id not in active_matches:
        await interaction.response.send_message("このチャンネルは対戦部屋ではありません。", ephemeral=True)
        return
    admin_channel = discord.utils.get(interaction.guild.text_channels, name=ADMIN_CHANNEL)
    if admin_channel:
        await admin_channel.send(f"⚠️ 切断報告: {user.mention} が room-{active_matches[channel.id]['room_id']} でトラブル発生\n内容: {description}")
    await interaction.response.send_message("運営にトラブルを報告しました。", ephemeral=True)

@bot.tree.command(name="close", description="現在の対戦部屋を削除する")
async def close(interaction: discord.Interaction):
    channel = interaction.channel
    user = interaction.user
    if channel.id in active_matches:
        match_info = active_matches[channel.id]
        if user.id not in (match_info['player1_id'], match_info['player2_id']):
            await interaction.response.send_message("あなたはこの部屋の参加者ではありません。", ephemeral=True)
            return
        await interaction.response.send_message("部屋を削除します...", ephemeral=True)
        voice_name = f"🔊 room-{match_info['room_id']}"
        for vc in channel.category.voice_channels:
            if vc.name == voice_name:
                await vc.delete()
                break
        del active_matches[channel.id]
        delete_active_room(channel.id)
        await channel.delete()
        return
    if channel.category and channel.category.name == "対戦中部屋" and re.match(r'^room-[A-Z0-9]{4}$', channel.name):
        room_id = channel.name.split('-')[1]
        voice_name = f"🔊 room-{room_id}"
        for vc in channel.category.voice_channels:
            if vc.name == voice_name:
                await vc.delete()
                break
        await interaction.response.send_message("部屋を削除します...", ephemeral=True)
        await channel.delete()
        return
    await interaction.response.send_message("このチャンネルは対戦部屋ではありません。", ephemeral=True)

@bot.tree.command(name="stats", description="自分の戦績を表示")
async def stats(interaction: discord.Interaction):
    s = get_user_stats(interaction.user.id)
    embed = discord.Embed(title=f"📊 {interaction.user.display_name} の戦績", color=0x3498db)
    embed.add_field(name="DMP", value=str(s['dmp']), inline=True)
    embed.add_field(name="勝ち / 負け / 分け", value=f"{s['wins']} / {s['losses']} / {s['draws']}", inline=True)
    embed.add_field(name="連勝", value=f"現在: {s['current_win_streak']} / 最大: {s['highest_win_streak']}", inline=True)
    if s['is_wanted']:
        embed.add_field(name="🌟 賞金首", value="現在賞金がかかっています！", inline=False)
    if s['current_title']:
        embed.add_field(name="🏅 称号", value=s['current_title'], inline=False)
    embed.set_footer(text=f"総試合数: {s['wins'] + s['losses'] + s['draws']}")
    await interaction.response.send_message(embed=embed)

@bot.tree.command(name="rank", description="DMPランキングを表示")
async def rank(interaction: discord.Interaction):
    conn = sqlite3.connect(DB_FILE)
    rows = conn.execute('SELECT discord_id, dmp, wins, losses, draws FROM users ORDER BY dmp DESC LIMIT 10').fetchall()
    conn.close()
    embed = discord.Embed(title="🏆 DMPランキング TOP10", color=0xf1c40f)
    if not rows:
        embed.description = "まだデータがありません。"
    else:
        desc = ""
        for i, row in enumerate(rows, start=1):
            user = bot.get_user(row[0])
            name = user.display_name if user else f"User {row[0]}"
            total = row[2] + row[3] + row[4]
            desc += f"**{i}.** {name}  `{row[1]} DMP`  ({row[2]}勝{row[3]}敗{row[4]}分 / {total}戦)\n"
        embed.description = desc
    await interaction.response.send_message(embed=embed)

# ---------- Bot 起動（Render対応）----------
from flask import Flask
from threading import Thread

app = Flask(__name__)

@app.route('/')
def home():
    return "LoungeBot is running!"

def run_web():
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))

if __name__ == '__main__':
    # まずFlaskを起動
    Thread(target=run_web).start()
    # Botをリトライ付きで起動
    asyncio.get_event_loop().run_until_complete(bot_login_with_retry())
