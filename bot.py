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
from datetime import datetime
from collections import defaultdict

load_dotenv()
TOKEN = os.getenv('DISCORD_TOKEN')

intents = discord.Intents.default()
intents.message_content = True

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
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
    cursor.execute('SELECT user_id, user_name, condition FROM match_queue ORDER BY created_at ASC')
    rows = cursor.fetchall()
    conn.close()
    return [{'user_id': row[0], 'user_name': row[1], 'condition': row[2]} for row in rows]

def add_to_queue_db(user_id: int, user_name: str, condition: int):
    global match_queue
    match_queue.append({'user_id': user_id, 'user_name': user_name, 'condition': condition})
    conn = sqlite3.connect(DB_FILE)
    conn.execute('INSERT OR IGNORE INTO match_queue (user_id, user_name, condition) VALUES (?, ?, ?)',
                 (user_id, user_name, condition))
    conn.commit()
    conn.close()

def remove_from_queue_db(user_id: int):
    global match_queue
    match_queue = [entry for entry in match_queue if entry['user_id'] != user_id]
    conn = sqlite3.connect(DB_FILE)
    conn.execute('DELETE FROM match_queue WHERE user_id = ?', (user_id,))
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
def calculate_dmp(player_a_dmp, player_b_dmp, score_a, score_b, total_races, player_a_streak=0):
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
    delta_a = round(K * weight * multiplier * (SA - expected_a))
    return delta_a, -delta_a

# ---------- Bot クラス ----------
class MyBot(discord.Client):
    def __init__(self):
        super().__init__(intents=intents)
        self.tree = app_commands.CommandTree(self)

    async def setup_hook(self):
        await self.tree.sync()
        print('✅ スラッシュコマンドをグローバル同期しました')
        self.bg_task = self.loop.create_task(leaderboard_updater())
        self.queue_info_task = self.loop.create_task(queue_info_updater())

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

# ---------- サービス ----------
async def rating_service(submitter_id, opponent_id, score_a, score_b, total_races, condition, room_id, player1_id, player2_id, interaction_guild):
    dmp_a = get_user_stats(submitter_id)['dmp']
    dmp_b = get_user_stats(opponent_id)['dmp']
    change_a, change_b = calculate_dmp(dmp_a, dmp_b, score_a, score_b, total_races, 
                                       player_a_streak=(-3 if get_user_stats(submitter_id)['losses'] >= 3 else 0))
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
    save_rating_history(submitter_id, match_id, dmp_a, new_dmp_a, change_a, expected_a, multiplier_val, weight_val, bonus_a)
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
                    user = bot.get_user(row[0])
                    name = user.display_name if user else f"User {row[0]}"
                    total = row[2] + row[3] + row[4]
                    desc += f"**{i}.** {name}  `{row[1]} DMP`  ({row[2]}勝{row[3]}敗{row[4]}分 / {total}戦)\n"
                embed.description = desc
            async for msg in channel.history(limit=1):
                await msg.delete()
            await channel.send(embed=embed)
        await asyncio.sleep(600)

# ---------- キュー情報更新（#match-lobby で編集表示） ----------
async def queue_info_updater():
    await bot.wait_until_ready()
    last_message_ids = {}
    while not bot.is_closed():
        for guild in bot.guilds:
            channel = discord.utils.get(guild.text_channels, name=MATCH_LOBBY_CHANNEL)
            if channel is None:
                continue
            conn = sqlite3.connect(DB_FILE)
            rows = conn.execute('SELECT user_id, user_name, condition, created_at FROM match_queue ORDER BY created_at ASC').fetchall()
            conn.close()
            embed = discord.Embed(title="🔄 現在の対戦待機キュー", color=0x00ff00)
            if not rows:
                embed.description = "待機中のプレイヤーはいません。"
            else:
                desc = ""
                for row in rows:
                    user = bot.get_user(row[0])
                    name = user.display_name if user else row[1]
                    desc += f"**{name}** : {row[2]}分待機 (登録: {row[3]})\n"
                embed.description = desc
            if guild.id in last_message_ids:
                try:
                    msg = await channel.fetch_message(last_message_ids[guild.id])
                    await msg.edit(embed=embed)
                    continue
                except:
                    pass
            msg = await channel.send(embed=embed)
            last_message_ids[guild.id] = msg.id
        await asyncio.sleep(30)

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

async def fire_result_confirmed_event(match_info, submitter_id, opponent_id, score_a, score_b, condition, guild, channel):
    total_score = score_a + score_b
    total_races = 3 if total_score == 54 else (6 if total_score == 108 else (12 if total_score == 216 else 3))
    result = await rating_service(submitter_id, opponent_id, score_a, score_b, total_races, condition,
                                  match_info['room_id'], match_info['player1_id'], match_info['player2_id'], guild)
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
        embed = await fire_result_confirmed_event(
            match_info,
            match_info['submitter_id'],
            match_info['opponent_id'],
            match_info['submitter_score'],
            match_info['opponent_score'],
            match_info['condition'],
            interaction.guild,
            channel
        )
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
        }
        for name, config in channels_to_create.items():
            cat = config.get('category')
            perms = config['perms']
            if not discord.utils.get(guild.text_channels, name=name):
                if cat:
                    await cat.create_text_channel(name, overwrites=perms)
                else:
                    await guild.create_text_channel(name, overwrites=perms)

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

@bot.tree.command(name="season_change", description="シーズンを切り替え、MVPを付与、DMPをソフトリセット（管理者専用）")
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
    await interaction.response.send_message("⚠️ 現在対戦待ちをしていません。", ephemeral=True)

@bot.tree.command(name="call", description="対戦相手を募集する（#match-lobby でのみ使用可能）")
@app_commands.describe(condition="募集時間（分）", opponent="指名したい相手（省略可）", debug="デバッグ用")
async def call(interaction: discord.Interaction, condition: int, opponent: discord.Member = None, debug: bool = False):
    user = interaction.user
    channel = interaction.channel

    if channel.name != MATCH_LOBBY_CHANNEL:
        await interaction.response.send_message(f"⚠️ このコマンドは {MATCH_LOBBY_CHANNEL} でのみ使用できます。", ephemeral=True)
        return

    if is_match_room(channel):
        await interaction.response.send_message("⚠️ このチャンネルは対戦部屋です。", ephemeral=True)
        return

    if debug:
        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message("サーバー内でのみ使用できます。", ephemeral=True)
            return
        room_id = generate_room_id()
        overwrites = {
            guild.default_role: discord.PermissionOverwrite(view_channel=False, send_messages=False, connect=False),
            user: discord.PermissionOverwrite(view_channel=True, send_messages=True, connect=True),
            guild.me: discord.PermissionOverwrite(view_channel=True, send_messages=True, connect=True)
        }
        category = discord.utils.get(guild.categories, name="対戦中部屋")
        if category is None:
            category = await guild.create_category("対戦中部屋")
        text_ch = await guild.create_text_channel(name=f"room-{room_id}", category=category, overwrites=overwrites)
        voice_ch = await guild.create_voice_channel(name=f"🔊 room-{room_id}", category=category, overwrites=overwrites)
        match_info = {
            'room_id': room_id, 'player1_id': user.id, 'player2_id': bot.user.id,
            'condition': condition, 'status': 'ACTIVE', 'submitter_id': None,
            'submitter_score': 0, 'opponent_score': 0, 'opponent_id': None
        }
        active_matches[text_ch.id] = match_info
        save_active_room(text_ch.id, match_info)
        await text_ch.send(f"🧪 デバッグ部屋 🧪\n{user.mention} 一人用\n募集時間: {condition}分\n部屋を消すには `/close`")
        await interaction.response.send_message(f"🧪 デバッグ部屋作成: {text_ch.mention} / {voice_ch.mention}", ephemeral=True)
        async def auto_delete():
            await asyncio.sleep(30)
            try:
                await voice_ch.delete()
                await text_ch.delete()
                if text_ch.id in active_matches:
                    del active_matches[text_ch.id]
                    delete_active_room(text_ch.id)
            except:
                pass
        bot.loop.create_task(auto_delete())
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
        player2 = await bot.fetch_user(opponent_entry['user_id'])
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
    # 🔥 Renderの環境変数 PORT を使用（なければ8080）
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))

if __name__ == '__main__':
    Thread(target=run_web).start()
    bot.run(TOKEN)
