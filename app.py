import os
import json
import hmac
import hashlib
import requests
from urllib.parse import unquote
from datetime import datetime, timedelta, timezone
from flask import Flask, request, jsonify, send_file
import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor

app = Flask(__name__)

DB_URL = os.environ.get("DATABASE_URL")
BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
ROBLOX_SECRET = os.environ.get("ROBLOX_SHARED_SECRET", "default_secret")

ADMINS_ENV = os.environ.get("TELEGRAM_ADMINS", "TickFreezek, ipvp6").split(",")
ADMINS_ENV += os.environ.get("TELEGRAM_ADMIN_IDS", "").split(",")
ADMIN_IDS = []
ADMIN_USERNAMES = []

for x in ADMINS_ENV:
    x = x.strip()
    if not x: continue
    if x.isdigit() or (x.startswith('-') and x[1:].isdigit()):
        ADMIN_IDS.append(int(x))
    else:
        ADMIN_USERNAMES.append(x.lower().lstrip('@'))

db_pool = psycopg2.pool.SimpleConnectionPool(1, 10, DB_URL)

def get_db():
    conn = db_pool.getconn()
    conn.autocommit = True
    return conn

def release_db(conn):
    db_pool.putconn(conn)

# Жесткая авто-починка базы данных при запуске
def setup_database():
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS server_players (
                    job_id TEXT,
                    user_id BIGINT,
                    username TEXT,
                    display_name TEXT,
                    account_age INT DEFAULT 0,
                    deaths INT DEFAULT 0,
                    coins INT DEFAULT 0,
                    ping INT DEFAULT 0,
                    avatar_url TEXT,
                    last_seen_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (job_id, user_id)
                )
            """)
            cur.execute("ALTER TABLE server_players ADD COLUMN IF NOT EXISTS ping INT DEFAULT 0;")
            cur.execute("ALTER TABLE server_players ADD COLUMN IF NOT EXISTS avatar_url TEXT;")
    except Exception as e:
        print("DB Setup Warning:", e)
    finally:
        release_db(conn)

setup_database()

def verify_telegram_data(init_data):
    if not init_data: return False, None
    try:
        parsed_data = dict(x.split('=') for x in unquote(init_data).split('&'))
        hash_val = parsed_data.pop('hash', None)
        if not hash_val: return False, None
        data_check_string = '\n'.join(f"{k}={v}" for k, v in sorted(parsed_data.items()))
        secret_key = hmac.new(b"WebAppData", BOT_TOKEN.encode(), hashlib.sha256).digest()
        calc_hash = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256).hexdigest()
        if calc_hash == hash_val:
            user_data = json.loads(parsed_data.get('user', '{}'))
            return True, user_data
    except Exception:
        pass
    return False, None

def require_auth(f):
    def wrapper(*args, **kwargs):
        if app.debug: return f(*args, **kwargs)
        init_data = request.headers.get("X-Telegram-Init-Data")
        valid, user = verify_telegram_data(init_data)
        if not valid: return jsonify({"error": "Unauthorized"}), 403
            
        u_id = user.get('id', 0)
        u_name = user.get('username', '').lower()
        
        if (u_id not in ADMIN_IDS) and (u_name not in ADMIN_USERNAMES):
            return jsonify({"error": "Unauthorized"}), 403
            
        return f(*args, **kwargs)
    wrapper.__name__ = f.__name__
    return wrapper

def require_roblox_auth(f):
    def wrapper(*args, **kwargs):
        secret = request.headers.get("Authorization")
        if secret != ROBLOX_SECRET:
            return jsonify({"error": "Unauthorized"}), 403
        return f(*args, **kwargs)
    wrapper.__name__ = f.__name__
    return wrapper

@app.route('/')
def index():
    return send_file('static/index.html')

@app.route('/health')
def health():
    return "OK", 200

@app.route('/webhook', methods=['POST'])
def telegram_webhook():
    data = request.json
    if not data or 'message' not in data:
        return jsonify({"status": "ok"}), 200
    
    chat_id = data['message']['chat']['id']
    text = data['message'].get('text', '')
    
    u_id = data['message']['from'].get('id', 0)
    u_name = data['message']['from'].get('username', '').lower()

    if (u_id not in ADMIN_IDS) and (u_name not in ADMIN_USERNAMES):
        return jsonify({"status": "ok"}), 200

    if text == '/start':
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        webapp_url = request.host_url.replace("http://", "https://").rstrip('/')
        payload = {
            "chat_id": chat_id,
            "text": "Панель модерации активна. Нажми кнопку ниже, чтобы войти.",
            "reply_markup": {
                "inline_keyboard": [[{"text": "Открыть Moderation Center", "web_app": {"url": webapp_url}}]]
            }
        }
        requests.post(url, json=payload)

    return jsonify({"status": "ok"}), 200

@app.route('/api/servers', methods=['GET'])
@require_auth
def get_servers():
    conn = get_db()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("DELETE FROM servers WHERE last_seen_at < NOW() - INTERVAL '3 minutes'")
            cur.execute("SELECT * FROM servers ORDER BY player_count DESC")
            servers = cur.fetchall()
            for s in servers:
                if 'first_players_json' in s and isinstance(s['first_players_json'], str):
                    s['first_players_json'] = json.loads(s['first_players_json'])
                s['firstPlayers'] = s.pop('first_players_json', [])
        return jsonify({"servers": servers})
    finally:
        release_db(conn)

@app.route('/api/servers/<job_id>', methods=['GET'])
@require_auth
def get_server_detail(job_id):
    conn = get_db()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM servers WHERE job_id = %s", (job_id,))
            server = cur.fetchone()
            if not server:
                return jsonify({"error": "Server not found"}), 404
            cur.execute("SELECT * FROM server_players WHERE job_id = %s", (job_id,))
            players = cur.fetchall()
            return jsonify({"server": server, "players": players})
    finally:
        release_db(conn)

@app.route('/api/servers/<job_id>/players/<int:user_id>', methods=['GET'])
@require_auth
def get_player_detail(job_id, user_id):
    conn = get_db()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM server_players WHERE job_id = %s AND user_id = %s", (job_id, user_id))
            player = cur.fetchone()
            if not player:
                return jsonify({"error": "Player not found"}), 404
            return jsonify({"player": player})
    finally:
        release_db(conn)

@app.route('/api/search/players', methods=['GET'])
@require_auth
def search_players():
    q = request.args.get('q', '').strip()
    if not q: return jsonify([]), 200
    
    conn = get_db()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            search_pattern = f"%{q}%"
            cur.execute("""
                SELECT p.*, s.job_id, s.player_count, s.tps, s.started_at 
                FROM server_players p
                JOIN servers s ON p.job_id = s.job_id
                WHERE p.username ILIKE %s OR p.display_name ILIKE %s
            """, (search_pattern, search_pattern))
            results = cur.fetchall()
            output = []
            for r in results:
                player = {k: v for k, v in r.items() if k not in ['job_id', 'player_count', 'tps', 'started_at']}
                server = {'job_id': r['job_id'], 'player_count': r['player_count'], 'tps': r['tps'], 'started_at': r['started_at']}
                output.append({"player": player, "server": server})
            return jsonify(output)
    finally:
        release_db(conn)

@app.route('/api/bans', methods=['POST'])
@require_auth
def issue_ban():
    data = request.json
    conn = get_db()
    try:
        with conn.cursor() as cur:
            expires_at = None
            permanent = True
            if int(data.get('days', 0)) > 0:
                expires_at = datetime.now(timezone.utc) + timedelta(days=int(data['days']))
                permanent = False
                
            cur.execute("""
                INSERT INTO bans (user_id, username, display_name, avatar_url, reason, expires_at, permanent, alt_ban_requested, active)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (user_id) DO UPDATE SET
                username = EXCLUDED.username,
                reason = EXCLUDED.reason,
                expires_at = EXCLUDED.expires_at,
                permanent = EXCLUDED.permanent,
                active = TRUE
            """, (data['user_id'], data['username'], data['display_name'], data.get('avatar_url', ''), data['reason'], expires_at, permanent, data.get('ban_alts', False), True))
            
            if 'job_id' in data:
                cur.execute("""
                    INSERT INTO action_queue (job_id, user_id, action_type, payload_json)
                    VALUES (%s, %s, %s, %s)
                """, (data['job_id'], data['user_id'], 'kick', json.dumps({"reason": data['reason']})))
                
        return jsonify({"success": True})
    finally:
        release_db(conn)

@app.route('/api/bans/<int:user_id>', methods=['DELETE'])
@require_auth
def issue_unban(user_id):
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("UPDATE bans SET active = FALSE WHERE user_id = %s", (user_id,))
        return jsonify({"success": True})
    finally:
        release_db(conn)

@app.route('/api/actions', methods=['POST'])
@require_auth
def queue_action():
    data = request.json
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO action_queue (job_id, user_id, action_type, payload_json)
                VALUES (%s, %s, %s, %s)
            """, (data['job_id'], data['user_id'], data['action_type'], json.dumps(data.get('payload', {}))))
        return jsonify({"success": True})
    finally:
        release_db(conn)

@app.route('/api/servers/<job_id>/players/<int:user_id>/ping', methods=['POST'])
@require_auth
def request_ping(job_id, user_id):
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO action_queue (job_id, user_id, action_type)
                VALUES (%s, %s, 'get_ping')
            """, (job_id, user_id))
        return jsonify({"success": True})
    finally:
        release_db(conn)

@app.route('/api/roblox/user', methods=['GET'])
@require_auth
def roblox_user_lookup():
    username = request.args.get('username')
    if not username: return jsonify({"error": "Username required"}), 400
        
    try:
        res = requests.post("https://users.roblox.com/v1/usernames/users", json={"usernames": [username], "excludeBannedUsers": False})
        data = res.json()
        if not data.get("data"): return jsonify({"error": "User not found"}), 404
            
        user_info = data["data"][0]
        user_id = user_info["id"]
        
        avatar_res = requests.get(f"https://thumbnails.roblox.com/v1/users/avatar-headshot?userIds={user_id}&size=150x150&format=Png&isCircular=false")
        avatar_data = avatar_res.json()
        avatar_url = avatar_data["data"][0]["imageUrl"] if avatar_data.get("data") else ""
        
        conn = get_db()
        banned = False
        ban_info = None
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT * FROM bans WHERE user_id = %s AND active = TRUE", (user_id,))
                ban_record = cur.fetchone()
                if ban_record:
                    banned = True
                    ban_info = ban_record
        finally:
            release_db(conn)
            
        return jsonify({
            "user": {
                "userId": user_id,
                "username": user_info["name"],
                "displayName": user_info["displayName"],
                "avatarUrl": avatar_url
            },
            "banned": banned,
            "ban": ban_info
        })
    except Exception:
        return jsonify({"error": "Failed to fetch from Roblox API"}), 500

@app.route('/roblox/heartbeat', methods=['POST'])
@require_roblox_auth
def roblox_heartbeat():
    data = request.json
    conn = get_db()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            first_players = json.dumps(data.get('players', [])[:5])
            cur.execute("""
                INSERT INTO servers (job_id, place_id, player_count, tps, first_players_json, last_seen_at)
                VALUES (%s, %s, %s, %s, %s, NOW())
                ON CONFLICT (job_id) DO UPDATE SET
                player_count = EXCLUDED.player_count,
                tps = EXCLUDED.tps,
                first_players_json = EXCLUDED.first_players_json,
                last_seen_at = NOW()
            """, (data['job_id'], data.get('place_id', 0), data.get('player_count', 0), data.get('tps', 20.0), first_players))
            
            cur.execute("SELECT * FROM action_queue WHERE job_id = %s AND status = 'pending'", (data['job_id'],))
            actions = cur.fetchall()
            
            if actions:
                action_ids = tuple(a['id'] for a in actions)
                cur.execute("UPDATE action_queue SET status = 'sent' WHERE id IN %s", (action_ids,))
                
            cur.execute("SELECT user_id FROM bans WHERE active = TRUE")
            bans = [r['user_id'] for r in cur.fetchall()]
                
        return jsonify({"actions": actions, "active_bans": bans})
    finally:
        release_db(conn)

@app.route('/roblox/snapshot', methods=['POST'])
@require_roblox_auth
def roblox_snapshot():
    data = request.json
    job_id = data.get('job_id')
    players = data.get('players', [])
    
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO servers (job_id, last_seen_at) 
                VALUES (%s, NOW()) 
                ON CONFLICT (job_id) DO NOTHING
            """, (job_id,))
            
            cur.execute("DELETE FROM server_players WHERE job_id = %s", (job_id,))
            for p in players:
                # Жесткая защита типов
                cur.execute("""
                    INSERT INTO server_players (job_id, user_id, username, display_name, account_age, deaths, coins, ping, avatar_url, last_seen_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                """, (
                    job_id, 
                    int(p.get('user_id', 0)), 
                    str(p.get('username', 'Unknown')), 
                    str(p.get('display_name', 'Unknown')), 
                    int(p.get('account_age', 0)), 
                    int(p.get('deaths', 0)), 
                    int(p.get('coins', 0)), 
                    int(p.get('ping', 0)), 
                    str(p.get('avatar_url', ''))
                ))
        return jsonify({"success": True})
    finally:
        release_db(conn)

@app.route('/roblox/offline', methods=['POST'])
@require_roblox_auth
def roblox_offline():
    data = request.json
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM servers WHERE job_id = %s", (data.get('job_id'),))
        return jsonify({"success": True})
    finally:
        release_db(conn)
