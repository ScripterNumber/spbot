import os
import json
import hmac
import hashlib
import requests
from urllib.parse import parse_qsl
from datetime import datetime, timedelta, timezone
from flask import Flask, request, jsonify, send_from_directory
import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor

app = Flask(__name__, static_folder="static", static_url_path="/static")

DB_URL = os.environ.get("DATABASE_URL", "").strip()
BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
ROBLOX_SECRET = os.environ.get("ROBLOX_SHARED_SECRET", "").strip()

admins_raw = []
admins_raw.extend(os.environ.get("TELEGRAM_ADMINS", "").split(","))
admins_raw.extend(os.environ.get("TELEGRAM_ADMIN_IDS", "").split(","))

ADMIN_IDS = set()
ADMIN_USERNAMES = set()

for item in admins_raw:
    value = item.strip()
    if not value:
        continue
    if value.isdigit() or (value.startswith("-") and value[1:].isdigit()):
        ADMIN_IDS.add(int(value))
    else:
        ADMIN_USERNAMES.add(value.lower().lstrip("@"))

db_pool = pool.SimpleConnectionPool(1, 10, DB_URL)

def get_db():
    conn = db_pool.getconn()
    conn.autocommit = True
    return conn

def release_db(conn):
    db_pool.putconn(conn)

def setup_database():
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS servers (
                    job_id TEXT PRIMARY KEY,
                    place_id BIGINT,
                    started_at TIMESTAMPTZ DEFAULT NOW(),
                    last_seen_at TIMESTAMPTZ DEFAULT NOW(),
                    player_count INT DEFAULT 0,
                    tps REAL DEFAULT 20.0,
                    first_players_json JSONB DEFAULT '[]'::jsonb
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS server_players (
                    job_id TEXT NOT NULL,
                    user_id BIGINT NOT NULL,
                    username TEXT NOT NULL,
                    display_name TEXT NOT NULL,
                    account_age INT DEFAULT 0,
                    deaths INT DEFAULT 0,
                    coins INT DEFAULT 0,
                    ping INT DEFAULT 0,
                    avatar_url TEXT DEFAULT '',
                    last_seen_at TIMESTAMPTZ DEFAULT NOW(),
                    PRIMARY KEY (job_id, user_id)
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS bans (
                    user_id BIGINT PRIMARY KEY,
                    username TEXT,
                    display_name TEXT,
                    avatar_url TEXT,
                    reason TEXT,
                    expires_at TIMESTAMPTZ,
                    permanent BOOLEAN DEFAULT FALSE,
                    alt_ban_requested BOOLEAN DEFAULT FALSE,
                    active BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS action_queue (
                    id SERIAL PRIMARY KEY,
                    job_id TEXT,
                    user_id BIGINT,
                    action_type TEXT,
                    payload_json JSONB DEFAULT '{}'::jsonb,
                    status TEXT DEFAULT 'pending',
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)
    finally:
        release_db(conn)

setup_database()

def verify_telegram_data(init_data):
    if not init_data or not BOT_TOKEN:
        return False, None
    try:
        pairs = dict(parse_qsl(init_data, keep_blank_values=True))
        received_hash = pairs.pop("hash", None)
        if not received_hash:
            return False, None
        data_check_string = "\n".join(f"{k}={v}" for k, v in sorted(pairs.items(), key=lambda x: x[0]))
        secret_key = hmac.new(b"WebAppData", BOT_TOKEN.encode(), hashlib.sha256).digest()
        calc_hash = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(calc_hash, received_hash):
            return False, None
        user = json.loads(pairs.get("user", "{}"))
        return True, user
    except Exception:
        return False, None

def is_admin(user):
    if not user:
        return False
    user_id = user.get("id")
    username = str(user.get("username", "")).lower()
    if user_id in ADMIN_IDS:
        return True
    if username in ADMIN_USERNAMES:
        return True
    return False

def require_auth(fn):
    def wrapper(*args, **kwargs):
        init_data = request.headers.get("X-Telegram-Init-Data", "").strip()
        ok, user = verify_telegram_data(init_data)
        if not ok or not is_admin(user):
            return jsonify({"error": "Unauthorized"}), 403
        return fn(*args, **kwargs)
    wrapper.__name__ = fn.__name__
    return wrapper

def require_roblox_auth(fn):
    def wrapper(*args, **kwargs):
        secret = request.headers.get("Authorization", "").strip()
        if not ROBLOX_SECRET or not hmac.compare_digest(secret, ROBLOX_SECRET):
            return jsonify({"error": "Unauthorized"}), 403
        return fn(*args, **kwargs)
    wrapper.__name__ = fn.__name__
    return wrapper

def fetch_avatars_map(user_ids):
    ids = []
    seen = set()
    for user_id in user_ids:
        try:
            uid = int(user_id)
        except Exception:
            continue
        if uid <= 0 or uid in seen:
            continue
        ids.append(str(uid))
        seen.add(uid)
    if not ids:
        return {}
    result = {}
    for i in range(0, len(ids), 100):
        chunk = ids[i:i + 100]
        try:
            response = requests.get(
                "https://thumbnails.roblox.com/v1/users/avatar-headshot",
                params={
                    "userIds": ",".join(chunk),
                    "size": "150x150",
                    "format": "Png",
                    "isCircular": "false"
                },
                timeout=5
            )
            if response.status_code != 200:
                continue
            data = response.json().get("data", [])
            for item in data:
                target_id = int(item.get("targetId", 0))
                image_url = item.get("imageUrl", "")
                if target_id > 0 and image_url:
                    result[target_id] = image_url
        except Exception:
            continue
    return result

def normalize_players(raw_players):
    normalized = []
    for player in raw_players:
        if not isinstance(player, dict):
            continue
        raw_user_id = player.get("user_id", player.get("userId", 0))
        try:
            user_id = int(raw_user_id)
        except Exception:
            continue
        if user_id <= 0:
            continue
        username = str(player.get("username", player.get("name", user_id))).strip() or str(user_id)
        display_name = str(player.get("display_name", player.get("displayName", username))).strip() or username
        try:
            account_age = int(player.get("account_age", player.get("accountAge", 0)) or 0)
        except Exception:
            account_age = 0
        try:
            deaths = int(player.get("deaths", 0) or 0)
        except Exception:
            deaths = 0
        try:
            coins = int(player.get("coins", 0) or 0)
        except Exception:
            coins = 0
        try:
            ping = int(player.get("ping", player.get("lastPingMs", 0)) or 0)
        except Exception:
            ping = 0
        normalized.append({
            "user_id": user_id,
            "username": username,
            "display_name": display_name,
            "account_age": account_age,
            "deaths": deaths,
            "coins": coins,
            "ping": ping
        })
    return normalized

@app.route("/")
def index():
    return send_from_directory(app.static_folder, "index.html")

@app.route("/health")
def health():
    return jsonify({"ok": True})

@app.route("/api/debug/server-players")
def debug_server_players():
    conn = get_db()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM server_players ORDER BY last_seen_at DESC LIMIT 100")
            rows = cur.fetchall()
            return jsonify({"rows": rows})
    finally:
        release_db(conn)

@app.route("/api/debug/servers")
def debug_servers():
    conn = get_db()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM servers ORDER BY last_seen_at DESC LIMIT 100")
            rows = cur.fetchall()
            return jsonify({"rows": rows})
    finally:
        release_db(conn)

@app.route("/roblox/snapshot", methods=["POST"])
@require_roblox_auth
def roblox_snapshot():
    data = request.get_json(silent=True) or {}
    job_id = str(data.get("job_id", data.get("jobId", ""))).strip()
    place_id = int(data.get("place_id", data.get("placeId", 0)) or 0)
    player_count = int(data.get("player_count", data.get("playerCount", 0)) or 0)
    tps = float(data.get("tps", 20.0) or 20.0)
    players_raw = data.get("players", []) if isinstance(data.get("players", []), list) else []

    if not job_id:
        return jsonify({"error": "job_id required"}), 400

    players = normalize_players(players_raw)
    avatar_map = fetch_avatars_map([player["user_id"] for player in players])

    first_players = []
    for player in players[:5]:
        first_players.append({
            "userId": player["user_id"],
            "username": player["username"],
            "displayName": player["display_name"],
            "avatarUrl": avatar_map.get(player["user_id"], "")
        })

    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO servers (job_id, place_id, player_count, tps, first_players_json, last_seen_at)
                VALUES (%s, %s, %s, %s, %s, NOW())
                ON CONFLICT (job_id) DO UPDATE SET
                    place_id = EXCLUDED.place_id,
                    player_count = EXCLUDED.player_count,
                    tps = EXCLUDED.tps,
                    first_players_json = EXCLUDED.first_players_json,
                    last_seen_at = NOW()
            """, (
                job_id,
                place_id,
                player_count if player_count > 0 else len(players),
                tps,
                json.dumps(first_players)
            ))

            cur.execute("DELETE FROM server_players WHERE job_id = %s", (job_id,))

            for player in players:
                cur.execute("""
                    INSERT INTO server_players (
                        job_id,
                        user_id,
                        username,
                        display_name,
                        account_age,
                        deaths,
                        coins,
                        ping,
                        avatar_url,
                        last_seen_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                """, (
                    job_id,
                    player["user_id"],
                    player["username"],
                    player["display_name"],
                    player["account_age"],
                    player["deaths"],
                    player["coins"],
                    player["ping"],
                    avatar_map.get(player["user_id"], "")
                ))
        return jsonify({"success": True, "players_saved": len(players)})
    finally:
        release_db(conn)

@app.route("/roblox/heartbeat", methods=["POST"])
@require_roblox_auth
def roblox_heartbeat():
    data = request.get_json(silent=True) or {}
    job_id = str(data.get("job_id", data.get("jobId", ""))).strip()
    place_id = int(data.get("place_id", data.get("placeId", 0)) or 0)
    player_count = int(data.get("player_count", data.get("playerCount", 0)) or 0)
    tps = float(data.get("tps", 20.0) or 20.0)
    players_raw = data.get("players", []) if isinstance(data.get("players", []), list) else []
    players = normalize_players(players_raw)

    if not job_id:
        return jsonify({"error": "job_id required"}), 400

    preview_ids = []
    preview_players = []
    for player in players[:5]:
        preview_ids.append(player["user_id"])
        preview_players.append({
            "userId": player["user_id"],
            "username": player["username"],
            "displayName": player["display_name"],
            "avatarUrl": ""
        })

    avatar_map = fetch_avatars_map(preview_ids)
    for item in preview_players:
        if item["userId"] in avatar_map:
            item["avatarUrl"] = avatar_map[item["userId"]]

    conn = get_db()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                INSERT INTO servers (job_id, place_id, player_count, tps, first_players_json, last_seen_at)
                VALUES (%s, %s, %s, %s, %s, NOW())
                ON CONFLICT (job_id) DO UPDATE SET
                    place_id = EXCLUDED.place_id,
                    player_count = EXCLUDED.player_count,
                    tps = EXCLUDED.tps,
                    first_players_json = EXCLUDED.first_players_json,
                    last_seen_at = NOW()
            """, (job_id, place_id, player_count if player_count > 0 else len(players), tps, json.dumps(preview_players)))

            cur.execute("""
                SELECT id, job_id, user_id, action_type, payload_json
                FROM action_queue
                WHERE job_id = %s AND status = 'pending'
                ORDER BY id ASC
                LIMIT 50
            """, (job_id,))
            actions = cur.fetchall()

            if actions:
                ids = [str(int(action["id"])) for action in actions]
                cur.execute(f"UPDATE action_queue SET status = 'sent' WHERE id IN ({','.join(ids)})")

            cur.execute("SELECT user_id FROM bans WHERE active = TRUE")
            bans = [int(row["user_id"]) for row in cur.fetchall()]

        normalized_actions = []
        for action in actions:
            payload = action.get("payload_json")
            if isinstance(payload, str):
                try:
                    payload = json.loads(payload)
                except Exception:
                    payload = {}
            normalized_actions.append({
                "id": int(action["id"]),
                "job_id": action["job_id"],
                "user_id": int(action["user_id"]) if action["user_id"] is not None else None,
                "action_type": action["action_type"],
                "payload_json": payload or {}
            })

        return jsonify({"actions": normalized_actions, "active_bans": bans})
    finally:
        release_db(conn)

@app.route("/api/servers", methods=["GET"])
@require_auth
def api_servers():
    conn = get_db()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("DELETE FROM servers WHERE last_seen_at < NOW() - INTERVAL '180 seconds'")
            cur.execute("DELETE FROM server_players WHERE job_id NOT IN (SELECT job_id FROM servers)")
            cur.execute("SELECT * FROM servers ORDER BY player_count DESC, last_seen_at DESC")
            rows = cur.fetchall()
            servers = []
            for row in rows:
                first_players = row.get("first_players_json") or []
                if isinstance(first_players, str):
                    try:
                        first_players = json.loads(first_players)
                    except Exception:
                        first_players = []
                servers.append({
                    "jobId": row["job_id"],
                    "placeId": row.get("place_id"),
                    "startedAt": row.get("started_at"),
                    "lastSeenAt": row.get("last_seen_at"),
                    "playerCount": int(row.get("player_count", 0) or 0),
                    "tps": float(row.get("tps", 0) or 0),
                    "firstPlayers": first_players
                })
        return jsonify({"servers": servers})
    finally:
        release_db(conn)

@app.route("/api/servers/<job_id>", methods=["GET"])
@require_auth
def api_server_detail(job_id):
    conn = get_db()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM servers WHERE job_id = %s", (job_id,))
            server_row = cur.fetchone()
            if not server_row:
                return jsonify({"error": "Server not found"}), 404

            first_players = server_row.get("first_players_json") or []
            if isinstance(first_players, str):
                try:
                    first_players = json.loads(first_players)
                except Exception:
                    first_players = []

            cur.execute("""
                SELECT job_id, user_id, username, display_name, account_age, deaths, coins, ping, avatar_url, last_seen_at
                FROM server_players
                WHERE job_id = %s
                ORDER BY LOWER(display_name), LOWER(username)
            """, (job_id,))
            player_rows = cur.fetchall()

            server = {
                "jobId": server_row["job_id"],
                "placeId": server_row.get("place_id"),
                "startedAt": server_row.get("started_at"),
                "lastSeenAt": server_row.get("last_seen_at"),
                "playerCount": int(server_row.get("player_count", 0) or 0),
                "tps": float(server_row.get("tps", 0) or 0),
                "firstPlayers": first_players
            }

            players = []
            for row in player_rows:
                players.append({
                    "jobId": row["job_id"],
                    "userId": int(row["user_id"]),
                    "username": row["username"],
                    "displayName": row["display_name"],
                    "accountAge": int(row.get("account_age", 0) or 0),
                    "deaths": int(row.get("deaths", 0) or 0),
                    "coins": int(row.get("coins", 0) or 0),
                    "ping": int(row.get("ping", 0) or 0),
                    "avatarUrl": row.get("avatar_url", "")
                })

        return jsonify({"server": server, "players": players})
    finally:
        release_db(conn)

@app.route("/api/search/players", methods=["GET"])
@require_auth
def api_search_players():
    q = (request.args.get("q") or "").strip()
    if not q:
        return jsonify({"results": []})
    conn = get_db()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            pattern = f"%{q}%"
            cur.execute("""
                SELECT
                    p.job_id,
                    p.user_id,
                    p.username,
                    p.display_name,
                    p.account_age,
                    p.deaths,
                    p.coins,
                    p.ping,
                    p.avatar_url,
                    s.player_count,
                    s.tps,
                    s.started_at
                FROM server_players p
                JOIN servers s ON s.job_id = p.job_id
                WHERE p.username ILIKE %s OR p.display_name ILIKE %s
                ORDER BY LOWER(p.display_name), LOWER(p.username)
                LIMIT 100
            """, (pattern, pattern))
            rows = cur.fetchall()
            results = []
            for row in rows:
                results.append({
                    "player": {
                        "jobId": row["job_id"],
                        "userId": int(row["user_id"]),
                        "username": row["username"],
                        "displayName": row["display_name"],
                        "accountAge": int(row.get("account_age", 0) or 0),
                        "deaths": int(row.get("deaths", 0) or 0),
                        "coins": int(row.get("coins", 0) or 0),
                        "ping": int(row.get("ping", 0) or 0),
                        "avatarUrl": row.get("avatar_url", "")
                    },
                    "server": {
                        "jobId": row["job_id"],
                        "playerCount": int(row.get("player_count", 0) or 0),
                        "tps": float(row.get("tps", 0) or 0),
                        "startedAt": row.get("started_at")
                    }
                })
        return jsonify({"results": results})
    finally:
        release_db(conn)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "10000")))
