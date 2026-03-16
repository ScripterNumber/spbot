import os
import hmac
import json
import hashlib
from functools import wraps
from datetime import datetime, timezone, timedelta
from urllib.parse import parse_qsl

import requests
from flask import Flask, request, jsonify, send_from_directory, g
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import or_, and_, func, UniqueConstraint

def utcnow():
    return datetime.now(timezone.utc)

def normalize_database_url(url):
    if url.startswith("postgres://"):
        return url.replace("postgres://", "postgresql+psycopg://", 1)
    if url.startswith("postgresql://") and "+psycopg" not in url:
        return url.replace("postgresql://", "postgresql+psycopg://", 1)
    return url

app = Flask(__name__, static_folder="static", static_url_path="/static")

database_url = normalize_database_url(os.getenv("DATABASE_URL", "sqlite:///local.db"))
app.config["SQLALCHEMY_DATABASE_URI"] = database_url
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
app.config["SQLALCHEMY_ENGINE_OPTIONS"] = {"pool_pre_ping": True}

db = SQLAlchemy(app)

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
APP_URL = os.getenv("APP_URL", "").strip()
WEBAPP_URL = os.getenv("WEBAPP_URL", "").strip() or APP_URL
ROBLOX_SHARED_SECRET = os.getenv("ROBLOX_SHARED_SECRET", "").strip()
ADMIN_IDS = {int(x.strip()) for x in os.getenv("TELEGRAM_ADMIN_IDS", "").split(",") if x.strip().isdigit()}
ROBLOX_HTTP_HEADERS = {"User-Agent": "RobloxModerationBot/1.0"}

LAST_MAINTENANCE_AT = None

class Moderator(db.Model):
    __tablename__ = "moderators"

    telegram_id = db.Column(db.BigInteger, primary_key=True)
    role = db.Column(db.String(32), nullable=False, default="admin")
    active = db.Column(db.Boolean, nullable=False, default=True)
    created_at = db.Column(db.DateTime(timezone=True), nullable=False, default=utcnow)

class Server(db.Model):
    __tablename__ = "servers"

    job_id = db.Column(db.String(128), primary_key=True)
    place_id = db.Column(db.BigInteger, nullable=True, index=True)
    started_at = db.Column(db.DateTime(timezone=True), nullable=False)
    last_seen_at = db.Column(db.DateTime(timezone=True), nullable=False, index=True)
    player_count = db.Column(db.Integer, nullable=False, default=0)
    tps = db.Column(db.Float, nullable=False, default=20.0)
    first_players_json = db.Column(db.JSON, nullable=False, default=list)
    created_at = db.Column(db.DateTime(timezone=True), nullable=False, default=utcnow)

class ServerPlayer(db.Model):
    __tablename__ = "server_players"
    __table_args__ = (
        UniqueConstraint("job_id", "user_id", name="uq_server_players_job_user"),
    )

    id = db.Column(db.Integer, primary_key=True)
    job_id = db.Column(db.String(128), db.ForeignKey("servers.job_id", ondelete="CASCADE"), nullable=False, index=True)
    user_id = db.Column(db.BigInteger, nullable=False, index=True)
    username = db.Column(db.String(64), nullable=False, index=True)
    display_name = db.Column(db.String(64), nullable=False, index=True)
    account_age = db.Column(db.Integer, nullable=False, default=0)
    deaths = db.Column(db.Integer, nullable=False, default=0)
    coins = db.Column(db.Integer, nullable=False, default=0)
    avatar_url = db.Column(db.Text, nullable=True)
    last_ping_ms = db.Column(db.Integer, nullable=True)
    last_seen_at = db.Column(db.DateTime(timezone=True), nullable=False, default=utcnow, index=True)

class Ban(db.Model):
    __tablename__ = "bans"

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.BigInteger, nullable=False, index=True)
    username = db.Column(db.String(64), nullable=False, index=True)
    display_name = db.Column(db.String(64), nullable=False, default="")
    avatar_url = db.Column(db.Text, nullable=True)
    reason = db.Column(db.Text, nullable=False)
    expires_at = db.Column(db.DateTime(timezone=True), nullable=True, index=True)
    permanent = db.Column(db.Boolean, nullable=False, default=False)
    alt_ban_requested = db.Column(db.Boolean, nullable=False, default=False)
    active = db.Column(db.Boolean, nullable=False, default=True, index=True)
    moderator_telegram_id = db.Column(db.BigInteger, nullable=False, index=True)
    created_at = db.Column(db.DateTime(timezone=True), nullable=False, default=utcnow)
    revoked_at = db.Column(db.DateTime(timezone=True), nullable=True)
    revoked_by = db.Column(db.BigInteger, nullable=True)

class ActionQueue(db.Model):
    __tablename__ = "action_queue"

    id = db.Column(db.Integer, primary_key=True)
    job_id = db.Column(db.String(128), nullable=False, index=True)
    user_id = db.Column(db.BigInteger, nullable=True, index=True)
    action_type = db.Column(db.String(32), nullable=False, index=True)
    payload_json = db.Column(db.JSON, nullable=False, default=dict)
    status = db.Column(db.String(32), nullable=False, default="pending", index=True)
    result_json = db.Column(db.JSON, nullable=True)
    created_at = db.Column(db.DateTime(timezone=True), nullable=False, default=utcnow)
    updated_at = db.Column(db.DateTime(timezone=True), nullable=False, default=utcnow, onupdate=utcnow)
    executed_at = db.Column(db.DateTime(timezone=True), nullable=True)

class AuditLog(db.Model):
    __tablename__ = "audit_logs"

    id = db.Column(db.Integer, primary_key=True)
    moderator_telegram_id = db.Column(db.BigInteger, nullable=False, index=True)
    target_user_id = db.Column(db.BigInteger, nullable=True, index=True)
    job_id = db.Column(db.String(128), nullable=True, index=True)
    action_type = db.Column(db.String(64), nullable=False, index=True)
    payload_json = db.Column(db.JSON, nullable=False, default=dict)
    created_at = db.Column(db.DateTime(timezone=True), nullable=False, default=utcnow)

def to_int(value, default=None):
    try:
        return int(value)
    except Exception:
        return default

def to_float(value, default=None):
    try:
        return float(value)
    except Exception:
        return default

def parse_datetime_value(value):
    if value is None:
        return utcnow()
    if isinstance(value, (int, float)):
        timestamp = float(value)
        if timestamp > 9999999999:
            timestamp = timestamp / 1000.0
        return datetime.fromtimestamp(timestamp, tz=timezone.utc)
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return utcnow()
        try:
            if value.isdigit():
                return datetime.fromtimestamp(int(value), tz=timezone.utc)
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except Exception:
            return utcnow()
    return utcnow()

def safe_iso(dt):
    if not dt:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()

def clamp_tps(value):
    number = to_float(value, 20.0)
    if number is None:
        number = 20.0
    return max(0.0, min(20.0, number))

def server_uptime_minutes(server):
    start = server.started_at or utcnow()
    if start.tzinfo is None:
        start = start.replace(tzinfo=timezone.utc)
    return max(0, int((utcnow() - start.astimezone(timezone.utc)).total_seconds() // 60))

def serialize_server(server):
    return {
        "jobId": server.job_id,
        "placeId": server.place_id,
        "startedAt": safe_iso(server.started_at),
        "lastSeenAt": safe_iso(server.last_seen_at),
        "playerCount": int(server.player_count or 0),
        "tps": round(float(server.tps or 0), 2),
        "uptimeMinutes": server_uptime_minutes(server),
        "firstPlayers": server.first_players_json or []
    }

def serialize_player(player):
    return {
        "jobId": player.job_id,
        "userId": int(player.user_id),
        "username": player.username,
        "displayName": player.display_name,
        "accountAge": int(player.account_age or 0),
        "deaths": int(player.deaths or 0),
        "coins": int(player.coins or 0),
        "avatarUrl": player.avatar_url,
        "lastPingMs": player.last_ping_ms,
        "lastSeenAt": safe_iso(player.last_seen_at)
    }

def serialize_ban(ban):
    if not ban:
        return None
    return {
        "id": ban.id,
        "userId": int(ban.user_id),
        "username": ban.username,
        "displayName": ban.display_name,
        "avatarUrl": ban.avatar_url,
        "reason": ban.reason,
        "expiresAt": safe_iso(ban.expires_at),
        "permanent": bool(ban.permanent),
        "altBanRequested": bool(ban.alt_ban_requested),
        "active": bool(ban.active),
        "createdAt": safe_iso(ban.created_at),
        "revokedAt": safe_iso(ban.revoked_at),
        "revokedBy": ban.revoked_by
    }

def serialize_action(action):
    return {
        "id": action.id,
        "jobId": action.job_id,
        "userId": int(action.user_id) if action.user_id is not None else None,
        "actionType": action.action_type,
        "status": action.status,
        "payload": action.payload_json or {},
        "result": action.result_json or {},
        "createdAt": safe_iso(action.created_at),
        "updatedAt": safe_iso(action.updated_at),
        "executedAt": safe_iso(action.executed_at)
    }

def serialize_presence(server, player):
    return {
        "jobId": server.job_id,
        "server": serialize_server(server),
        "player": serialize_player(player)
    }

def seed_moderators():
    created = False
    for admin_id in ADMIN_IDS:
        existing = db.session.get(Moderator, admin_id)
        if not existing:
            db.session.add(Moderator(telegram_id=admin_id, role="admin", active=True))
            created = True
    if created:
        db.session.commit()

def is_authorized_telegram_id(telegram_id):
    if telegram_id in ADMIN_IDS:
        return True
    moderator = db.session.get(Moderator, telegram_id)
    return bool(moderator and moderator.active)

def telegram_api_url(method):
    return f"https://api.telegram.org/bot{BOT_TOKEN}/{method}"

def send_telegram_message(chat_id, text, include_webapp_button=False):
    if not BOT_TOKEN:
        return
    payload = {
        "chat_id": chat_id,
        "text": text
    }
    if include_webapp_button and WEBAPP_URL:
        payload["reply_markup"] = {
            "inline_keyboard": [
                [
                    {
                        "text": "Открыть Mini App",
                        "web_app": {
                            "url": WEBAPP_URL
                        }
                    }
                ]
            ]
        }
    try:
        requests.post(telegram_api_url("sendMessage"), json=payload, timeout=10)
    except Exception:
        pass

def verify_webapp_init_data(init_data):
    if not BOT_TOKEN or not init_data:
        return None
    try:
        parsed = dict(parse_qsl(init_data, keep_blank_values=True))
        received_hash = parsed.pop("hash", None)
        if not received_hash:
            return None
        auth_date = to_int(parsed.get("auth_date"), 0)
        if auth_date:
            auth_dt = datetime.fromtimestamp(auth_date, tz=timezone.utc)
            if auth_dt < utcnow() - timedelta(days=1):
                return None
        data_check_string = "\n".join(f"{key}={value}" for key, value in sorted(parsed.items(), key=lambda item: item[0]))
        secret_key = hmac.new(b"WebAppData", BOT_TOKEN.encode(), hashlib.sha256).digest()
        calculated_hash = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(calculated_hash, received_hash):
            return None
        user_raw = parsed.get("user")
        if not user_raw:
            return None
        user = json.loads(user_raw)
        user_id = to_int(user.get("id"), 0)
        if not user_id:
            return None
        return {
            "id": user_id,
            "firstName": user.get("first_name", ""),
            "lastName": user.get("last_name", ""),
            "username": user.get("username", ""),
            "languageCode": user.get("language_code", "")
        }
    except Exception:
        return None

def deactivate_expired_bans():
    now = utcnow()
    expired = Ban.query.filter(
        Ban.active.is_(True),
        Ban.expires_at.isnot(None),
        Ban.expires_at <= now
    ).all()
    if not expired:
        return 0
    for ban in expired:
        ban.active = False
    db.session.commit()
    return len(expired)

def cleanup_stale_servers():
    cutoff = utcnow() - timedelta(seconds=90)
    stale_servers = Server.query.filter(Server.last_seen_at < cutoff).all()
    if not stale_servers:
        return 0
    job_ids = [server.job_id for server in stale_servers]
    stale_actions = ActionQueue.query.filter(
        ActionQueue.job_id.in_(job_ids),
        ActionQueue.status.in_(["pending", "processing"])
    ).all()
    for action in stale_actions:
        action.status = "failed"
        action.result_json = {"error": "server_offline"}
        action.updated_at = utcnow()
        action.executed_at = utcnow()
    db.session.query(ServerPlayer).filter(ServerPlayer.job_id.in_(job_ids)).delete(synchronize_session=False)
    db.session.query(Server).filter(Server.job_id.in_(job_ids)).delete(synchronize_session=False)
    db.session.commit()
    return len(job_ids)

def run_maintenance(force=False):
    global LAST_MAINTENANCE_AT
    now = utcnow()
    if not force and LAST_MAINTENANCE_AT and (now - LAST_MAINTENANCE_AT).total_seconds() < 30:
        return
    deactivate_expired_bans()
    cleanup_stale_servers()
    LAST_MAINTENANCE_AT = now

def log_audit(moderator_telegram_id, target_user_id, job_id, action_type, payload):
    db.session.add(
        AuditLog(
            moderator_telegram_id=moderator_telegram_id,
            target_user_id=target_user_id,
            job_id=job_id,
            action_type=action_type,
            payload_json=payload or {}
        )
    )

def roblox_avatar_headshots(user_ids):
    clean_ids = []
    for user_id in user_ids:
        value = to_int(user_id)
        if value:
            clean_ids.append(str(value))
    if not clean_ids:
        return {}
    try:
        response = requests.get(
            "https://thumbnails.roblox.com/v1/users/avatar-headshot",
            params={
                "userIds": ",".join(clean_ids),
                "size": "150x150",
                "format": "Png",
                "isCircular": "false"
            },
            headers=ROBLOX_HTTP_HEADERS,
            timeout=10
        )
        if response.status_code != 200:
            return {}
        data = response.json().get("data", [])
        result = {}
        for item in data:
            target_id = to_int(item.get("targetId"))
            if target_id:
                result[target_id] = item.get("imageUrl")
        return result
    except Exception:
        return {}

def lookup_roblox_user(query):
    query = (query or "").strip()
    if not query:
        return None
    try:
        if query.isdigit():
            response = requests.get(
                f"https://users.roblox.com/v1/users/{int(query)}",
                headers=ROBLOX_HTTP_HEADERS,
                timeout=10
            )
            if response.status_code != 200:
                return None
            data = response.json()
            profile = {
                "userId": int(data["id"]),
                "username": data["name"],
                "displayName": data.get("displayName") or data["name"]
            }
        else:
            response = requests.post(
                "https://users.roblox.com/v1/usernames/users",
                headers={**ROBLOX_HTTP_HEADERS, "Content-Type": "application/json"},
                json={
                    "usernames": [query],
                    "excludeBannedUsers": False
                },
                timeout=10
            )
            if response.status_code != 200:
                return None
            data = response.json().get("data", [])
            if not data:
                return None
            user = data[0]
            profile = {
                "userId": int(user["id"]),
                "username": user["name"],
                "displayName": user.get("displayName") or user["name"]
            }
        avatars = roblox_avatar_headshots([profile["userId"]])
        profile["avatarUrl"] = avatars.get(profile["userId"])
        return profile
    except Exception:
        return None

def get_online_presence_for_user(user_id):
    row = db.session.query(ServerPlayer, Server).join(Server, ServerPlayer.job_id == Server.job_id).filter(ServerPlayer.user_id == user_id).first()
    if not row:
        return None
    player, server = row
    return serialize_presence(server, player)

def active_ban_for_user(user_id):
    ban = Ban.query.filter_by(user_id=user_id, active=True).order_by(Ban.created_at.desc()).first()
    if not ban:
        return None
    if ban.expires_at and ban.expires_at <= utcnow():
        ban.active = False
        db.session.commit()
        return None
    return ban

def enqueue_action(job_id, user_id, action_type, payload):
    action = ActionQueue(
        job_id=job_id,
        user_id=user_id,
        action_type=action_type,
        payload_json=payload or {},
        status="pending"
    )
    db.session.add(action)
    return action

def require_webapp_auth(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        init_data = request.headers.get("X-Telegram-Init-Data", "").strip()
        telegram_user = verify_webapp_init_data(init_data)
        if not telegram_user:
            return jsonify({"ok": False, "error": "Unauthorized"}), 401
        if not is_authorized_telegram_id(telegram_user["id"]):
            return jsonify({"ok": False, "error": "Access denied"}), 403
        g.telegram_user = telegram_user
        run_maintenance()
        return fn(*args, **kwargs)
    return wrapper

def require_roblox_auth(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        provided_secret = request.headers.get("X-Roblox-Secret", "").strip()
        if not ROBLOX_SHARED_SECRET or not hmac.compare_digest(provided_secret, ROBLOX_SHARED_SECRET):
            return jsonify({"ok": False, "error": "Forbidden"}), 403
        return fn(*args, **kwargs)
    return wrapper

@app.errorhandler(404)
def handle_404(error):
    if request.path.startswith("/api/"):
        return jsonify({"ok": False, "error": "Not found"}), 404
    return send_from_directory(app.static_folder, "index.html")

@app.route("/", methods=["GET"])
def index():
    return send_from_directory(app.static_folder, "index.html")

@app.route("/health", methods=["GET"])
def health():
    return jsonify({
        "ok": True,
        "service": "roblox-moderation-bot",
        "timestamp": safe_iso(utcnow())
    })

@app.route("/telegram/webhook", methods=["POST"])
def telegram_webhook():
    update = request.get_json(silent=True) or {}
    message = update.get("message") or update.get("edited_message")
    if not message:
        return jsonify({"ok": True})
    text = (message.get("text") or "").strip()
    chat = message.get("chat") or {}
    user = message.get("from") or {}
    chat_id = chat.get("id")
    user_id = to_int(user.get("id"), 0)
    if not chat_id or not user_id:
        return jsonify({"ok": True})
    if text.startswith("/start") or text.startswith("/panel"):
        if is_authorized_telegram_id(user_id):
            send_telegram_message(
                chat_id,
                "Панель модерации готова. Открывай Mini App кнопкой ниже.",
                include_webapp_button=True
            )
        else:
            send_telegram_message(chat_id, "У тебя нет доступа к этой панели.")
    else:
        if is_authorized_telegram_id(user_id):
            send_telegram_message(
                chat_id,
                "Используй /start чтобы открыть панель модерации.",
                include_webapp_button=True
            )
        else:
            send_telegram_message(chat_id, "У тебя нет доступа к этой панели.")
    return jsonify({"ok": True})

@app.route("/api/bootstrap", methods=["GET"])
@require_webapp_auth
def api_bootstrap():
    active_bans_count = Ban.query.filter(Ban.active.is_(True)).count()
    servers_count = Server.query.count()
    players_count = ServerPlayer.query.count()
    return jsonify({
        "ok": True,
        "user": g.telegram_user,
        "stats": {
            "servers": servers_count,
            "players": players_count,
            "activeBans": active_bans_count
        }
    })

@app.route("/api/servers", methods=["GET"])
@require_webapp_auth
def api_servers():
    servers = Server.query.order_by(Server.player_count.desc(), Server.last_seen_at.desc()).all()
    return jsonify({
        "ok": True,
        "servers": [serialize_server(server) for server in servers]
    })

@app.route("/api/servers/<job_id>", methods=["GET"])
@require_webapp_auth
def api_server_detail(job_id):
    server = db.session.get(Server, job_id)
    if not server:
        return jsonify({"ok": False, "error": "Server not found"}), 404
    players = ServerPlayer.query.filter_by(job_id=job_id).order_by(func.lower(ServerPlayer.display_name).asc(), func.lower(ServerPlayer.username).asc()).all()
    return jsonify({
        "ok": True,
        "server": serialize_server(server),
        "players": [serialize_player(player) for player in players]
    })

@app.route("/api/servers/<job_id>/players/<int:user_id>", methods=["GET"])
@require_webapp_auth
def api_server_player_detail(job_id, user_id):
    server = db.session.get(Server, job_id)
    if not server:
        return jsonify({"ok": False, "error": "Server not found"}), 404
    player = ServerPlayer.query.filter_by(job_id=job_id, user_id=user_id).first()
    if not player:
        return jsonify({"ok": False, "error": "Player not found"}), 404
    ban = active_ban_for_user(user_id)
    return jsonify({
        "ok": True,
        "server": serialize_server(server),
        "player": serialize_player(player),
        "activeBan": serialize_ban(ban)
    })

@app.route("/api/players/search", methods=["GET"])
@require_webapp_auth
def api_player_search():
    query = (request.args.get("q") or "").strip()
    if not query:
        return jsonify({"ok": True, "results": []})
    pattern = f"%{query}%"
    rows = db.session.query(ServerPlayer, Server).join(Server, ServerPlayer.job_id == Server.job_id).filter(
        or_(
            ServerPlayer.username.ilike(pattern),
            ServerPlayer.display_name.ilike(pattern)
        )
    ).order_by(func.lower(ServerPlayer.display_name).asc(), Server.player_count.desc()).limit(50).all()
    results = []
    for player, server in rows:
        results.append({
            "server": serialize_server(server),
            "player": serialize_player(player)
        })
    return jsonify({
        "ok": True,
        "results": results
    })

@app.route("/api/bans/lookup", methods=["GET"])
@require_webapp_auth
def api_ban_lookup():
    query = (request.args.get("query") or "").strip()
    if not query:
        return jsonify({"ok": False, "error": "Query is required"}), 400
    profile = lookup_roblox_user(query)
    if not profile:
        return jsonify({"ok": False, "error": "Игрок не найден"}), 404
    active_ban = active_ban_for_user(profile["userId"])
    online_presence = get_online_presence_for_user(profile["userId"])
    return jsonify({
        "ok": True,
        "profile": profile,
        "activeBan": serialize_ban(active_ban),
        "onlinePresence": online_presence
    })

@app.route("/api/bans", methods=["POST"])
@require_webapp_auth
def api_create_ban():
    body = request.get_json(silent=True) or {}
    user_id = to_int(body.get("userId"), 0)
    username = (body.get("username") or "").strip()
    display_name = (body.get("displayName") or username).strip()
    avatar_url = (body.get("avatarUrl") or "").strip() or None
    reason = (body.get("reason") or "").strip()
    permanent = bool(body.get("permanent"))
    days = to_int(body.get("days"), 0)
    alt_ban_requested = bool(body.get("altBanRequested"))
    job_id = (body.get("jobId") or "").strip() or None

    if not user_id or not username or not reason:
        return jsonify({"ok": False, "error": "Некорректные данные для бана"}), 400

    if not permanent and days < 1:
        return jsonify({"ok": False, "error": "Укажи срок бана в днях"}), 400

    active_bans = Ban.query.filter_by(user_id=user_id, active=True).all()
    for item in active_bans:
        item.active = False
        item.revoked_at = utcnow()
        item.revoked_by = g.telegram_user["id"]

    expires_at = None if permanent else utcnow() + timedelta(days=days)

    ban = Ban(
        user_id=user_id,
        username=username,
        display_name=display_name,
        avatar_url=avatar_url,
        reason=reason,
        expires_at=expires_at,
        permanent=permanent,
        alt_ban_requested=alt_ban_requested,
        active=True,
        moderator_telegram_id=g.telegram_user["id"]
    )
    db.session.add(ban)

    queue_job_ids = set()
    if job_id:
        queue_job_ids.add(job_id)

    online_rows = ServerPlayer.query.filter_by(user_id=user_id).all()
    for row in online_rows:
        queue_job_ids.add(row.job_id)

    for online_job_id in queue_job_ids:
        enqueue_action(
            online_job_id,
            user_id,
            "ban",
            {
                "reason": reason,
                "permanent": permanent,
                "days": None if permanent else days,
                "expiresAt": safe_iso(expires_at),
                "altBanRequested": alt_ban_requested,
                "banId": None
            }
        )

    log_audit(
        g.telegram_user["id"],
        user_id,
        job_id,
        "ban_create",
        {
            "username": username,
            "displayName": display_name,
            "reason": reason,
            "permanent": permanent,
            "days": None if permanent else days,
            "altBanRequested": alt_ban_requested,
            "queuedServers": list(queue_job_ids)
        }
    )

    db.session.commit()

    return jsonify({
        "ok": True,
        "ban": serialize_ban(ban),
        "queuedServers": list(queue_job_ids)
    })

@app.route("/api/bans/unban", methods=["POST"])
@require_webapp_auth
def api_unban():
    body = request.get_json(silent=True) or {}
    user_id = to_int(body.get("userId"), 0)
    if not user_id:
        return jsonify({"ok": False, "error": "Некорректный userId"}), 400
    active_bans = Ban.query.filter_by(user_id=user_id, active=True).all()
    if not active_bans:
        return jsonify({"ok": False, "error": "Активный бан не найден"}), 404
    for ban in active_bans:
        ban.active = False
        ban.revoked_at = utcnow()
        ban.revoked_by = g.telegram_user["id"]
    log_audit(
        g.telegram_user["id"],
        user_id,
        None,
        "ban_revoke",
        {"count": len(active_bans)}
    )
    db.session.commit()
    return jsonify({
        "ok": True,
        "revokedCount": len(active_bans)
    })

@app.route("/api/actions/player", methods=["POST"])
@require_webapp_auth
def api_enqueue_player_action():
    body = request.get_json(silent=True) or {}
    job_id = (body.get("jobId") or "").strip()
    user_id = to_int(body.get("userId"), 0)
    action_type = (body.get("actionType") or "").strip()

    if action_type not in {"kick", "kill", "query_ping"}:
        return jsonify({"ok": False, "error": "Недопустимое действие"}), 400

    server = db.session.get(Server, job_id)
    if not server:
        return jsonify({"ok": False, "error": "Сервер не найден"}), 404

    player = ServerPlayer.query.filter_by(job_id=job_id, user_id=user_id).first()
    if not player:
        return jsonify({"ok": False, "error": "Игрок не найден на сервере"}), 404

    if action_type == "query_ping":
        recent = ActionQueue.query.filter(
            ActionQueue.job_id == job_id,
            ActionQueue.user_id == user_id,
            ActionQueue.action_type == "query_ping",
            ActionQueue.created_at >= utcnow() - timedelta(seconds=5),
            ActionQueue.status.in_(["pending", "processing", "completed"])
        ).order_by(ActionQueue.created_at.desc()).first()
        if recent:
            return jsonify({
                "ok": False,
                "error": "Подожди несколько секунд перед следующим запросом пинга",
                "retryAfterSeconds": 5
            }), 429

    payload = {}
    if action_type == "query_ping":
        payload = {"requestedAt": safe_iso(utcnow())}

    action = enqueue_action(job_id, user_id, action_type, payload)

    log_audit(
        g.telegram_user["id"],
        user_id,
        job_id,
        f"{action_type}_queue",
        {}
    )

    db.session.commit()

    return jsonify({
        "ok": True,
        "action": serialize_action(action)
    }), 201

@app.route("/api/actions/<int:action_id>", methods=["GET"])
@require_webapp_auth
def api_action_detail(action_id):
    action = db.session.get(ActionQueue, action_id)
    if not action:
        return jsonify({"ok": False, "error": "Action not found"}), 404
    return jsonify({
        "ok": True,
        "action": serialize_action(action)
    })

@app.route("/api/roblox/server/heartbeat", methods=["POST"])
@require_roblox_auth
def api_roblox_server_heartbeat():
    body = request.get_json(silent=True) or {}
    job_id = (body.get("jobId") or "").strip()
    if not job_id:
        return jsonify({"ok": False, "error": "jobId is required"}), 400

    started_at = parse_datetime_value(body.get("startedAt"))
    place_id = to_int(body.get("placeId"))
    player_count = to_int(body.get("playerCount"), 0)
    tps = clamp_tps(body.get("tps"))

    server = db.session.get(Server, job_id)
    if not server:
        server = Server(
            job_id=job_id,
            started_at=started_at,
            place_id=place_id,
            player_count=player_count,
            tps=tps,
            last_seen_at=utcnow(),
            first_players_json=[]
        )
        db.session.add(server)
    else:
        server.place_id = place_id
        server.player_count = player_count
        server.tps = tps
        server.last_seen_at = utcnow()
        if body.get("startedAt"):
            server.started_at = started_at

    db.session.commit()

    return jsonify({
        "ok": True,
        "server": serialize_server(server)
    })

@app.route("/api/roblox/server/snapshot", methods=["POST"])
@require_roblox_auth
def api_roblox_server_snapshot():
    body = request.get_json(silent=True) or {}
    job_id = (body.get("jobId") or "").strip()
    if not job_id:
        return jsonify({"ok": False, "error": "jobId is required"}), 400

    players_payload = body.get("players") or []
    if not isinstance(players_payload, list):
        return jsonify({"ok": False, "error": "players must be a list"}), 400

    started_at = parse_datetime_value(body.get("startedAt"))
    place_id = to_int(body.get("placeId"))
    tps = clamp_tps(body.get("tps"))

    server = db.session.get(Server, job_id)
    if not server:
        server = Server(
            job_id=job_id,
            started_at=started_at,
            place_id=place_id,
            player_count=0,
            tps=tps,
            last_seen_at=utcnow(),
            first_players_json=[]
        )
        db.session.add(server)
    else:
        server.place_id = place_id
        server.tps = tps
        server.last_seen_at = utcnow()
        if body.get("startedAt"):
            server.started_at = started_at

    existing_players = ServerPlayer.query.filter_by(job_id=job_id).all()
    existing_by_user_id = {int(player.user_id): player for player in existing_players}

    valid_players = []
    avatar_missing_ids = []
    avatar_map = {}

    for raw in players_payload:
        user_id = to_int(raw.get("userId"), 0)
        if not user_id:
            continue
        username = (raw.get("username") or "").strip()
        display_name = (raw.get("displayName") or username).strip()
        avatar_url = (raw.get("avatarUrl") or "").strip() or None
        player_data = {
            "userId": user_id,
            "username": username,
            "displayName": display_name,
            "accountAge": to_int(raw.get("accountAge"), 0),
            "deaths": to_int(raw.get("deaths"), 0),
            "coins": to_int(raw.get("coins"), 0),
            "avatarUrl": avatar_url,
            "lastPingMs": to_int(raw.get("lastPingMs")),
        }
        valid_players.append(player_data)
        if avatar_url:
            avatar_map[user_id] = avatar_url
        else:
            avatar_missing_ids.append(user_id)

    if avatar_missing_ids:
        avatar_map.update(roblox_avatar_headshots(avatar_missing_ids))

    valid_user_ids = {player["userId"] for player in valid_players}
    for old_player in existing_players:
        if int(old_player.user_id) not in valid_user_ids:
            db.session.delete(old_player)

    for item in valid_players:
        user_id = item["userId"]
        player = existing_by_user_id.get(user_id)
        if not player:
            player = ServerPlayer(
                job_id=job_id,
                user_id=user_id
            )
            db.session.add(player)
        player.username = item["username"] or str(user_id)
        player.display_name = item["displayName"] or item["username"] or str(user_id)
        player.account_age = item["accountAge"]
        player.deaths = item["deaths"]
        player.coins = item["coins"]
        player.avatar_url = item["avatarUrl"] or avatar_map.get(user_id)
        if item["lastPingMs"] is not None:
            player.last_ping_ms = item["lastPingMs"]
        player.last_seen_at = utcnow()

    first_players = []
    for item in valid_players[:5]:
        first_players.append({
            "userId": item["userId"],
            "username": item["username"],
            "displayName": item["displayName"],
            "avatarUrl": item["avatarUrl"] or avatar_map.get(item["userId"])
        })

    payload_player_count = to_int(body.get("playerCount"))
    server.player_count = payload_player_count if payload_player_count is not None else len(valid_players)
    server.first_players_json = first_players

    db.session.commit()

    return jsonify({
        "ok": True,
        "server": serialize_server(server),
        "playersCount": len(valid_players)
    })

@app.route("/api/roblox/server/offline", methods=["POST"])
@require_roblox_auth
def api_roblox_server_offline():
    body = request.get_json(silent=True) or {}
    job_id = (body.get("jobId") or "").strip()
    if not job_id:
        return jsonify({"ok": False, "error": "jobId is required"}), 400

    actions = ActionQueue.query.filter(
        ActionQueue.job_id == job_id,
        ActionQueue.status.in_(["pending", "processing"])
    ).all()
    for action in actions:
        action.status = "failed"
        action.result_json = {"error": "server_offline"}
        action.updated_at = utcnow()
        action.executed_at = utcnow()

    db.session.query(ServerPlayer).filter_by(job_id=job_id).delete(synchronize_session=False)
    db.session.query(Server).filter_by(job_id=job_id).delete(synchronize_session=False)
    db.session.commit()

    return jsonify({
        "ok": True,
        "jobId": job_id
    })

@app.route("/api/roblox/server/actions", methods=["GET"])
@require_roblox_auth
def api_roblox_server_actions():
    job_id = (request.args.get("jobId") or "").strip()
    if not job_id:
        return jsonify({"ok": False, "error": "jobId is required"}), 400

    retry_before = utcnow() - timedelta(seconds=10)

    actions = ActionQueue.query.filter(
        ActionQueue.job_id == job_id,
        or_(
            ActionQueue.status == "pending",
            and_(ActionQueue.status == "processing", ActionQueue.updated_at < retry_before)
        )
    ).order_by(ActionQueue.created_at.asc()).limit(25).all()

    now = utcnow()
    for action in actions:
        action.status = "processing"
        action.updated_at = now

    db.session.commit()

    return jsonify({
        "ok": True,
        "actions": [
            {
                "id": action.id,
                "jobId": action.job_id,
                "userId": int(action.user_id) if action.user_id is not None else None,
                "actionType": action.action_type,
                "payload": action.payload_json or {}
            }
            for action in actions
        ]
    })

@app.route("/api/roblox/server/actions/<int:action_id>/complete", methods=["POST"])
@require_roblox_auth
def api_roblox_server_action_complete(action_id):
    action = db.session.get(ActionQueue, action_id)
    if not action:
        return jsonify({"ok": False, "error": "Action not found"}), 404

    body = request.get_json(silent=True) or {}
    provided_job_id = (body.get("jobId") or "").strip()
    if provided_job_id and provided_job_id != action.job_id:
        return jsonify({"ok": False, "error": "Job mismatch"}), 403

    ok = bool(body.get("ok", True))
    result = body.get("result") if isinstance(body.get("result"), dict) else {}
    status = (body.get("status") or "").strip()
    if status not in {"completed", "failed"}:
        status = "completed" if ok else "failed"

    action.status = status
    action.result_json = result
    action.updated_at = utcnow()
    action.executed_at = utcnow()

    if action.action_type == "query_ping" and status == "completed":
        ping_ms = to_int(result.get("pingMs"))
        if ping_ms is not None:
            player = ServerPlayer.query.filter_by(job_id=action.job_id, user_id=action.user_id).first()
            if player:
                player.last_ping_ms = ping_ms
                player.last_seen_at = utcnow()

    db.session.commit()

    return jsonify({
        "ok": True,
        "action": serialize_action(action)
    })

@app.route("/api/roblox/player/ban-check", methods=["GET"])
@require_roblox_auth
def api_roblox_ban_check():
    user_id = to_int(request.args.get("userId"), 0)
    if not user_id:
        return jsonify({"ok": False, "error": "userId is required"}), 400
    ban = active_ban_for_user(user_id)
    if not ban:
        return jsonify({
            "ok": True,
            "banned": False
        })
    return jsonify({
        "ok": True,
        "banned": True,
        "ban": serialize_ban(ban)
    })

with app.app_context():
    db.create_all()
    seed_moderators()
    run_maintenance(force=True)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "10000")))
