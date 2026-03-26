"""
GREENBEANS CC — Telegram bot UI (hub, profile, top-up, payments, cart).
"""
from __future__ import annotations

import asyncio
import html
import io
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from aiohttp import web
from dotenv import load_dotenv
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update, User
from telegram.error import Conflict
from telegram.ext import Application, CallbackQueryHandler, CommandHandler, ContextTypes

load_dotenv()

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
log = logging.getLogger(__name__)

CAPTION = """♨️ Big Restocks Every Day
♨️ Convenient Deposit Via Btc, Ltc
♨️ Support Will Help You 24/7

COUNTRY LIST: 🇺🇸 🇫🇷 🇨🇦 🇩🇪 🇪🇸 🇦🇪 🇮🇱 🇨🇴 🇲🇽 🇨🇱 🇯🇵 🇵🇭

⚜️ Have a good day with GREENBEANS CC ⚜️"""

USERS: dict[int, dict[str, Any]] = {}

BTC_ADDR = os.getenv("BTC_WALLET", "bc1q…set BTC_WALLET in .env").strip()
LTC_ADDR = os.getenv("LTC_WALLET", "ltc1q…set LTC_WALLET in .env").strip()

ROOT_DIR = Path(__file__).resolve().parent
DATA_DIR = ROOT_DIR / "data"
ASSETS_DIR = ROOT_DIR / "assets"
KNOWN_USERS_PATH = DATA_DIR / "known_users.json"
PAYMENTS_PATH = DATA_DIR / "payments.json"
STOCK_PATH = DATA_DIR / "stock_tiers.json"

TIER_IDS: tuple[str, ...] = ("random", "70", "80", "90", "100")
STOCK_BY_TIER: dict[str, dict[str, list[str]]] = {k: {} for k in TIER_IDS}

CORS_HEADERS: dict[str, str] = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type, X-Leadbot-Secret",
}


def _parse_admin_ids() -> set[int]:
    raw = os.getenv("ADMIN_USER_IDS", "").strip()
    out: set[int] = set()
    for part in raw.replace(" ", "").split(","):
        if not part:
            continue
        try:
            out.add(int(part))
        except ValueError:
            log.warning("Invalid ADMIN_USER_IDS entry: %s", part)
    return out


ADMIN_USER_IDS: set[int] = _parse_admin_ids()

LEADBOT_API_SECRET: str = os.getenv("LEADBOT_API_SECRET", "").strip()


def load_stock_tiers() -> None:
    global STOCK_BY_TIER
    STOCK_BY_TIER = {k: {} for k in TIER_IDS}
    if not STOCK_PATH.is_file():
        return
    try:
        raw = json.loads(STOCK_PATH.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            return
        for tid in TIER_IDS:
            tier_obj = raw.get(tid)
            if not isinstance(tier_obj, dict):
                continue
            for bin_key, lines in tier_obj.items():
                if not isinstance(lines, list):
                    continue
                STOCK_BY_TIER[tid][str(bin_key)] = [str(x) for x in lines]
    except (OSError, ValueError, TypeError) as e:
        log.warning("Could not load stock tiers: %s", e)


def save_stock_tiers() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    STOCK_PATH.write_text(
        json.dumps(STOCK_BY_TIER, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def merge_stock_groups(tier: str, groups: dict[str, list[str]]) -> tuple[int, int]:
    if tier not in TIER_IDS:
        raise ValueError("invalid tier")
    t = STOCK_BY_TIER[tier]
    bins_touched = 0
    lines_added = 0
    for bin_key, lines in groups.items():
        bk = str(bin_key)
        arr = t.setdefault(bk, [])
        for line in lines:
            arr.append(str(line))
            lines_added += 1
        bins_touched += 1
    save_stock_tiers()
    return bins_touched, lines_added


def stock_tiers_api_payload() -> dict[str, Any]:
    out: dict[str, Any] = {}
    for tid in TIER_IDS:
        tier_map = STOCK_BY_TIER.get(tid) or {}
        bins = [
            {"bin": b, "count": len(lines)}
            for b, lines in sorted(tier_map.items(), key=lambda x: x[0])
        ]
        out[tid] = {"bins": bins}
    return out


@web.middleware
async def cors_middleware(
    request: web.Request, handler: Any
) -> web.StreamResponse:
    if request.method == "OPTIONS":
        return web.Response(status=204, headers=CORS_HEADERS)
    resp = await handler(request)
    for hk, hv in CORS_HEADERS.items():
        resp.headers[hk] = hv
    return resp


async def _leadbot_secret_denied(request: web.Request) -> web.Response | None:
    if not LEADBOT_API_SECRET:
        return None
    got = (request.headers.get("X-Leadbot-Secret") or "").strip()
    if got != LEADBOT_API_SECRET:
        return web.json_response({"ok": False, "error": "Unauthorized"}, status=401)
    return None


async def handle_stock_tiers(_request: web.Request) -> web.Response:
    return web.json_response(stock_tiers_api_payload())


async def handle_sync_groups(request: web.Request) -> web.Response:
    deny = await _leadbot_secret_denied(request)
    if deny is not None:
        return deny
    try:
        body = await request.json()
    except (json.JSONDecodeError, ValueError):
        return web.json_response({"ok": False, "error": "Invalid JSON"}, status=400)
    if not isinstance(body, dict):
        return web.json_response({"ok": False, "error": "Invalid body"}, status=400)
    groups = body.get("groups")
    tier = str(body.get("tier") or "")
    if not isinstance(groups, dict):
        return web.json_response({"ok": False, "error": "Missing groups"}, status=400)
    if tier not in TIER_IDS:
        return web.json_response({"ok": False, "error": "Invalid tier"}, status=400)
    clean: dict[str, list[str]] = {}
    for k, v in groups.items():
        bk = str(k)
        if isinstance(v, list):
            clean[bk] = [str(x) for x in v]
        else:
            clean[bk] = [str(v)]
    try:
        bins_touched, lines_added = merge_stock_groups(tier, clean)
    except ValueError as e:
        return web.json_response({"ok": False, "error": str(e)}, status=400)
    return web.json_response(
        {"ok": True, "bins_touched": bins_touched, "lines_added": lines_added}
    )


async def handle_sendout(request: web.Request) -> web.Response:
    deny = await _leadbot_secret_denied(request)
    if deny is not None:
        return deny
    if not ADMIN_USER_IDS:
        return web.json_response(
            {"ok": False, "error": "ADMIN_USER_IDS not set in .env"}, status=503
        )
    ptb_app: Application = request.app["ptb_app"]
    bot = ptb_app.bot
    summary_lines: list[str] = ["📤 <b>BIN sorter sendout</b>", ""]
    full_text_parts: list[str] = []
    total_lines = 0
    for tid in TIER_IDS:
        tier_map = STOCK_BY_TIER.get(tid) or {}
        n_bins = len(tier_map)
        n_lines = sum(len(lines) for lines in tier_map.values())
        total_lines += n_lines
        summary_lines.append(f"• <b>{tid}</b>: {n_bins} BIN(s), {n_lines} line(s)")
        full_text_parts.append(f"\n=== tier:{tid} ===\n")
        for bkey in sorted(tier_map.keys()):
            for line in tier_map[bkey]:
                full_text_parts.append(line)
    if total_lines == 0:
        return web.json_response(
            {"ok": False, "error": "No stock — run START in the HTML to sync groups first"},
            status=400,
        )
    summary_text = "\n".join(summary_lines)
    full_body = "\n".join(full_text_parts)
    use_pre = len(full_body) <= 3500
    failed = 0
    for aid in ADMIN_USER_IDS:
        try:
            await bot.send_message(chat_id=aid, text=summary_text, parse_mode="HTML")
            if use_pre:
                await bot.send_message(
                    chat_id=aid,
                    text=f"<pre>{html.escape(full_body)}</pre>",
                    parse_mode="HTML",
                )
            else:
                doc = io.BytesIO(full_body.encode("utf-8"))
                doc.name = "sendout_stock.txt"
                await bot.send_document(
                    chat_id=aid,
                    document=doc,
                    caption="📤 Full sendout dump",
                )
            await asyncio.sleep(0.05)
        except Exception:
            log.exception("Sendout failed for admin chat_id=%s", aid)
            failed += 1
    if failed == len(ADMIN_USER_IDS):
        return web.json_response(
            {"ok": False, "error": "Could not deliver to any admin (check bot / chat id)"},
            status=502,
        )
    return web.json_response({"ok": True})


_ROOT_HTML = """<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8"/><title>Leadbot API</title>
<style>body{font-family:system-ui,sans-serif;max-width:42rem;margin:2rem auto;padding:0 1rem;
background:#111;color:#e5e5e5;line-height:1.5}code{background:#222;padding:.15rem .4rem;border-radius:4px}
a{color:#f97316}h1{font-size:1.25rem}.ok{color:#86efac}</style></head><body>
<h1>Leadbot HTTP API</h1>
<p class="ok">Server is running.</p>
<p>This URL is only the <strong>API</strong> for the BIN sorter HTML tool — there is no web UI here.
Open your <code>deepseek_html_*.html</code> file locally (or host it yourself) and set its public API URL
to this deployment’s <strong>https</strong> base (Railway domain).</p>
<ul>
<li><code>GET /api/stock-tiers</code> — stock by tier (JSON)</li>
<li><code>POST /api/sync-groups</code> — sync after START in the HTML</li>
<li><code>POST /api/sendout</code> — send stock to Telegram admins</li>
</ul>
<p><a href="/api/stock-tiers">Try stock-tiers JSON</a></p>
</body></html>"""


async def handle_root(_request: web.Request) -> web.Response:
    return web.Response(text=_ROOT_HTML, content_type="text/html", charset="utf-8")


def _http_listen_port() -> int:
    """Railway (and similar) set PORT; local dev can use LEADBOT_HTTP_PORT."""
    for key in ("PORT", "LEADBOT_HTTP_PORT"):
        raw = os.getenv(key, "").strip()
        if raw.isdigit():
            return int(raw)
    return 8787


async def start_leadbot_http(ptb_application: Application) -> None:
    web_app = web.Application(middlewares=[cors_middleware])
    web_app["ptb_app"] = ptb_application
    web_app.router.add_get("/", handle_root)
    web_app.router.add_get("/api/stock-tiers", handle_stock_tiers)
    web_app.router.add_post("/api/sync-groups", handle_sync_groups)
    web_app.router.add_post("/api/sendout", handle_sendout)
    runner = web.AppRunner(web_app)
    await runner.setup()
    port = _http_listen_port()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    log.info(
        "Leadbot HTTP API on port %s — / + GET /api/stock-tiers, POST /api/sync-groups, /api/sendout",
        port,
    )


def resolve_header_image_path() -> Path | None:
    """Banner on /start: BOT_HEADER_IMAGE env, else assets/header.png|.jpg|…. Telegram menu buttons cannot show photos."""
    env = os.getenv("BOT_HEADER_IMAGE", "").strip()
    if env:
        p = Path(env).expanduser()
        if p.is_file():
            return p
        log.warning("BOT_HEADER_IMAGE set but not a file: %s", env)
    for name in ("header.png", "header.jpg", "header.jpeg", "header.webp"):
        candidate = ASSETS_DIR / name
        if candidate.is_file():
            return candidate
    return None


async def post_init(ptb_application: Application) -> None:
    load_stock_tiers()
    if resolve_header_image_path() is None:
        log.warning(
            "No header banner for /start — add %s/header.png or set BOT_HEADER_IMAGE in env",
            ASSETS_DIR,
        )
    await start_leadbot_http(ptb_application)


def load_known_users() -> set[int]:
    if not KNOWN_USERS_PATH.is_file():
        return set()
    try:
        data = json.loads(KNOWN_USERS_PATH.read_text(encoding="utf-8"))
        return {int(x) for x in data}
    except (OSError, ValueError, TypeError) as e:
        log.warning("Could not load %s: %s", KNOWN_USERS_PATH, e)
        return set()


def save_known_users(user_ids: set[int]) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    KNOWN_USERS_PATH.write_text(
        json.dumps(sorted(user_ids), indent=2),
        encoding="utf-8",
    )


known_user_ids: set[int] = load_known_users()


def _default_payment_store() -> dict[str, Any]:
    return {"next_id": 1, "claims": []}


def load_payment_store() -> dict[str, Any]:
    if not PAYMENTS_PATH.is_file():
        return _default_payment_store()
    try:
        return json.loads(PAYMENTS_PATH.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError) as e:
        log.warning("Could not load payments: %s", e)
        return _default_payment_store()


def save_payment_store(store: dict[str, Any]) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    PAYMENTS_PATH.write_text(
        json.dumps(store, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


def add_payment_claim(
    user: User, amount_usd: float, coin: str, pay_source: str
) -> dict[str, Any]:
    store = load_payment_store()
    cid = int(store["next_id"])
    store["next_id"] = cid + 1
    claim: dict[str, Any] = {
        "id": cid,
        "user_id": user.id,
        "username": user.username or "",
        "full_name": user.full_name or "",
        "amount_usd": float(amount_usd),
        "coin": coin,
        "pay_source": pay_source,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "status": "pending",
        "resolved_at": None,
        "resolved_by": None,
    }
    store["claims"].append(claim)
    save_payment_store(store)
    return claim


def apply_claim_resolution(
    claim_id: int,
    status: str,
    admin_id: int,
) -> tuple[bool, str, dict[str, Any] | None]:
    if status not in ("accepted", "rejected"):
        return False, "Invalid status", None
    store = load_payment_store()
    for c in store["claims"]:
        if int(c["id"]) != claim_id:
            continue
        if c["status"] != "pending":
            return False, f"Claim #{claim_id} is already {c['status']}.", c
        c["status"] = status
        c["resolved_at"] = datetime.now(timezone.utc).isoformat()
        c["resolved_by"] = admin_id
        if status == "accepted":
            bal_user = ensure_user(int(c["user_id"]))
            bal_user["balance"] += float(c["amount_usd"])
            bal_user["deposits"] += float(c["amount_usd"])
        save_payment_store(store)
        return True, "Updated.", c
    return False, f"Claim #{claim_id} not found.", None


def payment_user_stats() -> dict[str, Any]:
    store = load_payment_store()
    claimed_users = {int(c["user_id"]) for c in store["claims"]}
    pending = sum(1 for c in store["claims"] if c["status"] == "pending")
    accepted = sum(1 for c in store["claims"] if c["status"] == "accepted")
    rejected = sum(1 for c in store["claims"] if c["status"] == "rejected")
    total_users = len(known_user_ids)
    browse_only = len(known_user_ids - claimed_users)
    return {
        "total_users": total_users,
        "users_ever_claimed": len(claimed_users),
        "users_browse_only": browse_only,
        "pending": pending,
        "accepted": accepted,
        "rejected": rejected,
        "total_claims": len(store["claims"]),
    }


def list_pending_claims(limit: int = 25) -> list[dict[str, Any]]:
    store = load_payment_store()
    pend = [c for c in store["claims"] if c["status"] == "pending"]
    pend.sort(key=lambda x: int(x["id"]), reverse=True)
    return pend[:limit]


def list_recent_claims(limit: int = 30) -> list[dict[str, Any]]:
    store = load_payment_store()
    allc = list(store["claims"])
    allc.sort(key=lambda x: int(x["id"]), reverse=True)
    return allc[:limit]


def claim_detail_html(claim: dict[str, Any]) -> str:
    uname = f"@{claim['username']}" if claim.get("username") else "—"
    extra = ""
    if claim.get("resolved_at"):
        extra = f"\nResolved: <code>{html.escape(str(claim['resolved_at']))}</code>"
    return (
        f"📥 <b>Payment claim</b> #{claim['id']}\n\n"
        f"User: {html.escape(str(claim.get('full_name') or '—'))} "
        f"({html.escape(uname)})\n"
        f"ID: <code>{claim['user_id']}</code>\n"
        f"Amount: <b>{fmt_usd(float(claim['amount_usd']))}</b> USD\n"
        f"Coin: <b>{str(claim.get('coin', '')).upper()}</b>\n"
        f"Flow: <b>{html.escape(str(claim.get('pay_source', '')))}</b>\n"
        f"Status: <b>{html.escape(str(claim.get('status', '')))}</b>\n"
        f"Created: <code>{html.escape(str(claim.get('created_at', '')))}</code>"
        f"{extra}"
    )


def format_claim_oneline(c: dict[str, Any]) -> str:
    un = f"@{c['username']}" if c.get("username") else "—"
    return (
        f"#{c['id']} {html.escape(str(c.get('full_name') or '—'))} ({html.escape(un)}) "
        f"{fmt_usd(float(c['amount_usd']))} <b>{html.escape(str(c.get('coin', '')).upper())}</b> "
        f"<i>{html.escape(str(c.get('status', '')))}</i>"
    )


async def notify_admins_new_claim(bot, claim: dict[str, Any]) -> None:
    if not ADMIN_USER_IDS:
        return
    text = claim_detail_html(claim)
    kb = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("✅ Accept", callback_data=f"adm_acc_{claim['id']}"),
                InlineKeyboardButton("❌ Reject", callback_data=f"adm_rej_{claim['id']}"),
            ]
        ]
    )
    for aid in ADMIN_USER_IDS:
        try:
            await bot.send_message(
                chat_id=aid,
                text=text,
                parse_mode="HTML",
                reply_markup=kb,
            )
            await asyncio.sleep(0.04)
        except Exception:
            log.info("Admin notify failed aid=%s", aid, exc_info=True)


async def admin_claim_button_action(
    query,
    context: ContextTypes.DEFAULT_TYPE,
    data: str,
    admin_user: User,
) -> None:
    if not is_admin(admin_user.id):
        await query.answer("Not authorized.", show_alert=True)
        return
    if data.startswith("adm_acc_"):
        status = "accepted"
        prefix = "adm_acc_"
    elif data.startswith("adm_rej_"):
        status = "rejected"
        prefix = "adm_rej_"
    else:
        return
    try:
        cid = int(data.removeprefix(prefix))
    except ValueError:
        await query.answer("Invalid claim id.", show_alert=True)
        return
    ok, msg, claim = apply_claim_resolution(cid, status, admin_user.id)
    if not ok or not claim:
        await query.answer(msg[:200], show_alert=True)
        return
    await query.answer("Saved.")
    try:
        await query.edit_message_text(
            claim_detail_html(claim),
            parse_mode="HTML",
            reply_markup=None,
        )
    except Exception:
        log.warning("Could not edit admin claim message", exc_info=True)


def register_known_user(user_id: int) -> None:
    if user_id not in known_user_ids:
        known_user_ids.add(user_id)
        save_known_users(known_user_ids)


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_USER_IDS


def ensure_user(user_id: int) -> dict[str, Any]:
    register_known_user(user_id)
    if user_id not in USERS:
        USERS[user_id] = {
            "balance": 0.0,
            "deposits": 0.0,
            "spent": 0.0,
            "status": "active",
        }
    return USERS[user_id]


def fmt_usd(n: float) -> str:
    return f"${n:,.2f}"


def hub_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("💰 My Balance", callback_data="m_bal"),
                InlineKeyboardButton("💳 Top Up", callback_data="m_top"),
            ],
            [
                InlineKeyboardButton("💳 Buy CCs", callback_data="m_buy"),
                InlineKeyboardButton("🛒 My Cart", callback_data="m_cart"),
            ],
            [InlineKeyboardButton("👤 My Profile", callback_data="m_prof")],
        ]
    )


TIER_PRICES: dict[str, float] = {
    "random": 5.0,
    "70": 10.0,
    "80": 15.0,
    "90": 20.0,
    "100": 25.0,
}

BUY_CALLBACK_TO_TIER: dict[str, str] = {
    "buy_random": "random",
    "buy_70": "70",
    "buy_80": "80",
    "buy_90": "90",
    "buy_100": "100",
}

TIER_DISPLAY_TITLE: dict[str, str] = {
    "random": "🎲 Random validity",
    "70": "70% validity",
    "80": "80% validity",
    "90": "90% validity",
    "100": "100% validity",
}

BUY_MENU_ORDER: tuple[str, ...] = (
    "buy_random",
    "buy_70",
    "buy_80",
    "buy_90",
    "buy_100",
)


def tier_total_line_count(tier_key: str) -> int:
    t = STOCK_BY_TIER.get(tier_key) or {}
    return sum(len(lines) for lines in t.values())


def buy_menu_keyboard() -> InlineKeyboardMarkup:
    """Tier buttons show live stock; empty tiers are Out of stock (tap shows alert)."""
    load_stock_tiers()
    rows: list[list[InlineKeyboardButton]] = []
    for cb in BUY_MENU_ORDER:
        tier_key = BUY_CALLBACK_TO_TIER[cb]
        price = TIER_PRICES[tier_key]
        title = TIER_DISPLAY_TITLE.get(tier_key, tier_key)
        n = tier_total_line_count(tier_key)
        if n <= 0:
            rows.append(
                [
                    InlineKeyboardButton(
                        f"{title} · {fmt_usd(price)} · Out of stock",
                        callback_data=f"oos:{tier_key}",
                    )
                ]
            )
        else:
            rows.append(
                [
                    InlineKeyboardButton(
                        f"{title} · {fmt_usd(price)} ({n} lines)",
                        callback_data=cb,
                    )
                ]
            )
    rows.append([InlineKeyboardButton("⬅️ Back", callback_data="buy_back")])
    return InlineKeyboardMarkup(rows)


BUY_CATALOG_PAGE_SIZE = 8
# Telegram allows up to 128 chars on inline button text; cap for readability.
BIN_BTN_TEXT_MAX = 128


def extract_city_state_from_line(line: str) -> tuple[str, str]:
    """Pipe format: card|mm|yy|cvv|name|address|city|state|... (matches HTML sorter)."""
    parts = line.split("|")
    if len(parts) < 8:
        return "?", "?"
    city = (parts[6] or "").strip().strip('"').strip()
    state = (parts[7] or "").strip().strip('"').strip().upper()
    if not city:
        city = "?"
    if not state:
        state = "?"
    elif len(state) > 3:
        state = state[:2].upper()
    else:
        state = state.upper()
    return city[:20], state[:2]


def primary_location_label(lines: list[str]) -> str:
    """Most common city, ST from stock lines (same idea as BIN chips in HTML)."""
    freq: dict[str, int] = {}
    for line in lines:
        city, st = extract_city_state_from_line(line)
        if city == "?" or st in ("", "?"):
            continue
        key = f"{city}, {st}"
        freq[key] = freq.get(key, 0) + 1
    if not freq:
        return ""
    return max(freq.items(), key=lambda x: (x[1], x[0]))[0]


def format_bin_row_button_text(
    bin_key: str, line_count: int, price: float, location: str
) -> str:
    price_s = fmt_usd(price)
    core = f"{bin_key} ×{line_count} · {price_s}"
    if not location:
        return core[:BIN_BTN_TEXT_MAX]
    extra = f" · {location}"
    if len(core + extra) <= BIN_BTN_TEXT_MAX:
        return core + extra
    room = BIN_BTN_TEXT_MAX - len(core) - 3
    if room < 4:
        return core[:BIN_BTN_TEXT_MAX]
    loc_short = location[:room] + ("…" if len(location) > room else "")
    return core + " · " + loc_short


def tier_catalog_text_and_keyboard(
    tier_key: str, user_id: int, page: int = 0
) -> tuple[str, InlineKeyboardMarkup]:
    load_stock_tiers()
    u = ensure_user(user_id)
    price = TIER_PRICES[tier_key]
    title = TIER_DISPLAY_TITLE.get(tier_key, tier_key)
    tier_stock = STOCK_BY_TIER.get(tier_key) or {}
    bins_sorted = sorted(tier_stock.keys())
    total_bins = len(bins_sorted)
    total_lines = sum(len(v) for v in tier_stock.values())

    if total_bins == 0:
        text = (
            f"💳 <b>{html.escape(title)}</b>\n"
            f"Price: <b>{fmt_usd(price)}</b> per line · "
            f"Your balance: <b>{fmt_usd(u['balance'])}</b>\n\n"
            "<b>Out of stock.</b> Nothing listed for this tier.\n"
            "Sync stock from the BIN sorter (HTML tool), then open Buy CCs again."
        )
        kb = InlineKeyboardMarkup(
            [[InlineKeyboardButton("⬅️ Back", callback_data="buy_tier_back")]]
        )
        return text, kb

    start = page * BUY_CATALOG_PAGE_SIZE
    chunk = bins_sorted[start : start + BUY_CATALOG_PAGE_SIZE]
    more_note = ""
    if total_bins > BUY_CATALOG_PAGE_SIZE:
        more_note = (
            f"\n<i>Showing {start + 1}–{start + len(chunk)} of {total_bins} BINs</i>"
        )

    text = (
        f"💳 <b>{html.escape(title)}</b>\n"
        f"Price: <b>{fmt_usd(price)}</b> per line · "
        f"Lines in stock: <b>{total_lines}</b>\n"
        f"Your balance: <b>{fmt_usd(u['balance'])}</b>\n\n"
        "Tap a <b>BIN</b> to buy <b>one</b> line from that group."
        f"{more_note}"
    )

    rows: list[list[InlineKeyboardButton]] = []
    for bk in chunk:
        cnt = len(tier_stock[bk])
        loc = primary_location_label(tier_stock[bk])
        btn_text = format_bin_row_button_text(bk, cnt, price, loc)
        rows.append(
            [
                InlineKeyboardButton(
                    btn_text,
                    callback_data=f"bpr:{tier_key}:{bk}",
                )
            ]
        )

    nav_row: list[InlineKeyboardButton] = []
    if page > 0:
        nav_row.append(
            InlineKeyboardButton(
                "« Prev", callback_data=f"bpg:{tier_key}:{page - 1}"
            )
        )
    if start + BUY_CATALOG_PAGE_SIZE < total_bins:
        nav_row.append(
            InlineKeyboardButton(
                "Next »", callback_data=f"bpg:{tier_key}:{page + 1}"
            )
        )
    if nav_row:
        rows.append(nav_row)

    rows.append([InlineKeyboardButton("⬅️ Back", callback_data="buy_tier_back")])
    return text, InlineKeyboardMarkup(rows)


async def handle_buy_product(
    query, user: User, data: str
) -> None:
    parts = data.split(":", 2)
    if len(parts) != 3 or parts[0] != "bpr":
        await query.answer("Invalid selection.", show_alert=True)
        return
    _, tier_key, bin_key = parts
    if tier_key not in TIER_PRICES:
        await query.answer("Invalid tier.", show_alert=True)
        return
    load_stock_tiers()
    tier_stock = STOCK_BY_TIER.get(tier_key) or {}
    lines = tier_stock.get(bin_key)
    if not lines:
        await query.answer(
            "This BIN is empty or sold out. Refresh the list.",
            show_alert=True,
        )
        return
    price = TIER_PRICES[tier_key]
    u = ensure_user(user.id)
    if float(u["balance"]) < price:
        await query.answer(
            f"Insufficient balance.\nYou have {fmt_usd(float(u['balance']))}.\n"
            f"This line costs {fmt_usd(price)}.\nTop up under 💳 Top Up.",
            show_alert=True,
        )
        return
    line = lines.pop(0)
    if not lines:
        del tier_stock[bin_key]
    save_stock_tiers()
    u["balance"] = float(u["balance"]) - price
    u["spent"] = float(u["spent"]) + price
    await query.answer("Purchased! Delivered below.", show_alert=False)
    escaped = html.escape(line)
    await query.message.reply_text(
        f"✅ <b>Delivered</b> · {html.escape(TIER_DISPLAY_TITLE.get(tier_key, tier_key))}\n"
        f"BIN <code>{html.escape(bin_key)}</code> · paid {fmt_usd(price)}\n\n"
        f"<code>{escaped}</code>",
        parse_mode="HTML",
    )


async def handle_buy_catalog_page(query, user: User, data: str) -> None:
    parts = data.split(":")
    if len(parts) != 3 or parts[0] != "bpg":
        await query.answer("Invalid.", show_alert=True)
        return
    tier_key = parts[1]
    try:
        page = int(parts[2])
    except ValueError:
        await query.answer("Invalid page.", show_alert=True)
        return
    if tier_key not in TIER_PRICES or page < 0:
        await query.answer("Invalid.", show_alert=True)
        return
    text, kb = tier_catalog_text_and_keyboard(tier_key, user.id, page=page)
    await edit_safe(query, text, kb)
    await query.answer()


def topup_amount_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("$10", callback_data="tu_10"),
                InlineKeyboardButton("$100", callback_data="tu_100"),
                InlineKeyboardButton("$200", callback_data="tu_200"),
            ],
            [
                InlineKeyboardButton("$500", callback_data="tu_500"),
                InlineKeyboardButton("$1,000", callback_data="tu_1000"),
                InlineKeyboardButton("Custom", callback_data="tu_custom"),
            ],
            [InlineKeyboardButton("⬅️ Back", callback_data="tu_back")],
        ]
    )


def pay_method_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("₿ BTC", callback_data="pay_btc"),
                InlineKeyboardButton("Ł LTC", callback_data="pay_ltc"),
            ],
            [InlineKeyboardButton("⬅️ Back", callback_data="pay_m_back")],
        ]
    )


def coin_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("📥 Submit — I sent payment", callback_data="pay_submit")],
            [InlineKeyboardButton("⬅️ Back", callback_data="pay_coin_back")],
        ]
    )


TOPUP_TEXT = """💰 <b>Top-Up Your Balance</b>

Select an amount to add to your balance:

• Minimum deposit: <b>$10</b>
• Payment methods: Crypto (BTC, LTC)

Your balance will be updated automatically after payment confirmation."""


def profile_html(user: User) -> str:
    u = ensure_user(user.id)
    name = html.escape(user.full_name or "—")
    uname = user.username or "—"
    if uname != "—":
        uname_h = f'<a href="https://t.me/{html.escape(user.username or "")}">@{html.escape(user.username or "")}</a>'
    else:
        uname_h = "—"
    return (
        "👤 <b>Profile</b>\n\n"
        f"Name: <b>{name}</b>\n"
        f"Username: {uname_h}\n"
        f"Telegram ID: <code>{user.id}</code>\n\n"
        f"Balance: <b>{fmt_usd(u['balance'])}</b>\n"
        f"Total Deposits: <b>{fmt_usd(u['deposits'])}</b>\n"
        f"Total Spent: <b>{fmt_usd(u['spent'])}</b>\n\n"
        f"Status: <b>{html.escape(str(u['status']))}</b>"
    )


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_user:
        return
    ensure_user(update.effective_user.id)
    markup = hub_keyboard()
    photo_path = resolve_header_image_path()
    if photo_path is not None:
        with photo_path.open("rb") as f:
            await update.message.reply_photo(
                photo=f,
                caption=CAPTION,
                reply_markup=markup,
            )
    else:
        await update.message.reply_text(
            CAPTION,
            reply_markup=markup,
        )


def pay_method_text(amount: float) -> str:
    return (
        "💰 <b>SELECT PAYMENT METHOD</b>\n\n"
        f"Invoice Amount: <b>{fmt_usd(amount)} USD</b>\n\n"
        "<b>Available Cryptocurrencies:</b> Choose your preferred payment method:"
    )


def coin_invoice_text(coin: str, amount: float, address: str) -> str:
    labels = {
        "btc": ("₿", "Bitcoin (BTC)"),
        "ltc": ("Ł", "Litecoin (LTC)"),
    }
    sym, title = labels[coin]
    addr = html.escape(address)
    return (
        f"{sym} <b>{title}</b>\n\n"
        f"Invoice: <b>{fmt_usd(amount)} USD</b>\n\n"
        "Send crypto to:\n"
        f"<code>{addr}</code>\n\n"
        "After you send, tap <b>Submit — I sent payment</b> below. "
        "An admin will verify on-chain and credit your balance."
    )


async def delete_message_safe(msg) -> None:
    try:
        await msg.delete()
    except Exception:
        log.warning("Could not delete message", exc_info=True)


async def edit_safe(query, text: str, reply_markup: InlineKeyboardMarkup) -> None:
    try:
        await query.edit_message_text(
            text=text,
            reply_markup=reply_markup,
            parse_mode="HTML",
            disable_web_page_preview=True,
        )
    except Exception:
        log.warning("Could not edit message", exc_info=True)


async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    user = update.effective_user
    if not query or not query.message or not user:
        return
    data = query.data or ""

    if data.startswith("adm_acc_") or data.startswith("adm_rej_"):
        await admin_claim_button_action(query, context, data, user)
        return

    if data.startswith("bpr:"):
        ensure_user(user.id)
        await handle_buy_product(query, user, data)
        return
    if data.startswith("bpg:"):
        ensure_user(user.id)
        await handle_buy_catalog_page(query, user, data)
        return
    if data == "buy_tier_back":
        ensure_user(user.id)
        await edit_safe(
            query,
            "💳 <b>Select a tier</b>",
            buy_menu_keyboard(),
        )
        await query.answer()
        return

    if data.startswith("oos:"):
        ensure_user(user.id)
        await query.answer(
            "Out of stock — add lines for this tier in the BIN sorter, then tap Buy CCs again.",
            show_alert=True,
        )
        return

    await query.answer()
    ensure_user(user.id)

    if data == "m_bal":
        u = ensure_user(user.id)
        bal_text = (
            "💰 <b>My Balance</b>\n\n"
            f"Balance: <b>{fmt_usd(u['balance'])}</b>\n"
            f"Total Deposits: <b>{fmt_usd(u['deposits'])}</b>\n"
            f"Total Spent: <b>{fmt_usd(u['spent'])}</b>"
        )
        await query.message.reply_text(
            bal_text,
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("⬅️ Back", callback_data="bal_back")]]
            ),
            parse_mode="HTML",
        )
        return

    if data == "bal_back":
        await delete_message_safe(query.message)
        return

    if data == "m_prof":
        await query.message.reply_text(
            profile_html(user),
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("⬅️ Back", callback_data="prof_back")]]
            ),
            parse_mode="HTML",
            disable_web_page_preview=True,
        )
        return

    if data == "prof_back":
        await delete_message_safe(query.message)
        return

    if data == "m_top":
        await query.message.reply_text(
            TOPUP_TEXT,
            reply_markup=topup_amount_keyboard(),
            parse_mode="HTML",
        )
        return

    if data == "tu_back":
        await delete_message_safe(query.message)
        return

    if data == "tu_custom":
        await edit_safe(
            query,
            "Custom amount: minimum deposit is <b>$10</b>. Please pick a preset.",
            InlineKeyboardMarkup(
                [[InlineKeyboardButton("⬅️ Back", callback_data="tu_restart")]]
            ),
        )
        return

    if data == "tu_restart":
        await edit_safe(query, TOPUP_TEXT, topup_amount_keyboard())
        return

    topup_map = {
        "tu_10": 10.0,
        "tu_100": 100.0,
        "tu_200": 200.0,
        "tu_500": 500.0,
        "tu_1000": 1000.0,
    }
    if data in topup_map:
        amt = topup_map[data]
        context.user_data["pending_invoice"] = amt
        context.user_data["pay_source"] = "topup"
        await edit_safe(query, pay_method_text(amt), pay_method_keyboard())
        return

    if data in ("m_cart", "m_buy"):
        await query.message.reply_text(
            "💳 <b>Select a tier</b>",
            reply_markup=buy_menu_keyboard(),
            parse_mode="HTML",
        )
        return

    if data == "buy_back":
        await delete_message_safe(query.message)
        return

    if data in BUY_CALLBACK_TO_TIER:
        tier_key = BUY_CALLBACK_TO_TIER[data]
        text, kb = tier_catalog_text_and_keyboard(tier_key, user.id, page=0)
        await edit_safe(query, text, kb)
        return

    if data == "pay_m_back":
        src = context.user_data.get("pay_source")
        if src == "topup":
            await edit_safe(query, TOPUP_TEXT, topup_amount_keyboard())
        elif src == "cart":
            await edit_safe(
                query,
                "💳 <b>Select a tier</b>",
                buy_menu_keyboard(),
            )
        return

    coin_addrs = {"btc": BTC_ADDR, "ltc": LTC_ADDR}
    if data in ("pay_btc", "pay_ltc"):
        coin = data.replace("pay_", "")
        context.user_data["pay_coin"] = coin
        amt = float(context.user_data.get("pending_invoice") or 0.0)
        await edit_safe(
            query,
            coin_invoice_text(coin, amt, coin_addrs[coin]),
            coin_keyboard(),
        )
        return

    if data == "pay_coin_back":
        amt = float(context.user_data.get("pending_invoice") or 0.0)
        await edit_safe(query, pay_method_text(amt), pay_method_keyboard())
        return

    if data == "pay_submit":
        coin = str(context.user_data.get("pay_coin") or "?")
        amt = float(context.user_data.get("pending_invoice") or 0.0)
        src = str(context.user_data.get("pay_source") or "?")
        claim = add_payment_claim(user, amt, coin, src)
        await notify_admins_new_claim(context.bot, claim)
        await edit_safe(
            query,
            "✅ <b>Payment submitted</b>\n\n"
            f"Claim reference: <b>#{claim['id']}</b>\n"
            "An admin will verify on-chain and credit your balance.",
            InlineKeyboardMarkup(
                [[InlineKeyboardButton("⬅️ Back", callback_data="pay_done_back")]]
            ),
        )
        return

    if data == "pay_done_back":
        src = context.user_data.get("pay_source")
        if src == "topup":
            await edit_safe(query, TOPUP_TEXT, topup_amount_keyboard())
        elif src == "cart":
            await edit_safe(
                query,
                "💳 <b>Select a tier</b>",
                buy_menu_keyboard(),
            )
        else:
            await delete_message_safe(query.message)
        return


def _announce_body(update: Update, context: ContextTypes.DEFAULT_TYPE) -> str | None:
    msg = update.message
    if not msg:
        return None
    if msg.reply_to_message:
        r = msg.reply_to_message
        return (r.text or r.caption or "").strip() or None
    args = context.args
    if args:
        return " ".join(args).strip() or None
    return None


async def cmd_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_user:
        return
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Not authorized.")
        return
    st = payment_user_stats()
    panel = (
        "🔐 <b>Admin portal</b>\n\n"
        "<b>/payportal</b> — payments: claims vs browsing-only users\n"
        "<b>/pendingclaims</b> — queue · <b>/allclaims</b> — history\n"
        "<b>/accept &lt;id&gt;</b> · <b>/reject &lt;id&gt;</b>\n\n"
        "<b>/users</b> — broadcast list size\n"
        "<b>/announce</b> — DM everyone (reply or text after command)\n\n"
        f"Subscribers: <b>{len(known_user_ids)}</b>\n"
        f"⏳ Pending claims: <b>{st['pending']}</b>"
    )
    await update.message.reply_text(panel, parse_mode="HTML")


async def cmd_users(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_user:
        return
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Not authorized.")
        return
    await update.message.reply_text(
        f"📊 <b>Broadcast list</b>\n\n"
        f"Users who used the bot: <b>{len(known_user_ids)}</b>",
        parse_mode="HTML",
    )


async def cmd_announce(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_user:
        return
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Not authorized.")
        return
    if not ADMIN_USER_IDS:
        await update.message.reply_text(
            "Set <code>ADMIN_USER_IDS</code> in <code>.env</code> first.",
            parse_mode="HTML",
        )
        return
    body = _announce_body(update, context)
    if not body:
        await update.message.reply_text(
            "Usage:\n"
            "/announce Your message here…\n"
            "Or reply to a message and send /announce (sends that message’s text).",
            parse_mode="HTML",
        )
        return
    if not known_user_ids:
        await update.message.reply_text("No subscribers yet (nobody has used /start).")
        return
    await update.message.reply_text(
        f"Sending to <b>{len(known_user_ids)}</b> users…",
        parse_mode="HTML",
    )
    ok, failed = 0, 0
    for uid in sorted(known_user_ids):
        try:
            await context.bot.send_message(chat_id=uid, text=body)
            ok += 1
            await asyncio.sleep(0.04)
        except Exception:
            failed += 1
            log.info("Broadcast failed for chat_id=%s", uid, exc_info=True)
    await update.message.reply_text(
        f"✅ Delivered: <b>{ok}</b>\n❌ Failed: <b>{failed}</b>",
        parse_mode="HTML",
    )


async def cmd_payportal(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_user:
        return
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Not authorized.")
        return
    s = payment_user_stats()
    text = (
        "💳 <b>Payment portal</b>\n\n"
        f"👥 Users who opened the bot: <b>{s['total_users']}</b>\n"
        f"🧾 Users who submitted “payment sent” (any time): <b>{s['users_ever_claimed']}</b>\n"
        f"👀 Browsing only (never filed a claim): <b>{s['users_browse_only']}</b>\n\n"
        f"Claims — ⏳ <b>{s['pending']}</b> pending · "
        f"✅ <b>{s['accepted']}</b> accepted · "
        f"❌ <b>{s['rejected']}</b> rejected\n"
        f"Total rows: <b>{s['total_claims']}</b>\n\n"
        "<b>/pendingclaims</b> — pending queue\n"
        "<b>/allclaims</b> — recent claims (all statuses)\n"
        "<b>/accept 12</b> / <b>/reject 12</b> — by claim #"
    )
    await update.message.reply_text(text, parse_mode="HTML")


async def cmd_pendingclaims(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_user:
        return
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Not authorized.")
        return
    lines = [format_claim_oneline(c) for c in list_pending_claims(30)]
    body = "\n".join(lines) if lines else "<i>No pending claims.</i>"
    await update.message.reply_text(
        "⏳ <b>Pending payment claims</b>\n\n" + body,
        parse_mode="HTML",
    )


async def cmd_allclaims(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_user:
        return
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Not authorized.")
        return
    lim = 30
    if context.args:
        try:
            lim = max(1, min(80, int(context.args[0])))
        except ValueError:
            pass
    lines = [format_claim_oneline(c) for c in list_recent_claims(lim)]
    body = "\n".join(lines) if lines else "<i>No claims yet.</i>"
    await update.message.reply_text(
        f"📋 <b>Recent claims</b> (last {lim})\n\n" + body,
        parse_mode="HTML",
    )


async def cmd_accept(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_user:
        return
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Not authorized.")
        return
    if not context.args:
        await update.message.reply_text(
            "Usage: /accept &lt;claim_id&gt;", parse_mode="HTML"
        )
        return
    try:
        cid = int(context.args[0])
    except ValueError:
        await update.message.reply_text("Claim id must be a number.")
        return
    ok, msg, claim = apply_claim_resolution(cid, "accepted", update.effective_user.id)
    if ok and claim:
        await update.message.reply_text(claim_detail_html(claim), parse_mode="HTML")
    else:
        await update.message.reply_text(html.escape(msg), parse_mode="HTML")


async def cmd_reject(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_user:
        return
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Not authorized.")
        return
    if not context.args:
        await update.message.reply_text(
            "Usage: /reject &lt;claim_id&gt;", parse_mode="HTML"
        )
        return
    try:
        cid = int(context.args[0])
    except ValueError:
        await update.message.reply_text("Claim id must be a number.")
        return
    ok, msg, claim = apply_claim_resolution(cid, "rejected", update.effective_user.id)
    if ok and claim:
        await update.message.reply_text(claim_detail_html(claim), parse_mode="HTML")
    else:
        await update.message.reply_text(html.escape(msg), parse_mode="HTML")


async def error_handler(_update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    err = context.error
    if isinstance(err, Conflict):
        log.error(
            "Telegram 409 Conflict: another client is already calling getUpdates for this bot. "
            "Only ONE process may poll. Stop: (1) bot.py on your PC if Railway is running, "
            "(2) duplicate Railway services using the same token, (3) extra replicas — use exactly 1. "
            "Webhook bots elsewhere must be disabled."
        )
        return
    log.exception("Unhandled exception in handler", exc_info=err)


def main() -> None:
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    if not token:
        raise SystemExit(
            "Set TELEGRAM_BOT_TOKEN in a .env file (see .env.example)."
        )
    app = Application.builder().token(token).post_init(post_init).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("admin", cmd_admin))
    app.add_handler(CommandHandler("users", cmd_users))
    app.add_handler(CommandHandler("announce", cmd_announce))
    app.add_handler(CommandHandler("payportal", cmd_payportal))
    app.add_handler(CommandHandler("pendingclaims", cmd_pendingclaims))
    app.add_handler(CommandHandler("allclaims", cmd_allclaims))
    app.add_handler(CommandHandler("accept", cmd_accept))
    app.add_handler(CommandHandler("reject", cmd_reject))
    app.add_handler(CallbackQueryHandler(on_callback))
    app.add_error_handler(error_handler)
    if not ADMIN_USER_IDS:
        log.warning("ADMIN_USER_IDS is empty — set it in .env to use /admin and /announce")
    log.info("Bot running — press Ctrl+C to stop")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
