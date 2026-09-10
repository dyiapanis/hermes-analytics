"""SQLite layer — per-profile DB path, schema, batched background writer,
retention, session helpers, context-pressure dedup."""

from __future__ import annotations
import csv
import functools
import hashlib
import json
import logging
import os
import queue
import sqlite3
import threading
import time
from collections import deque
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


# ── Per-profile DB path ────────────────────────────────────────────────────

_DB_PATH: Optional[str] = None


def _get_db_path() -> str:
    global _DB_PATH
    if _DB_PATH is None:
        # Tier 0: explicit env var (standalone / non-Hermes deployments)
        env_path = os.environ.get("ANALYTICS_DB_PATH")
        if env_path:
            _DB_PATH = os.path.expanduser(env_path)
            return _DB_PATH

        # Tier 1: Hermes profile-aware path
        hermes_home = os.environ.get("HERMES_HOME", os.path.expanduser("~/.hermes"))
        parent_name = os.path.basename(os.path.dirname(hermes_home))
        if parent_name == "profiles":
            root = os.path.dirname(os.path.dirname(hermes_home))
        elif os.path.isdir(os.path.join(hermes_home, "profiles")):
            root = hermes_home
        else:
            root = os.path.expanduser("~/.hermes")
        active = os.environ.get("HERMES_PROFILE")
        if not active:
            active = os.path.basename(hermes_home) if parent_name == "profiles" else "phoenix"
        _DB_PATH = os.path.join(root, "profiles", active, "analytics.db")
    return _DB_PATH


# ── SQLite connection ─────────────────────────────────────────────────────

_DB_CONN: Optional[Any] = None
_SCHEMA_READY = False
_conn_lock = threading.Lock()

# SQLite — no namespace/database concept needed


def _ensure_schema(conn: Any) -> None:
    global _SCHEMA_READY
    if _SCHEMA_READY:
        return

    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")

    # Tables with explicit schema (allow NULLs for optional fields)
    conn.execute("""CREATE TABLE IF NOT EXISTS llm_calls (
        session_id TEXT, task_id TEXT, model TEXT,
        prompt_tokens INTEGER, completion_tokens INTEGER, total_tokens INTEGER,
        cache_read_tokens INTEGER, cost_usd REAL, cost_status TEXT, cost_source TEXT,
        duration_ms INTEGER, status TEXT, turn_id TEXT, api_request_id TEXT,
        provider TEXT, finish_reason TEXT,
        timestamp INTEGER DEFAULT (strftime('s','now'))
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS tool_calls (
        session_id TEXT, task_id TEXT, tool_name TEXT, status TEXT,
        duration_ms INTEGER, args_hash TEXT, error_message TEXT,
        exit_code INTEGER, turn_id TEXT, tool_call_id TEXT, error_type TEXT,
        timestamp INTEGER DEFAULT (strftime('s','now'))
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS context_pressure (
        session_id TEXT, turn_number INTEGER, context_used_chars INTEGER,
        context_limit_chars INTEGER, utilization_pct REAL,
        compression_triggered INTEGER, model TEXT,
        timestamp INTEGER DEFAULT (strftime('s','now'))
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS session_summary (
        session_id TEXT PRIMARY KEY, platform TEXT,
        start_time INTEGER, end_time INTEGER,
        total_llm_calls INTEGER DEFAULT 0, total_tool_calls INTEGER DEFAULT 0,
        total_prompt_tokens INTEGER DEFAULT 0, total_completion_tokens INTEGER DEFAULT 0,
        total_cost_usd REAL DEFAULT 0, final_status TEXT
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS skill_usage (
        session_id TEXT, task_id TEXT, skill_name TEXT, action TEXT,
        timestamp INTEGER DEFAULT (strftime('s','now'))
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS approval_events (
        event_type TEXT, session_id TEXT, command_preview TEXT,
        platform TEXT, approved INTEGER,
        timestamp INTEGER DEFAULT (strftime('s','now'))
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS kanban_events (
        event_type TEXT, task_id TEXT, board TEXT, assignee TEXT,
        run_id INTEGER, profile_name TEXT, summary TEXT, reason TEXT,
        timestamp INTEGER DEFAULT (strftime('s','now'))
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS api_requests (
        session_id TEXT, task_id TEXT, turn_id TEXT, api_request_id TEXT,
        model TEXT, provider TEXT, api_mode TEXT, api_call_count INTEGER,
        message_count INTEGER, tool_count INTEGER, approx_input_tokens INTEGER,
        request_char_count INTEGER, max_tokens INTEGER, started_at REAL,
        timestamp INTEGER DEFAULT (strftime('s','now'))
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS inbound_messages (
        platform TEXT, chat_id TEXT, user_id TEXT, user_name TEXT,
        thread_id TEXT, reply_to_id TEXT, message_length INTEGER,
        message_preview TEXT,
        timestamp INTEGER DEFAULT (strftime('s','now'))
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS verify_events (
        session_id TEXT, model TEXT, platform TEXT, attempt INTEGER,
        edited_file_count INTEGER, edited_paths TEXT, response_length INTEGER,
        timestamp INTEGER DEFAULT (strftime('s','now'))
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS _schema_meta (
        version TEXT, stamped_at INTEGER, plugin_version TEXT
    )""")

    # Schema version stamp
    _SCHEMA_VERSION = "1.0"
    existing = None
    try:
        row = conn.execute("SELECT version FROM _schema_meta LIMIT 1").fetchone()
        if row:
            existing = row[0]
    except Exception:
        pass
    if existing != _SCHEMA_VERSION:
        conn.execute("DELETE FROM _schema_meta")
        conn.execute("INSERT INTO _schema_meta (version, stamped_at, plugin_version) VALUES (?, ?, ?)",
                     (_SCHEMA_VERSION, int(time.time()), _PLUGIN_VERSION))

    # Indexes
    conn.execute("CREATE INDEX IF NOT EXISTS llm_calls_sid_idx ON llm_calls(session_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS llm_calls_ts_idx ON llm_calls(timestamp)")
    conn.execute("CREATE INDEX IF NOT EXISTS llm_calls_model_idx ON llm_calls(model)")
    conn.execute("CREATE INDEX IF NOT EXISTS llm_calls_sid_ts_idx ON llm_calls(session_id, timestamp)")
    conn.execute("CREATE INDEX IF NOT EXISTS tool_calls_sid_idx ON tool_calls(session_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS tool_calls_ts_idx ON tool_calls(timestamp)")
    conn.execute("CREATE INDEX IF NOT EXISTS tool_calls_tool_idx ON tool_calls(tool_name)")
    conn.execute("CREATE INDEX IF NOT EXISTS tool_calls_status_idx ON tool_calls(status)")
    conn.execute("CREATE INDEX IF NOT EXISTS ctx_sid_idx ON context_pressure(session_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS skill_sid_idx ON skill_usage(session_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS skill_name_idx ON skill_usage(skill_name)")
    conn.execute("CREATE INDEX IF NOT EXISTS api_req_sid_idx ON api_requests(session_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS api_req_req_id_idx ON api_requests(api_request_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS api_req_ts_idx ON api_requests(timestamp)")
    conn.execute("CREATE INDEX IF NOT EXISTS inbound_platform_idx ON inbound_messages(platform)")
    conn.execute("CREATE INDEX IF NOT EXISTS inbound_ts_idx ON inbound_messages(timestamp)")
    conn.execute("CREATE INDEX IF NOT EXISTS inbound_chat_idx ON inbound_messages(chat_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS verify_sid_idx ON verify_events(session_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS verify_model_idx ON verify_events(model)")

    conn.commit()
    _SCHEMA_READY = True


def _db() -> Any:
    global _DB_CONN
    if _DB_CONN is None:
        path = _get_db_path()
        os.makedirs(os.path.dirname(path), exist_ok=True)
        _DB_CONN = sqlite3.connect(path, timeout=10, check_same_thread=False)
        _DB_CONN.row_factory = sqlite3.Row
        _ensure_schema(_DB_CONN)
    return _DB_CONN


# ── Retention ────────────────────────────────────────────────────────────────

_RETENTION_DAYS: int | None = None
_RETAIN_INTERVAL_S = 3600  # Run at most once per hour
_last_retain_ts = 0.0
_retention_thread: Optional[threading.Thread] = None


def _get_retention_days() -> int:
    """Load retention_days from plugin.yaml (default) then $HERMES_HOME/analytics.yaml (override)."""
    global _RETENTION_DAYS
    if _RETENTION_DAYS is not None:
        return _RETENTION_DAYS

    # Tier 0: explicit env var (standalone / non-Hermes deployments)
    env_retention = os.environ.get("ANALYTICS_RETENTION_DAYS")
    if env_retention:
        try:
            days = int(env_retention)
            if days > 0:
                _RETENTION_DAYS = days
                return days
        except ValueError:
            pass

    default = 365
    # 1. Plugin default from plugin.yaml
    plugin_dir = os.path.dirname(os.path.abspath(__file__))
    plugin_yaml = os.path.join(plugin_dir, "plugin.yaml")
    try:
        import yaml
        with open(plugin_yaml) as f:
            cfg = yaml.safe_load(f) or {}
        default = (cfg.get("config") or {}).get("retention_days", 365)
        if not isinstance(default, int):
            default = 365
    except Exception:
        pass

    # 2. Profile-local override: $HERMES_HOME/analytics.yaml
    hermes_home = os.environ.get("HERMES_HOME", os.path.expanduser("~/.hermes"))
    user_yaml = os.path.join(hermes_home, "analytics.yaml")
    try:
        import yaml
        with open(user_yaml) as f:
            user_cfg = yaml.safe_load(f) or {}
        days = user_cfg.get("retention_days", default)
        if isinstance(days, int) and days > 0:
            default = days
    except Exception:
        pass

    _RETENTION_DAYS = default
    return _RETENTION_DAYS or 365


def _maybe_retain() -> None:
    """Delete raw event records older than retention_days.

    Runs at most once per hour (throttled by _RETAIN_INTERVAL_S).
    Session summaries, approval_events, and kanban_events are kept
    indefinitely (small, useful for long-term trends).
    """
    global _last_retain_ts
    now = time.time()
    if (now - _last_retain_ts) < _RETAIN_INTERVAL_S:
        return
    _last_retain_ts = now
    retention = _get_retention_days()
    try:
        conn = _db()
        cutoff_ts = int(now - retention * 86400)
        for table in ("llm_calls", "tool_calls", "context_pressure", "skill_usage",
                       "api_requests", "inbound_messages", "verify_events"):
            try:
                conn.execute(f"DELETE FROM {table} WHERE timestamp < ?", (cutoff_ts,))
            except Exception:
                pass
        conn.commit()
        logger.info("Analytics retention: deleted records older than %d days (ts < %d)", retention, cutoff_ts)
    except Exception as exc:
        logger.warning("Analytics retention failed: %s", exc)

    # Optimize after retention to reclaim space
    try:
        conn = _db()
        conn.execute("PRAGMA optimize")
        logger.info("Analytics optimized")
    except Exception as exc:
        logger.debug("Analytics optimize after retention: %s", exc)


def _retention_timer() -> None:
    """Background loop that runs retention periodically, independent of sessions.

    Headless deployments with no sessions never trigger _maybe_retain() via
    on_session_finalize. This daemon thread calls it every _RETAIN_INTERVAL_S
    (throttled internally to at most once per hour).
    """
    while not _SHUTDOWN:
        time.sleep(_RETAIN_INTERVAL_S)
        if _SHUTDOWN:
            break
        try:
            _maybe_retain()
        except Exception as exc:
            logger.debug("Analytics retention timer run failed: %s", exc)


def _start_retention_timer() -> None:
    """Start the periodic retention timer thread (idempotent, daemon)."""
    global _retention_thread
    if _retention_thread is not None and _retention_thread.is_alive():
        return
    _retention_thread = threading.Thread(target=_retention_timer, daemon=True,
                                         name="analytics-retention-timer")
    _retention_thread.start()


# ── Batched background writer ───────────────────────────────────────────────

_WriteItem = Tuple[str, List[str], List[Any]]
_write_queue: queue.Queue = queue.Queue()  # type: ignore
_writer_thread: Optional[threading.Thread] = None
_writer_lock = threading.Lock()
_FLUSH_INTERVAL_S = 0.25
_BATCH_SIZE = 25
_SHUTDOWN = False


def _writer_loop() -> None:
    buf: deque = deque()
    while not _SHUTDOWN:
        try:
            item = _write_queue.get(timeout=_FLUSH_INTERVAL_S)
            buf.append(item)
        except queue.Empty:
            if buf:
                try:
                    _flush_batch(buf)
                except Exception as exc:
                    logger.warning("Analytics flush raised (recovered): %s", exc)
                    buf.clear()
            continue
        if len(buf) >= _BATCH_SIZE:
            try:
                _flush_batch(buf)
            except Exception as exc:
                logger.warning("Analytics flush raised (recovered): %s", exc)
                buf.clear()
    # Drain remaining on shutdown
    while True:
        try:
            buf.append(_write_queue.get_nowait())
        except queue.Empty:
            break
    if buf:
        try:
            _flush_batch(buf)
        except Exception as exc:
            logger.warning("Analytics final flush raised: %s", exc)


def _flush_batch(buf: deque) -> None:
    if not buf:
        return
    raw_items: List[Tuple[str, tuple]] = []
    grouped: Dict[str, List[Tuple[List[str], List[Any]]]] = {}
    while buf:
        item = buf.popleft()
        if item[0] == "__raw__":
            raw_items.append((item[1], item[2]))
        else:
            table, fields, values = item
            grouped.setdefault(table, []).append((fields, values))
    try:
        with _conn_lock:
            conn = _db()
            for table, items in grouped.items():
                for fields, values in items:
                    placeholders = ", ".join(["?"] * len(fields))
                    col_names = ", ".join(fields)
                    try:
                        conn.execute(f"INSERT INTO {table} ({col_names}) VALUES ({placeholders})", values)
                    except Exception as exc:
                        logger.warning("Analytics insert %s failed: %s", table, exc)
            for sql, params in raw_items:
                try:
                    if isinstance(params, (list, tuple)):
                        conn.execute(sql, params)
                    elif isinstance(params, dict):
                        conn.execute(sql, params)
                    else:
                        conn.execute(sql)
                    conn.commit()
                except Exception as exc:
                    logger.debug("Analytics raw query failed: %s", exc)
    except Exception as exc:
        logger.warning("Analytics flush failed: %s", exc)


def _start_writer() -> None:
    global _writer_thread
    with _writer_lock:
        if _writer_thread is None or not _writer_thread.is_alive():
            _writer_thread = threading.Thread(target=_writer_loop, daemon=True, name="analytics-writer")
            _writer_thread.start()


def _enqueue(table: str, fields: List[str], values: List[Any]) -> None:
    _start_writer()
    _write_queue.put((table, fields, values))


def _enqueue_raw(sql: str, params: Any = None) -> None:
    """Enqueue a raw SQL statement for the writer thread.

    params can be a tuple/list (for ? placeholders), a dict (for :named), or None.
    """
    _start_writer()
    _write_queue.put(("__raw__", sql, params if params is not None else {}))


def _update_session(sid: str, delta_cost: float = 0.0,
                     delta_prompt: int = 0, delta_completion: int = 0,
                     llm_calls: int = 0, tool_calls: int = 0) -> None:
    _enqueue_raw(
        "INSERT INTO session_summary (session_id, start_time, total_llm_calls, "
        "total_tool_calls, total_prompt_tokens, total_completion_tokens, total_cost_usd) "
        "VALUES (?, strftime('%s','now'), ?, ?, ?, ?, ?) "
        "ON CONFLICT(session_id) DO UPDATE SET "
        "total_llm_calls = total_llm_calls + ?, "
        "total_tool_calls = total_tool_calls + ?, "
        "total_prompt_tokens = total_prompt_tokens + ?, "
        "total_completion_tokens = total_completion_tokens + ?, "
        "total_cost_usd = total_cost_usd + ?",
        (sid, llm_calls, tool_calls, delta_prompt, delta_completion, delta_cost,
         llm_calls, tool_calls, delta_prompt, delta_completion, delta_cost),
    )


def _flush_sync(timeout: float = 5.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if _write_queue.empty():
            time.sleep(_FLUSH_INTERVAL_S + 0.05)
            if _write_queue.empty():
                return
        time.sleep(0.05)


# ── Helpers ─────────────────────────────────────────────────────────────────

def _args_hash(args: Any) -> str:
    if args is None:
        return ""
    try:
        payload = json.dumps(args, sort_keys=True, default=str)
        return hashlib.md5(payload[:4096].encode()).hexdigest()
    except Exception:
        return ""


def _extract_exit_code(result: Any) -> Optional[int]:
    parsed = None
    if isinstance(result, dict):
        parsed = result
    elif isinstance(result, str):
        try:
            parsed = json.loads(result)
        except (json.JSONDecodeError, ValueError):
            pass
    if isinstance(parsed, dict) and "exit_code" in parsed:
        val = parsed.get("exit_code")
        if isinstance(val, int):
            return val
    return None


# ── In-memory state ────────────────────────────────────────────────────────

class _State:
    __slots__ = ("lock", "turn_number", "session_platform", "model", "task_id", "api_request_start")
    def __init__(self):
        self.lock = threading.Lock()
        self.turn_number = 0
        self.session_platform = ""
        self.model = ""
        self.task_id = ""

_STATE: Dict[str, _State] = {}


def _state(sid: str) -> _State:
    if sid not in _STATE:
        _STATE[sid] = _State()
    return _STATE[sid]


# ── Context pressure dedup ─────────────────────────────────────────────────

_last_ctx_key: Tuple[str, int, int] = ("", 0, 0)
_last_ctx_ts = 0.0
_CTX_DEDUP_S = 1.5
_ctx_dedup_lock = threading.Lock()


