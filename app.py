#!/usr/bin/env python3
"""
PagerDuty Full-Feature SRE AI Assistant — Enhanced Edition
Powered by Groq (moonshotai/kimi-k2-instruct-0905) + PagerDuty REST API v2

New in this version:
  • Streaming final responses
  • Exponential-backoff retry on all PD API calls
  • TTL caching for slow-changing resources (services, users)
  • Natural-language time parsing  (dateparser)
  • Postmortem auto-generation
  • Proactive monitoring daemon (--monitor)
  • Dry-run mode  (--dry-run  or  DRY_RUN=true)
  • Smart context compression (summarises old turns instead of discarding)
  • Rich-formatted CLI output (tables, panels, markdown)
  • Incident pattern analysis, SLA breach detection, on-call burnout report
  • YAML config file  (config.yml)
  • Conversation persistence  (conversation_history.json)
  • Model fallback  (primary → fallback on transient failures)
  • Fixed update payloads for service/user/team/escalation_policy
  • Truncation flag in _safe_list so the LLM knows data was cut
  • Safe JSON arg parsing with graceful degradation
"""

# =====================================================
# IMPORTS
# =====================================================

import os
import re
import sys
import json
import time
import yaml
import signal
import argparse
import threading
import difflib
from pathlib import Path
from collections import defaultdict, Counter
from datetime import datetime, timezone, timedelta
from functools import wraps
from typing import Optional, List, Dict, Any

import dateparser
from dotenv import load_dotenv
from groq import Client, APIStatusError, APIConnectionError, RateLimitError
import pagerduty

try:
    from rich.console import Console
    from rich.table import Table
    from rich.panel import Panel
    from rich.markdown import Markdown
    from rich.text import Text
    from rich.rule import Rule
    from rich import print as rprint
    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False
    print("[WARNING] 'rich' not installed. Run: pip install rich  (plain text mode active)")

# =====================================================
# ARGUMENT PARSING
# =====================================================

def parse_args():
    parser = argparse.ArgumentParser(
        description="PagerDuty SRE AI Assistant",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python pagerduty_sre_bot.py
  python pagerduty_sre_bot.py --monitor
  python pagerduty_sre_bot.py --dry-run
  python pagerduty_sre_bot.py --config my_config.yml --no-persist
        """
    )
    parser.add_argument("--monitor", action="store_true",
                        help="Start proactive monitoring daemon alongside the chat loop")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print what destructive operations *would* do without executing them")
    parser.add_argument("--config", default="config.yml",
                        help="Path to YAML config file (default: config.yml)")
    parser.add_argument("--no-persist", action="store_true",
                        help="Do not load or save conversation history")
    parser.add_argument("--history", default=None,
                        help="Path to conversation history JSON (overrides config)")
    return parser.parse_args()

ARGS = parse_args()

# =====================================================
# YAML CONFIG  (config.yml — created with defaults if absent)
# =====================================================

DEFAULT_CONFIG = {
    "model": {
        "primary":  "moonshotai/kimi-k2-instruct-0905",
        "fallback": "llama-3.3-70b-versatile",
    },
    "defaults": {
        "time_window_hours": 24,
        "max_results":       50,
    },
    "sla": {
        "mtta_minutes": 5,
        "mttr_minutes": 60,
    },
    "monitoring": {
        "poll_interval_seconds": 60,
        "urgency_filter":        "high",
    },
    "cache": {
        "ttl_seconds": 300,
    },
    "history": {
        "file":         "conversation_history.json",
        "max_messages": 40,
        "compress_at":  30,
    },
    "output": {
        "rich_tables": True,
    },
    "dry_run": False,
}

def _deep_merge(base: dict, override: dict) -> dict:
    result = dict(base)
    for k, v in override.items():
        if isinstance(v, dict) and isinstance(result.get(k), dict):
            result[k] = _deep_merge(result[k], v)
        else:
            result[k] = v
    return result

def load_config(path: str) -> dict:
    cfg = DEFAULT_CONFIG.copy()
    p = Path(path)
    if p.exists():
        with open(p) as f:
            user_cfg = yaml.safe_load(f) or {}
        cfg = _deep_merge(cfg, user_cfg)
    else:
        with open(p, "w") as f:
            yaml.dump(DEFAULT_CONFIG, f, default_flow_style=False)
        print(f"[INFO] Created default config at {p}")
    return cfg

CONFIG = load_config(ARGS.config)

# Dry-run can be set via CLI flag, env var, or config
DRY_RUN = ARGS.dry_run or os.getenv("DRY_RUN", "false").lower() == "true" or CONFIG.get("dry_run", False)

MODEL_PRIMARY  = CONFIG["model"]["primary"]
MODEL_FALLBACK = CONFIG["model"]["fallback"]
MAX_RESULTS    = CONFIG["defaults"]["max_results"]
SLA_MTTA_SEC   = CONFIG["sla"]["mtta_minutes"] * 60
SLA_MTTR_SEC   = CONFIG["sla"]["mttr_minutes"] * 60

# =====================================================
# ENV & CLIENTS
# =====================================================

load_dotenv(".env")

GROQ_API_KEY      = os.getenv("GROQ_API_KEY")
PAGERDUTY_API_KEY = os.getenv("PAGERDUTY_API_KEY")
PAGERDUTY_EMAIL   = os.getenv("PAGERDUTY_EMAIL", "")

if not GROQ_API_KEY or not PAGERDUTY_API_KEY:
    raise ValueError("Missing required API keys. Set GROQ_API_KEY and PAGERDUTY_API_KEY in .env")

groq_client = Client(api_key=GROQ_API_KEY)
pd_client   = pagerduty.RestApiV2Client(PAGERDUTY_API_KEY, default_from=PAGERDUTY_EMAIL)

# =====================================================
# RICH CONSOLE
# =====================================================

console = Console() if RICH_AVAILABLE else None

def cprint(msg, **kwargs):
    if RICH_AVAILABLE:
        console.print(msg, **kwargs)
    else:
        # Strip rich markup for plain output
        plain = re.sub(r"\[.*?\]", "", str(msg))
        print(plain)

def print_rule(title=""):
    if RICH_AVAILABLE:
        console.print(Rule(title))
    else:
        print(f"\n{'─'*50} {title} {'─'*50}\n" if title else "─"*100)

def render_markdown(text: str):
    if RICH_AVAILABLE:
        console.print(Markdown(text))
    else:
        print(text)

# =====================================================
# RETRY DECORATOR  (exponential backoff)
# =====================================================

RETRYABLE_EXCEPTIONS = (APIConnectionError, RateLimitError)

def with_retry(max_retries: int = 3, base_delay: float = 1.0):
    """Retry a function on transient errors with exponential back-off."""
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            last_exc = None
            for attempt in range(max_retries):
                try:
                    return fn(*args, **kwargs)
                except RETRYABLE_EXCEPTIONS as e:
                    last_exc = e
                    delay = base_delay * (2 ** attempt)
                    cprint(f"[yellow]⚠  Transient error ({type(e).__name__}), retrying in {delay:.1f}s… (attempt {attempt+1}/{max_retries})[/yellow]")
                    time.sleep(delay)
                except APIStatusError as e:
                    if e.status_code == 429:
                        last_exc = e
                        delay = base_delay * (2 ** attempt)
                        cprint(f"[yellow]⚠  Rate-limited (429), retrying in {delay:.1f}s…[/yellow]")
                        time.sleep(delay)
                    else:
                        raise
            raise last_exc
        return wrapper
    return decorator

# =====================================================
# TTL CACHE  (for slow-changing PD resources)
# =====================================================

_cache_store: Dict[str, Any] = {}
_cache_expiry: Dict[str, float] = {}
_TTL = CONFIG["cache"]["ttl_seconds"]

def cache_get(key: str):
    if key in _cache_store and time.time() < _cache_expiry[key]:
        return _cache_store[key]
    return None

def cache_set(key: str, value: Any, ttl: float = None):
    _cache_store[key] = value
    _cache_expiry[key] = time.time() + (ttl or _TTL)

def cache_clear(pattern: str = None):
    if pattern is None:
        _cache_store.clear()
        _cache_expiry.clear()
    else:
        for k in list(_cache_store):
            if pattern in k:
                del _cache_store[k]
                del _cache_expiry[k]

# =====================================================
# NATURAL-LANGUAGE TIME PARSING
# =====================================================

def parse_nl_time(text: str, default_tz: str = "UTC") -> Optional[str]:
    """
    Parse natural-language time strings into ISO-8601 UTC.
    Examples: 'yesterday', 'last Monday 9am', '3 days ago', 'now'
    Returns None if unparseable.
    """
    if not text:
        return None
    dt = dateparser.parse(
        text,
        settings={
            "RETURN_AS_TIMEZONE_AWARE": True,
            "TIMEZONE": default_tz,
            "PREFER_LOCALE_DATE_FORMAT": False,
            "TO_TIMEZONE": "UTC",
        }
    )
    if dt:
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    return None

def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def iso_now() -> str:
    return now_utc().strftime("%Y-%m-%dT%H:%M:%SZ")

def iso_hours_ago(n: float) -> str:
    return (now_utc() - timedelta(hours=n)).strftime("%Y-%m-%dT%H:%M:%SZ")

# =====================================================
# CONVERSATION PERSISTENCE
# =====================================================

def _history_path() -> Path:
    if ARGS.history:
        return Path(ARGS.history)
    return Path(CONFIG["history"]["file"])

def _sanitize_history(history: list) -> list:
    """
    Drop any entry that is not a plain dict with a 'role' key.
    This guards against Pydantic objects or strings that were accidentally
    saved to JSON (via default=str) in previous sessions.
    Also drops tool-call assistant turns and raw tool results — these
    are intermediate scaffolding that bloat the token count on reload;
    only user turns and final assistant text turns are kept.
    """
    clean = []
    for m in history:
        if not isinstance(m, dict):
            continue                          # discard serialised Pydantic strings
        role = m.get("role")
        if role == "user":
            clean.append(m)
        elif role == "assistant" and not m.get("tool_calls"):
            # Only keep final assistant answers, not intermediate tool-call turns
            clean.append(m)
        # "tool" role results and assistant+tool_calls turns are intentionally dropped
    return clean


def load_history() -> List[dict]:
    if ARGS.no_persist:
        return []
    p = _history_path()
    if p.exists():
        try:
            raw = json.loads(p.read_text())
            data = _sanitize_history(raw)
            dropped = len(raw) - len(data)
            suffix = f" ({dropped} intermediate tool messages stripped)" if dropped else ""
            cprint(f"[dim]Loaded {len(data)} messages from {p}{suffix}[/dim]")
            return data
        except Exception:
            pass
    return []

def save_history(history: List[dict]):
    if ARGS.no_persist:
        return
    p = _history_path()
    try:
        p.write_text(json.dumps(history, indent=2, default=str))
    except Exception as e:
        cprint(f"[yellow]⚠  Could not save history: {e}[/yellow]")

# =====================================================
# SMART CONTEXT COMPRESSION
# =====================================================

def compress_history(history: List[dict]) -> List[dict]:
    """
    When history exceeds compress_at, summarise the oldest half into a
    single assistant message so the LLM retains context without token bloat.
    """
    compress_at  = CONFIG["history"]["compress_at"]
    max_messages = CONFIG["history"]["max_messages"]

    if len(history) < compress_at:
        return history

    half = len(history) // 2
    old_messages = history[:half]
    new_messages = history[half:]

    # Build a mini-transcript for the summary call
    transcript = "\n".join(
        f"{m['role'].upper()}: {str(m.get('content',''))[:500]}"
        for m in old_messages
        if m.get("content") and m.get("role") in ("user", "assistant")
    )

    if not transcript.strip():
        return new_messages

    try:
        summary_resp = groq_client.chat.completions.create(
            model=MODEL_FALLBACK,          # use cheaper model for summary
            messages=[
                {"role": "system", "content":
                    "You are a concise technical summariser. Summarise the following "
                    "SRE/PagerDuty conversation into a brief bullet-point memory block "
                    "covering: key incidents discussed, actions taken, decisions made, "
                    "and any IDs or names that are important. Be terse — max 300 words."},
                {"role": "user", "content": transcript},
            ],
            max_tokens=400,
        )
        summary_text = summary_resp.choices[0].message.content or ""
        summary_message = {
            "role": "assistant",
            "content": f"[CONVERSATION SUMMARY — earlier context]\n{summary_text}"
        }
        compressed = [summary_message] + new_messages
        cprint(f"[dim]Context compressed: {len(old_messages)} messages → 1 summary[/dim]")
        return compressed
    except Exception as e:
        cprint(f"[yellow]⚠  Context compression failed: {e}. Trimming instead.[/yellow]")
        return history[-max_messages:]


# =====================================================
# TOOL DEFINITIONS  (Groq function-calling schema)
# =====================================================

TOOLS = [
    # ── Incidents ─────────────────────────────────────
    {
        "type": "function",
        "function": {
            "name": "list_incidents",
            "description": "List incidents within a time window, optionally filtered by status, urgency, service, or team.",
            "parameters": {
                "type": "object",
                "properties": {
                    "since":      {"type": "string", "description": "ISO8601 start time, e.g. 2025-01-01T00:00:00Z"},
                    "until":      {"type": "string", "description": "ISO8601 end time"},
                    "statuses":   {"type": "array",  "items": {"type": "string", "enum": ["triggered","acknowledged","resolved"]}},
                    "urgencies":  {"type": "array",  "items": {"type": "string", "enum": ["high","low"]}},
                    "service_ids":{"type": "array",  "items": {"type": "string"}},
                    "team_ids":   {"type": "array",  "items": {"type": "string"}},
                    "sort_by":    {"type": "string", "enum": ["created_at","resolved_at","urgency"]},
                    "limit":      {"type": "integer"}
                },
                "required": ["since","until"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_incident",
            "description": "Get full details of a single incident by ID.",
            "parameters": {
                "type": "object",
                "properties": {"incident_id": {"type": "string"}},
                "required": ["incident_id"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_incident_timeline",
            "description": "Get the full log-entry timeline for an incident (trigger, ack, resolve, escalation, etc.).",
            "parameters": {
                "type": "object",
                "properties": {"incident_id": {"type": "string"}},
                "required": ["incident_id"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_incident_notes",
            "description": "Get notes/annotations added to an incident.",
            "parameters": {
                "type": "object",
                "properties": {"incident_id": {"type": "string"}},
                "required": ["incident_id"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_incident_alerts",
            "description": "Get alerts associated with an incident.",
            "parameters": {
                "type": "object",
                "properties": {"incident_id": {"type": "string"}},
                "required": ["incident_id"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "manage_incident",
            "description": "Update an incident: acknowledge, resolve, reassign, change urgency, merge, snooze, or add a note.",
            "parameters": {
                "type": "object",
                "properties": {
                    "incident_id":     {"type": "string"},
                    "action":          {"type": "string", "enum": ["acknowledge","resolve","reassign","change_urgency","snooze","add_note","merge"]},
                    "assignee_id":     {"type": "string"},
                    "assignee_type":   {"type": "string", "enum": ["user_reference","escalation_policy_reference"]},
                    "urgency":         {"type": "string", "enum": ["high","low"]},
                    "snooze_duration": {"type": "integer", "description": "Snooze duration in seconds."},
                    "note_content":    {"type": "string"},
                    "merge_ids":       {"type": "array", "items": {"type": "string"}}
                },
                "required": ["incident_id","action"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "create_incident",
            "description": "Create a new incident on a service.",
            "parameters": {
                "type": "object",
                "properties": {
                    "title":                 {"type": "string"},
                    "service_id":            {"type": "string"},
                    "urgency":               {"type": "string", "enum": ["high","low"]},
                    "body":                  {"type": "string"},
                    "escalation_policy_id":  {"type": "string"},
                    "priority_id":           {"type": "string"}
                },
                "required": ["title","service_id"]
            }
        }
    },
    # ── Services ──────────────────────────────────────
    {
        "type": "function",
        "function": {
            "name": "list_services",
            "description": "List all services, optionally filtered by team or name.",
            "parameters": {
                "type": "object",
                "properties": {
                    "query":    {"type": "string"},
                    "team_ids": {"type": "array", "items": {"type": "string"}},
                    "include":  {"type": "array", "items": {"type": "string", "enum": ["escalation_policies","teams","integrations"]}}
                },
                "required": []
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_service",
            "description": "Get details of a single service by ID.",
            "parameters": {
                "type": "object",
                "properties": {
                    "service_id": {"type": "string"},
                    "include":    {"type": "array", "items": {"type": "string", "enum": ["escalation_policies","teams","integrations"]}}
                },
                "required": ["service_id"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "create_service",
            "description": "Create a new PagerDuty service.",
            "parameters": {
                "type": "object",
                "properties": {
                    "name":                 {"type": "string"},
                    "description":          {"type": "string"},
                    "escalation_policy_id": {"type": "string"},
                    "urgency_rule":         {"type": "string", "enum": ["high","low","severity_based"]},
                    "alert_creation":       {"type": "string", "enum": ["create_incidents","create_alerts_and_incidents"]}
                },
                "required": ["name","escalation_policy_id"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "update_service",
            "description": "Update an existing service.",
            "parameters": {
                "type": "object",
                "properties": {
                    "service_id":           {"type": "string"},
                    "name":                 {"type": "string"},
                    "description":          {"type": "string"},
                    "escalation_policy_id": {"type": "string"},
                    "status":               {"type": "string", "enum": ["active","warning","critical","maintenance","disabled"]}
                },
                "required": ["service_id"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "delete_service",
            "description": "Delete a service by ID. WARNING: irreversible.",
            "parameters": {
                "type": "object",
                "properties": {"service_id": {"type": "string"}},
                "required": ["service_id"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "list_service_integrations",
            "description": "List integrations for a service (shows integration keys and types).",
            "parameters": {
                "type": "object",
                "properties": {"service_id": {"type": "string"}},
                "required": ["service_id"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "create_service_integration",
            "description": "Create a new integration on a service.",
            "parameters": {
                "type": "object",
                "properties": {
                    "service_id": {"type": "string"},
                    "name":       {"type": "string"},
                    "type":       {"type": "string"},
                    "vendor_id":  {"type": "string"}
                },
                "required": ["service_id","name","type"]
            }
        }
    },
    # ── Users ─────────────────────────────────────────
    {
        "type": "function",
        "function": {
            "name": "list_users",
            "description": "List users in the account.",
            "parameters": {
                "type": "object",
                "properties": {
                    "query":    {"type": "string"},
                    "team_ids": {"type": "array", "items": {"type": "string"}},
                    "include":  {"type": "array", "items": {"type": "string", "enum": ["contact_methods","notification_rules","teams"]}}
                },
                "required": []
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_user",
            "description": "Get detailed profile info for a user by ID.",
            "parameters": {
                "type": "object",
                "properties": {
                    "user_id": {"type": "string"},
                    "include": {"type": "array", "items": {"type": "string", "enum": ["contact_methods","notification_rules","teams"]}}
                },
                "required": ["user_id"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "create_user",
            "description": "Create a new user in PagerDuty.",
            "parameters": {
                "type": "object",
                "properties": {
                    "name":      {"type": "string"},
                    "email":     {"type": "string"},
                    "role":      {"type": "string", "enum": ["admin","limited_user","observer","owner","read_only_limited_user","read_only_user","restricted_access","user"]},
                    "time_zone": {"type": "string"},
                    "job_title": {"type": "string"}
                },
                "required": ["name","email"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "update_user",
            "description": "Update a user's profile.",
            "parameters": {
                "type": "object",
                "properties": {
                    "user_id":   {"type": "string"},
                    "name":      {"type": "string"},
                    "email":     {"type": "string"},
                    "role":      {"type": "string"},
                    "job_title": {"type": "string"},
                    "time_zone": {"type": "string"}
                },
                "required": ["user_id"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "delete_user",
            "description": "Delete a user. WARNING: irreversible.",
            "parameters": {
                "type": "object",
                "properties": {"user_id": {"type": "string"}},
                "required": ["user_id"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_user_contact_methods",
            "description": "Get contact methods (phone, SMS, email, push) for a user.",
            "parameters": {
                "type": "object",
                "properties": {"user_id": {"type": "string"}},
                "required": ["user_id"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_user_notification_rules",
            "description": "Get notification rules for a user.",
            "parameters": {
                "type": "object",
                "properties": {"user_id": {"type": "string"}},
                "required": ["user_id"]
            }
        }
    },
    # ── Teams ─────────────────────────────────────────
    {
        "type": "function",
        "function": {
            "name": "list_teams",
            "description": "List all teams.",
            "parameters": {
                "type": "object",
                "properties": {"query": {"type": "string"}},
                "required": []
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_team",
            "description": "Get details of a team by ID.",
            "parameters": {
                "type": "object",
                "properties": {"team_id": {"type": "string"}},
                "required": ["team_id"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "create_team",
            "description": "Create a new team.",
            "parameters": {
                "type": "object",
                "properties": {
                    "name":        {"type": "string"},
                    "description": {"type": "string"}
                },
                "required": ["name"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "update_team",
            "description": "Update an existing team.",
            "parameters": {
                "type": "object",
                "properties": {
                    "team_id":     {"type": "string"},
                    "name":        {"type": "string"},
                    "description": {"type": "string"}
                },
                "required": ["team_id"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "delete_team",
            "description": "Delete a team. WARNING: irreversible.",
            "parameters": {
                "type": "object",
                "properties": {"team_id": {"type": "string"}},
                "required": ["team_id"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "manage_team_membership",
            "description": "Add or remove a user from a team.",
            "parameters": {
                "type": "object",
                "properties": {
                    "team_id": {"type": "string"},
                    "user_id": {"type": "string"},
                    "action":  {"type": "string", "enum": ["add","remove"]},
                    "role":    {"type": "string", "enum": ["manager","responder","observer"]}
                },
                "required": ["team_id","user_id","action"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "list_team_members",
            "description": "List all members of a team.",
            "parameters": {
                "type": "object",
                "properties": {"team_id": {"type": "string"}},
                "required": ["team_id"]
            }
        }
    },
    # ── Escalation Policies ───────────────────────────
    {
        "type": "function",
        "function": {
            "name": "list_escalation_policies",
            "description": "List escalation policies.",
            "parameters": {
                "type": "object",
                "properties": {
                    "query":    {"type": "string"},
                    "team_ids": {"type": "array", "items": {"type": "string"}}
                },
                "required": []
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_escalation_policy",
            "description": "Get full details of an escalation policy.",
            "parameters": {
                "type": "object",
                "properties": {"policy_id": {"type": "string"}},
                "required": ["policy_id"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "create_escalation_policy",
            "description": "Create a new escalation policy.",
            "parameters": {
                "type": "object",
                "properties": {
                    "name":              {"type": "string"},
                    "description":       {"type": "string"},
                    "num_loops":         {"type": "integer"},
                    "escalation_rules":  {"type": "array", "items": {"type": "object"}},
                    "team_id":           {"type": "string"}
                },
                "required": ["name","escalation_rules"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "update_escalation_policy",
            "description": "Update an escalation policy.",
            "parameters": {
                "type": "object",
                "properties": {
                    "policy_id":         {"type": "string"},
                    "name":              {"type": "string"},
                    "description":       {"type": "string"},
                    "num_loops":         {"type": "integer"},
                    "escalation_rules":  {"type": "array", "items": {"type": "object"}}
                },
                "required": ["policy_id"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "delete_escalation_policy",
            "description": "Delete an escalation policy.",
            "parameters": {
                "type": "object",
                "properties": {"policy_id": {"type": "string"}},
                "required": ["policy_id"]
            }
        }
    },
    # ── Schedules ─────────────────────────────────────
    {
        "type": "function",
        "function": {
            "name": "list_schedules",
            "description": "List all on-call schedules.",
            "parameters": {
                "type": "object",
                "properties": {"query": {"type": "string"}},
                "required": []
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_schedule",
            "description": "Get details of a schedule including rendered on-call entries.",
            "parameters": {
                "type": "object",
                "properties": {
                    "schedule_id": {"type": "string"},
                    "since":       {"type": "string"},
                    "until":       {"type": "string"}
                },
                "required": ["schedule_id"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "list_schedule_overrides",
            "description": "List overrides on a schedule within a time range.",
            "parameters": {
                "type": "object",
                "properties": {
                    "schedule_id": {"type": "string"},
                    "since":       {"type": "string"},
                    "until":       {"type": "string"}
                },
                "required": ["schedule_id","since","until"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "create_schedule_override",
            "description": "Create an override on a schedule (swap on-call).",
            "parameters": {
                "type": "object",
                "properties": {
                    "schedule_id": {"type": "string"},
                    "user_id":     {"type": "string"},
                    "start":       {"type": "string"},
                    "end":         {"type": "string"}
                },
                "required": ["schedule_id","user_id","start","end"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "delete_schedule_override",
            "description": "Delete a schedule override.",
            "parameters": {
                "type": "object",
                "properties": {
                    "schedule_id": {"type": "string"},
                    "override_id": {"type": "string"}
                },
                "required": ["schedule_id","override_id"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "list_schedule_users_on_call",
            "description": "List users currently on call for a schedule.",
            "parameters": {
                "type": "object",
                "properties": {
                    "schedule_id": {"type": "string"},
                    "since":       {"type": "string"},
                    "until":       {"type": "string"}
                },
                "required": ["schedule_id"]
            }
        }
    },
    # ── On-Calls ──────────────────────────────────────
    {
        "type": "function",
        "function": {
            "name": "list_oncalls",
            "description": "List current on-call entries across all or specific escalation policies/schedules.",
            "parameters": {
                "type": "object",
                "properties": {
                    "schedule_ids":           {"type": "array", "items": {"type": "string"}},
                    "escalation_policy_ids":  {"type": "array", "items": {"type": "string"}},
                    "user_ids":               {"type": "array", "items": {"type": "string"}},
                    "since":                  {"type": "string"},
                    "until":                  {"type": "string"},
                    "earliest":               {"type": "boolean"}
                },
                "required": []
            }
        }
    },
    # ── Priorities ────────────────────────────────────
    {
        "type": "function",
        "function": {
            "name": "list_priorities",
            "description": "List all incident priority levels.",
            "parameters": {"type": "object", "properties": {}, "required": []}
        }
    },
    # ── Maintenance Windows ───────────────────────────
    {
        "type": "function",
        "function": {
            "name": "list_maintenance_windows",
            "description": "List maintenance windows.",
            "parameters": {
                "type": "object",
                "properties": {
                    "service_ids": {"type": "array", "items": {"type": "string"}},
                    "team_ids":    {"type": "array", "items": {"type": "string"}},
                    "filter":      {"type": "string", "enum": ["past","future","ongoing","open","all"]}
                },
                "required": []
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "create_maintenance_window",
            "description": "Create a maintenance window on one or more services.",
            "parameters": {
                "type": "object",
                "properties": {
                    "start_time":  {"type": "string"},
                    "end_time":    {"type": "string"},
                    "description": {"type": "string"},
                    "service_ids": {"type": "array", "items": {"type": "string"}}
                },
                "required": ["start_time","end_time","service_ids"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "delete_maintenance_window",
            "description": "Delete/end a maintenance window.",
            "parameters": {
                "type": "object",
                "properties": {"window_id": {"type": "string"}},
                "required": ["window_id"]
            }
        }
    },
    # ── Log Entries ───────────────────────────────────
    {
        "type": "function",
        "function": {
            "name": "list_log_entries",
            "description": "List account-wide log entries (audit trail of all activity).",
            "parameters": {
                "type": "object",
                "properties": {
                    "since":       {"type": "string"},
                    "until":       {"type": "string"},
                    "is_overview": {"type": "boolean"}
                },
                "required": []
            }
        }
    },
    # ── Notifications ─────────────────────────────────
    {
        "type": "function",
        "function": {
            "name": "list_notifications",
            "description": "List notifications that were sent.",
            "parameters": {
                "type": "object",
                "properties": {
                    "since":   {"type": "string"},
                    "until":   {"type": "string"},
                    "filter":  {"type": "string", "enum": ["sms_notification","email_notification","phone_notification","push_notification"]},
                    "include": {"type": "array", "items": {"type": "string", "enum": ["users"]}}
                },
                "required": ["since","until"]
            }
        }
    },
    # ── Analytics ─────────────────────────────────────
    {
        "type": "function",
        "function": {
            "name": "get_analytics_incidents",
            "description": "Get raw analytics data for incidents (MTTA, MTTR, engaged_seconds, etc.).",
            "parameters": {
                "type": "object",
                "properties": {
                    "since":       {"type": "string"},
                    "until":       {"type": "string"},
                    "service_ids": {"type": "array", "items": {"type": "string"}},
                    "team_ids":    {"type": "array", "items": {"type": "string"}},
                    "urgencies":   {"type": "array", "items": {"type": "string", "enum": ["high","low"]}}
                },
                "required": []
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_analytics_services",
            "description": "Get aggregated analytics per service.",
            "parameters": {
                "type": "object",
                "properties": {
                    "since":       {"type": "string"},
                    "until":       {"type": "string"},
                    "service_ids": {"type": "array", "items": {"type": "string"}},
                    "team_ids":    {"type": "array", "items": {"type": "string"}}
                },
                "required": []
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_analytics_teams",
            "description": "Get aggregated analytics per team.",
            "parameters": {
                "type": "object",
                "properties": {
                    "since":    {"type": "string"},
                    "until":    {"type": "string"},
                    "team_ids": {"type": "array", "items": {"type": "string"}}
                },
                "required": []
            }
        }
    },
    # ── Tags ──────────────────────────────────────────
    {
        "type": "function",
        "function": {
            "name": "list_tags",
            "description": "List all tags.",
            "parameters": {
                "type": "object",
                "properties": {"query": {"type": "string"}},
                "required": []
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "manage_tags",
            "description": "Add or remove tags from a resource (user, team, or escalation_policy).",
            "parameters": {
                "type": "object",
                "properties": {
                    "resource_type": {"type": "string", "enum": ["users","teams","escalation_policies"]},
                    "resource_id":   {"type": "string"},
                    "add_tags":      {"type": "array", "items": {"type": "object"}},
                    "remove_tags":   {"type": "array", "items": {"type": "object"}}
                },
                "required": ["resource_type","resource_id"]
            }
        }
    },
    # ── Vendors ───────────────────────────────────────
    {
        "type": "function",
        "function": {
            "name": "list_vendors",
            "description": "List available integration vendors (Datadog, AWS CloudWatch, etc.).",
            "parameters": {
                "type": "object",
                "properties": {"query": {"type": "string"}},
                "required": []
            }
        }
    },
    # ── Response Plays ────────────────────────────────
    {
        "type": "function",
        "function": {
            "name": "list_response_plays",
            "description": "List response plays (pre-configured incident response actions).",
            "parameters": {
                "type": "object",
                "properties": {"query": {"type": "string"}},
                "required": []
            }
        }
    },
    # ── Business Services ─────────────────────────────
    {
        "type": "function",
        "function": {
            "name": "list_business_services",
            "description": "List business services.",
            "parameters": {"type": "object", "properties": {}, "required": []}
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_business_service",
            "description": "Get a business service by ID.",
            "parameters": {
                "type": "object",
                "properties": {"business_service_id": {"type": "string"}},
                "required": ["business_service_id"]
            }
        }
    },
    # ── Service Dependencies ──────────────────────────
    {
        "type": "function",
        "function": {
            "name": "get_service_dependencies",
            "description": "Get upstream/downstream service dependencies.",
            "parameters": {
                "type": "object",
                "properties": {
                    "service_id":   {"type": "string"},
                    "service_type": {"type": "string", "enum": ["technical","business"]}
                },
                "required": ["service_id","service_type"]
            }
        }
    },
    # ── Status Dashboard ──────────────────────────────
    {
        "type": "function",
        "function": {
            "name": "list_status_dashboards",
            "description": "List status dashboards.",
            "parameters": {"type": "object", "properties": {}, "required": []}
        }
    },
    # ── Audit Records ─────────────────────────────────
    {
        "type": "function",
        "function": {
            "name": "list_audit_records",
            "description": "List audit records (who changed what). Use for: 'audit log', 'configuration changes'.",
            "parameters": {
                "type": "object",
                "properties": {
                    "since":                {"type": "string"},
                    "until":               {"type": "string"},
                    "root_resource_types": {"type": "array", "items": {"type": "string", "enum": ["users","services","teams","escalation_policies","schedules"]}}
                },
                "required": []
            }
        }
    },
    # ── Event Orchestration / Rulesets ────────────────
    {
        "type": "function",
        "function": {
            "name": "list_event_orchestrations",
            "description": "List global event orchestration rules.",
            "parameters": {"type": "object", "properties": {}, "required": []}
        }
    },
    {
        "type": "function",
        "function": {
            "name": "list_rulesets",
            "description": "List rulesets (legacy event rules).",
            "parameters": {"type": "object", "properties": {}, "required": []}
        }
    },
    # ── Webhooks ──────────────────────────────────────
    {
        "type": "function",
        "function": {
            "name": "list_webhook_subscriptions",
            "description": "List V3 webhook subscriptions.",
            "parameters": {"type": "object", "properties": {}, "required": []}
        }
    },
    {
        "type": "function",
        "function": {
            "name": "create_webhook_subscription",
            "description": "Create a new V3 webhook subscription.",
            "parameters": {
                "type": "object",
                "properties": {
                    "delivery_method_url": {"type": "string"},
                    "description":         {"type": "string"},
                    "events":              {"type": "array", "items": {"type": "string"}},
                    "filter_type":         {"type": "string", "enum": ["service_reference","team_reference","account_reference"]},
                    "filter_id":           {"type": "string"}
                },
                "required": ["delivery_method_url","events","filter_type"]
            }
        }
    },
    # ── Abilities / Extensions / Workflows ────────────
    {
        "type": "function",
        "function": {
            "name": "list_abilities",
            "description": "List the account's enabled abilities/features.",
            "parameters": {"type": "object", "properties": {}, "required": []}
        }
    },
    {
        "type": "function",
        "function": {
            "name": "list_extensions",
            "description": "List extensions (service extensions like Slack channels, webhooks, etc.).",
            "parameters": {
                "type": "object",
                "properties": {
                    "query":                {"type": "string"},
                    "extension_schema_id":  {"type": "string"}
                },
                "required": []
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "list_incident_workflows",
            "description": "List incident workflows (automated response actions).",
            "parameters": {
                "type": "object",
                "properties": {"query": {"type": "string"}},
                "required": []
            }
        }
    },
    # ── Compound Analysis ─────────────────────────────
    {
        "type": "function",
        "function": {
            "name": "full_incident_analysis",
            "description": "Run a full analysis: fetch incidents in a time window, compute MTTA, MTTR, escalation counts, service distribution. Use for comprehensive operational summaries.",
            "parameters": {
                "type": "object",
                "properties": {
                    "since":       {"type": "string"},
                    "until":       {"type": "string"},
                    "service_ids": {"type": "array", "items": {"type": "string"}},
                    "team_ids":    {"type": "array", "items": {"type": "string"}}
                },
                "required": ["since","until"]
            }
        }
    },
    # ── NEW TOOL 1: Natural-language time resolver ─────
    {
        "type": "function",
        "function": {
            "name": "resolve_time",
            "description": "Convert a natural-language time expression into an ISO-8601 UTC timestamp. "
                           "Use when the user says things like 'yesterday', 'last Monday', '3 hours ago', 'start of this week'.",
            "parameters": {
                "type": "object",
                "properties": {
                    "expression": {"type": "string", "description": "Natural-language time, e.g. 'yesterday', 'last Monday 9am', '3 hours ago'."}
                },
                "required": ["expression"]
            }
        }
    },
    # ── NEW TOOL 2: Postmortem generator ──────────────
    {
        "type": "function",
        "function": {
            "name": "generate_postmortem",
            "description": "Auto-generate a structured postmortem document for a resolved incident. "
                           "Gathers timeline, notes, alerts and produces a formatted postmortem report saved to a file.",
            "parameters": {
                "type": "object",
                "properties": {
                    "incident_id":   {"type": "string", "description": "Incident ID to generate postmortem for."},
                    "output_file":   {"type": "string", "description": "Optional filename to save to (default: postmortem_<id>.md)."}
                },
                "required": ["incident_id"]
            }
        }
    },
    # ── NEW TOOL 3: Pattern analysis ──────────────────
    {
        "type": "function",
        "function": {
            "name": "analyze_patterns",
            "description": "Analyse incidents in a time window to find recurring patterns: "
                           "noisiest services, repeated alert titles, time-of-day clusters, top escalation chains.",
            "parameters": {
                "type": "object",
                "properties": {
                    "since":       {"type": "string"},
                    "until":       {"type": "string"},
                    "service_ids": {"type": "array", "items": {"type": "string"}},
                    "team_ids":    {"type": "array", "items": {"type": "string"}},
                    "top_n":       {"type": "integer", "description": "How many top items to surface per category. Default 10."}
                },
                "required": ["since","until"]
            }
        }
    },
    # ── NEW TOOL 4: SLA breach detector ───────────────
    {
        "type": "function",
        "function": {
            "name": "check_sla_breaches",
            "description": "Find incidents that breached MTTA or MTTR SLA targets. "
                           "Returns a list of breaching incidents with details on how far over the SLA they ran.",
            "parameters": {
                "type": "object",
                "properties": {
                    "since":                  {"type": "string"},
                    "until":                  {"type": "string"},
                    "mtta_threshold_seconds": {"type": "integer", "description": "MTTA SLA in seconds. Default from config."},
                    "mttr_threshold_seconds": {"type": "integer", "description": "MTTR SLA in seconds. Default from config."},
                    "service_ids":            {"type": "array", "items": {"type": "string"}},
                    "team_ids":               {"type": "array", "items": {"type": "string"}}
                },
                "required": ["since","until"]
            }
        }
    },
    # ── NEW TOOL 5: On-call burnout report ────────────
    {
        "type": "function",
        "function": {
            "name": "oncall_load_report",
            "description": "Analyse who is getting paged most frequently to detect on-call burnout risk. "
                           "Reports pages per user, after-hours pages, and average response time.",
            "parameters": {
                "type": "object",
                "properties": {
                    "since":    {"type": "string"},
                    "until":    {"type": "string"},
                    "team_ids": {"type": "array", "items": {"type": "string"}}
                },
                "required": ["since","until"]
            }
        }
    },
]


# =====================================================
# CORE HELPERS
# =====================================================

def _fmt_ts(iso_str: str) -> Optional[datetime]:
    if not iso_str:
        return None
    for fmt in (
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%dT%H:%M:%S%z",
            "%Y-%m-%dT%H:%M:%S.%fZ",
            "%Y-%m-%dT%H:%M:%S.%f%z",
    ):
        try:
            return datetime.strptime(iso_str, fmt)
        except ValueError:
            continue
    return None

def _diff_minutes(start_str: str, end_str: str) -> Optional[float]:
    s, e = _fmt_ts(start_str), _fmt_ts(end_str)
    if s and e:
        # Normalise to UTC for naive datetimes
        if s.tzinfo is None:
            s = s.replace(tzinfo=timezone.utc)
        if e.tzinfo is None:
            e = e.replace(tzinfo=timezone.utc)
        return round((e - s).total_seconds() / 60, 2)
    return None

@with_retry(max_retries=3)
def _safe_list(endpoint: str, params: dict = None, limit: int = None) -> dict | list:
    """
    Safely iterate and collect results from a PagerDuty endpoint.
    Returns a dict with 'items' and optional 'truncated' flag if limit hit.
    For backwards compatibility, also supports returning raw list.
    """
    if limit is None:
        limit = MAX_RESULTS
    results = []
    truncated = False
    try:
        for item in pd_client.iter_all(endpoint, params=params or {}):
            results.append(item)
            if len(results) >= limit:
                truncated = True
                break
    except Exception as e:
        return {"error": str(e)}
    if truncated:
        return {"items": results, "truncated": True,
                "note": f"Results truncated at {limit}. Refine your query for complete data."}
    return results   # plain list when not truncated (backwards compatible)

def _unwrap(result) -> list:
    """Unwrap _safe_list result to a plain list."""
    if isinstance(result, dict):
        if "error" in result:
            return []
        if "items" in result:
            return result["items"]
        return []
    return result if isinstance(result, list) else []

def _is_dry_run(action_desc: str) -> dict:
    if DRY_RUN:
        cprint(f"[yellow]🔒 DRY-RUN: Would have executed → {action_desc}[/yellow]")
        return {"dry_run": True, "would_execute": action_desc}
    return {}

DESTRUCTIVE_TOOLS = {
    "delete_service", "delete_user", "delete_team",
    "delete_escalation_policy", "delete_schedule_override",
    "delete_maintenance_window", "manage_incident",
}

# =====================================================
# TOOL IMPLEMENTATIONS
# =====================================================

# ── Incidents ─────────────────────────────────────────

def tool_list_incidents(args: dict) -> dict:
    params = {
        "since":   args["since"],
        "until":   args["until"],
        "sort_by": f"{args.get('sort_by', 'created_at')}:desc",
    }
    params["statuses[]"] = args.get("statuses") or ["triggered","acknowledged","resolved"]
    if args.get("urgencies"):
        params["urgencies[]"] = args["urgencies"]
    if args.get("service_ids"):
        params["service_ids[]"] = args["service_ids"]
    if args.get("team_ids"):
        params["team_ids[]"] = args["team_ids"]

    raw = _safe_list("incidents", params, args.get("limit", 25))
    if isinstance(raw, dict) and "error" in raw:
        return raw

    items = _unwrap(raw) if isinstance(raw, dict) else raw
    truncated = isinstance(raw, dict) and raw.get("truncated", False)

    results = [
        {
            "id": i["id"],
            "title": i["title"],
            "status": i["status"],
            "urgency": i.get("urgency"),
            "priority": i.get("priority", {}).get("summary") if i.get("priority") else None,
            "service": i.get("service", {}).get("summary"),
            "service_id": i.get("service", {}).get("id"),
            "created_at": i["created_at"],
            "resolved_at": i.get("resolved_at"),
            "assigned_to": [a.get("assignee",{}).get("summary") for a in i.get("assignments",[])],
            "escalation_policy": i.get("escalation_policy",{}).get("summary"),
            "incident_number": i.get("incident_number"),
            "html_url": i.get("html_url"),
        }
        for i in items
    ]
    out = {"total": len(results), "incidents": results}
    if truncated:
        out["truncated"] = True
    return out


@with_retry()
def tool_get_incident(args: dict) -> dict:
    try:
        inc = pd_client.rget(f"incidents/{args['incident_id']}")
        return {
            "id": inc["id"], "title": inc["title"], "status": inc["status"],
            "urgency": inc.get("urgency"), "priority": inc.get("priority"),
            "service": inc.get("service"), "created_at": inc["created_at"],
            "resolved_at": inc.get("resolved_at"),
            "last_status_change_at": inc.get("last_status_change_at"),
            "assignments": inc.get("assignments"),
            "acknowledgements": inc.get("acknowledgements"),
            "escalation_policy": inc.get("escalation_policy"),
            "teams": inc.get("teams"), "alert_counts": inc.get("alert_counts"),
            "body": inc.get("body"), "html_url": inc.get("html_url"),
            "incident_number": inc.get("incident_number"),
        }
    except Exception as e:
        return {"error": str(e)}


@with_retry()
def tool_get_incident_timeline(args: dict) -> dict:
    try:
        logs = list(pd_client.iter_all(
            f"incidents/{args['incident_id']}/log_entries",
            params={"is_overview": False}
        ))
        return {
            "incident_id": args["incident_id"],
            "timeline_entries": [
                {
                    "type": l["type"], "created_at": l["created_at"],
                    "summary": l.get("summary",""),
                    "agent": l.get("agent",{}).get("summary") if l.get("agent") else None,
                    "channel": l.get("channel",{}).get("type") if l.get("channel") else None,
                    "assignees": [a.get("summary") for a in l.get("assignees",[])] if l.get("assignees") else None,
                }
                for l in logs
            ]
        }
    except Exception as e:
        return {"error": str(e)}


def tool_get_incident_notes(args: dict) -> dict:
    raw = _safe_list(f"incidents/{args['incident_id']}/notes")
    if isinstance(raw, dict) and "error" in raw:
        return raw
    items = _unwrap(raw) if isinstance(raw, dict) else raw
    return {
        "incident_id": args["incident_id"],
        "notes": [
            {"id": n["id"], "content": n["content"], "created_at": n["created_at"],
             "user": n.get("user",{}).get("summary")}
            for n in items
        ]
    }


def tool_get_incident_alerts(args: dict) -> dict:
    raw = _safe_list(f"incidents/{args['incident_id']}/alerts")
    if isinstance(raw, dict) and "error" in raw:
        return raw
    items = _unwrap(raw) if isinstance(raw, dict) else raw
    return {
        "incident_id": args["incident_id"],
        "alerts": [
            {
                "id": a["id"], "status": a["status"], "severity": a.get("severity"),
                "summary": a.get("summary"), "created_at": a["created_at"],
                "body": a.get("body",{}).get("details") if a.get("body") else None,
            }
            for a in items
        ]
    }


@with_retry()
def tool_manage_incident(args: dict) -> dict:
    dry = _is_dry_run(f"manage_incident {args['incident_id']} → {args['action']}")
    if dry:
        return dry

    iid    = args["incident_id"]
    action = args["action"]
    try:
        if action in ("acknowledge", "resolve"):
            status = "acknowledged" if action == "acknowledge" else "resolved"
            pd_client.rput("incidents", json=[{"id": iid, "type": "incident_reference", "status": status}])
            return {"success": True, "action": action, "incident_id": iid}

        elif action == "reassign":
            atype = args.get("assignee_type", "user_reference")
            pd_client.rput("incidents", json=[{
                "id": iid, "type": "incident_reference",
                "assignments": [{"assignee": {"id": args["assignee_id"], "type": atype}}]
            }])
            return {"success": True, "action": "reassigned", "incident_id": iid, "assignee": args["assignee_id"]}

        elif action == "change_urgency":
            pd_client.rput("incidents", json=[{"id": iid, "type": "incident_reference", "urgency": args["urgency"]}])
            return {"success": True, "action": "urgency_changed", "incident_id": iid, "urgency": args["urgency"]}

        elif action == "snooze":
            resp = pd_client.post(f"incidents/{iid}/snooze", json={"duration": args.get("snooze_duration", 3600)})
            return {"success": resp.ok, "action": "snoozed", "incident_id": iid}

        elif action == "add_note":
            resp = pd_client.post(f"incidents/{iid}/notes", json={"note": {"content": args["note_content"]}})
            return {"success": resp.ok, "action": "note_added", "incident_id": iid}

        elif action == "merge":
            merge_refs = [{"id": mid, "type": "incident_reference"} for mid in args.get("merge_ids",[])]
            resp = pd_client.put(f"incidents/{iid}/merge", json={"source_incidents": merge_refs})
            return {"success": resp.ok, "action": "merged", "incident_id": iid}

        return {"error": f"Unknown action: {action}"}
    except Exception as e:
        return {"error": str(e)}


@with_retry()
def tool_create_incident(args: dict) -> dict:
    dry = _is_dry_run(f"create_incident title='{args['title']}' service={args['service_id']}")
    if dry:
        return dry
    try:
        body = {
            "type": "incident",
            "title": args["title"],
            "service": {"id": args["service_id"], "type": "service_reference"},
            "urgency": args.get("urgency", "high"),
        }
        if args.get("body"):
            body["body"] = {"type": "incident_body", "details": args["body"]}
        if args.get("escalation_policy_id"):
            body["escalation_policy"] = {"id": args["escalation_policy_id"], "type": "escalation_policy_reference"}
        if args.get("priority_id"):
            body["priority"] = {"id": args["priority_id"], "type": "priority_reference"}
        inc = pd_client.rpost("incidents", json=body)
        return {"success": True, "incident": {"id": inc["id"], "title": inc["title"], "html_url": inc.get("html_url")}}
    except Exception as e:
        return {"error": str(e)}


# ── Services ──────────────────────────────────────────

def tool_list_services(args: dict) -> dict:
    cache_key = f"services:{args.get('query','')}:{args.get('team_ids','')}"
    cached = cache_get(cache_key)
    if cached:
        return cached

    params = {}
    if args.get("query"):     params["query"] = args["query"]
    if args.get("team_ids"):  params["team_ids[]"] = args["team_ids"]
    if args.get("include"):   params["include[]"] = args["include"]

    raw = _safe_list("services", params)
    if isinstance(raw, dict) and "error" in raw:
        return raw

    items = _unwrap(raw) if isinstance(raw, dict) else raw
    result = {
        "total": len(items),
        "services": [
            {
                "id": s["id"], "name": s["name"], "status": s.get("status"),
                "description": s.get("description",""),
                "escalation_policy": s.get("escalation_policy",{}).get("summary"),
                "teams": [t.get("summary") for t in s.get("teams",[])],
                "html_url": s.get("html_url"),
            }
            for s in items
        ]
    }
    cache_set(cache_key, result)
    return result


@with_retry()
def tool_get_service(args: dict) -> dict:
    try:
        params = {}
        if args.get("include"):
            params["include[]"] = args["include"]
        return pd_client.rget(f"services/{args['service_id']}", params=params)
    except Exception as e:
        return {"error": str(e)}


@with_retry()
def tool_create_service(args: dict) -> dict:
    dry = _is_dry_run(f"create_service name='{args['name']}'")
    if dry: return dry
    try:
        body: dict = {
            "type": "service",
            "name": args["name"],
            "escalation_policy": {"id": args["escalation_policy_id"], "type": "escalation_policy_reference"},
        }
        if args.get("description"):   body["description"] = args["description"]
        if args.get("urgency_rule"):  body["incident_urgency_rule"] = {"type": "constant", "urgency": args["urgency_rule"]}
        if args.get("alert_creation"): body["alert_creation"] = args["alert_creation"]
        svc = pd_client.rpost("services", json=body)
        cache_clear("services:")
        return {"success": True, "service": {"id": svc["id"], "name": svc["name"]}}
    except Exception as e:
        return {"error": str(e)}


@with_retry()
def tool_update_service(args: dict) -> dict:
    """Fixed: builds a clean update payload instead of roundtripping the full PD object."""
    dry = _is_dry_run(f"update_service {args['service_id']}")
    if dry: return dry
    try:
        sid = args["service_id"]
        payload: dict = {"id": sid, "type": "service"}
        if args.get("name"):                 payload["name"] = args["name"]
        if args.get("description"):          payload["description"] = args["description"]
        if args.get("status"):               payload["status"] = args["status"]
        if args.get("escalation_policy_id"):
            payload["escalation_policy"] = {"id": args["escalation_policy_id"], "type": "escalation_policy_reference"}
        updated = pd_client.rput(f"services/{sid}", json=payload)
        cache_clear("services:")
        return {"success": True, "service": {"id": updated["id"], "name": updated["name"], "status": updated.get("status")}}
    except Exception as e:
        return {"error": str(e)}


@with_retry()
def tool_delete_service(args: dict) -> dict:
    dry = _is_dry_run(f"delete_service {args['service_id']}")
    if dry: return dry
    try:
        pd_client.delete(f"services/{args['service_id']}")
        cache_clear("services:")
        return {"success": True, "deleted_service_id": args["service_id"]}
    except Exception as e:
        return {"error": str(e)}


# ── Service Integrations ───────────────────────────────

def tool_list_service_integrations(args: dict) -> dict:
    raw = _safe_list(f"services/{args['service_id']}/integrations")
    if isinstance(raw, dict) and "error" in raw:
        return raw
    items = _unwrap(raw) if isinstance(raw, dict) else raw
    return {
        "integrations": [
            {
                "id": i["id"], "name": i.get("name"), "type": i.get("type"),
                "integration_key": i.get("integration_key"),
                "vendor": i.get("vendor",{}).get("summary") if i.get("vendor") else None,
            }
            for i in items
        ]
    }


@with_retry()
def tool_create_service_integration(args: dict) -> dict:
    dry = _is_dry_run(f"create_service_integration service={args['service_id']} type={args['type']}")
    if dry: return dry
    try:
        body = {"type": args["type"], "name": args["name"]}
        if args.get("vendor_id"):
            body["vendor"] = {"id": args["vendor_id"], "type": "vendor_reference"}
        resp = pd_client.rpost(f"services/{args['service_id']}/integrations", json=body)
        return {"success": True, "integration": {"id": resp["id"], "integration_key": resp.get("integration_key")}}
    except Exception as e:
        return {"error": str(e)}


# ── Users ──────────────────────────────────────────────

def tool_list_users(args: dict) -> dict:
    cache_key = f"users:{args.get('query','')}:{args.get('team_ids','')}"
    cached = cache_get(cache_key)
    if cached:
        return cached

    params = {}
    if args.get("query"):    params["query"] = args["query"]
    if args.get("team_ids"): params["team_ids[]"] = args["team_ids"]
    if args.get("include"):  params["include[]"] = args["include"]

    raw = _safe_list("users", params)
    if isinstance(raw, dict) and "error" in raw:
        return raw

    items = _unwrap(raw) if isinstance(raw, dict) else raw
    result = {
        "total": len(items),
        "users": [
            {
                "id": u["id"], "name": u["name"], "email": u.get("email"),
                "role": u.get("role"), "job_title": u.get("job_title"),
                "time_zone": u.get("time_zone"), "html_url": u.get("html_url"),
            }
            for u in items
        ]
    }
    cache_set(cache_key, result)
    return result


@with_retry()
def tool_get_user(args: dict) -> dict:
    try:
        params = {}
        if args.get("include"):
            params["include[]"] = args["include"]
        return pd_client.rget(f"users/{args['user_id']}", params=params)
    except Exception as e:
        return {"error": str(e)}


@with_retry()
def tool_create_user(args: dict) -> dict:
    dry = _is_dry_run(f"create_user name='{args['name']}' email={args['email']}")
    if dry: return dry
    try:
        body = {"type": "user", "name": args["name"], "email": args["email"]}
        for f in ("role","time_zone","job_title"):
            if args.get(f): body[f] = args[f]
        user = pd_client.rpost("users", json=body)
        cache_clear("users:")
        return {"success": True, "user": {"id": user["id"], "name": user["name"], "email": user.get("email")}}
    except Exception as e:
        return {"error": str(e)}


@with_retry()
def tool_update_user(args: dict) -> dict:
    """Fixed: builds a clean update payload."""
    dry = _is_dry_run(f"update_user {args['user_id']}")
    if dry: return dry
    try:
        uid = args["user_id"]
        payload: dict = {"id": uid, "type": "user"}
        for field in ("name","email","role","job_title","time_zone"):
            if args.get(field): payload[field] = args[field]
        updated = pd_client.rput(f"users/{uid}", json=payload)
        cache_clear("users:")
        return {"success": True, "user": {"id": updated["id"], "name": updated["name"]}}
    except Exception as e:
        return {"error": str(e)}


@with_retry()
def tool_delete_user(args: dict) -> dict:
    dry = _is_dry_run(f"delete_user {args['user_id']}")
    if dry: return dry
    try:
        pd_client.delete(f"users/{args['user_id']}")
        cache_clear("users:")
        return {"success": True, "deleted_user_id": args["user_id"]}
    except Exception as e:
        return {"error": str(e)}


def tool_get_user_contact_methods(args: dict) -> dict:
    raw = _safe_list(f"users/{args['user_id']}/contact_methods")
    items = _unwrap(raw) if isinstance(raw, dict) else raw
    return {
        "user_id": args["user_id"],
        "contact_methods": [
            {"id": m["id"], "type": m["type"], "label": m.get("label"),
             "address": m.get("address"), "country_code": m.get("country_code")}
            for m in items
        ]
    }


def tool_get_user_notification_rules(args: dict) -> dict:
    raw = _safe_list(f"users/{args['user_id']}/notification_rules")
    items = _unwrap(raw) if isinstance(raw, dict) else raw
    return {
        "user_id": args["user_id"],
        "notification_rules": [
            {
                "id": r["id"], "type": r["type"], "urgency": r.get("urgency"),
                "start_delay_in_minutes": r.get("start_delay_in_minutes"),
                "contact_method": r.get("contact_method",{}).get("summary") if r.get("contact_method") else None,
            }
            for r in items
        ]
    }


# ── Teams ──────────────────────────────────────────────

def tool_list_teams(args: dict) -> dict:
    params = {}
    if args.get("query"): params["query"] = args["query"]
    raw = _safe_list("teams", params)
    if isinstance(raw, dict) and "error" in raw: return raw
    items = _unwrap(raw) if isinstance(raw, dict) else raw
    return {"total": len(items), "teams": [
        {"id": t["id"], "name": t["name"], "description": t.get("description",""), "html_url": t.get("html_url")}
        for t in items
    ]}


@with_retry()
def tool_get_team(args: dict) -> dict:
    try:
        return pd_client.rget(f"teams/{args['team_id']}")
    except Exception as e:
        return {"error": str(e)}


@with_retry()
def tool_create_team(args: dict) -> dict:
    dry = _is_dry_run(f"create_team name='{args['name']}'")
    if dry: return dry
    try:
        body = {"type": "team", "name": args["name"]}
        if args.get("description"): body["description"] = args["description"]
        team = pd_client.rpost("teams", json=body)
        return {"success": True, "team": {"id": team["id"], "name": team["name"]}}
    except Exception as e:
        return {"error": str(e)}


@with_retry()
def tool_update_team(args: dict) -> dict:
    """Fixed: clean payload."""
    dry = _is_dry_run(f"update_team {args['team_id']}")
    if dry: return dry
    try:
        tid = args["team_id"]
        payload: dict = {"id": tid, "type": "team"}
        if args.get("name"):        payload["name"] = args["name"]
        if args.get("description"): payload["description"] = args["description"]
        updated = pd_client.rput(f"teams/{tid}", json=payload)
        return {"success": True, "team": {"id": updated["id"], "name": updated["name"]}}
    except Exception as e:
        return {"error": str(e)}


@with_retry()
def tool_delete_team(args: dict) -> dict:
    dry = _is_dry_run(f"delete_team {args['team_id']}")
    if dry: return dry
    try:
        pd_client.delete(f"teams/{args['team_id']}")
        return {"success": True, "deleted_team_id": args["team_id"]}
    except Exception as e:
        return {"error": str(e)}


@with_retry()
def tool_manage_team_membership(args: dict) -> dict:
    dry = _is_dry_run(f"manage_team_membership team={args['team_id']} user={args['user_id']} action={args['action']}")
    if dry: return dry
    try:
        tid, uid = args["team_id"], args["user_id"]
        if args["action"] == "add":
            role = args.get("role", "responder")
            pd_client.put(f"teams/{tid}/users/{uid}", json={"role": role})
            return {"success": True, "action": "added", "team_id": tid, "user_id": uid, "role": role}
        else:
            pd_client.delete(f"teams/{tid}/users/{uid}")
            return {"success": True, "action": "removed", "team_id": tid, "user_id": uid}
    except Exception as e:
        return {"error": str(e)}


def tool_list_team_members(args: dict) -> dict:
    raw = _safe_list(f"teams/{args['team_id']}/members")
    items = _unwrap(raw) if isinstance(raw, dict) else raw
    return {
        "team_id": args["team_id"],
        "members": [
            {"user_id": m.get("user",{}).get("id"), "name": m.get("user",{}).get("summary"), "role": m.get("role")}
            for m in items
        ]
    }


# ── Escalation Policies ────────────────────────────────

def tool_list_escalation_policies(args: dict) -> dict:
    params = {}
    if args.get("query"):    params["query"] = args["query"]
    if args.get("team_ids"): params["team_ids[]"] = args["team_ids"]
    raw = _safe_list("escalation_policies", params)
    if isinstance(raw, dict) and "error" in raw: return raw
    items = _unwrap(raw) if isinstance(raw, dict) else raw
    return {"total": len(items), "escalation_policies": [
        {
            "id": ep["id"], "name": ep["name"], "description": ep.get("description",""),
            "num_loops": ep.get("num_loops"),
            "teams": [t.get("summary") for t in ep.get("teams",[])],
            "rules_count": len(ep.get("escalation_rules",[])),
            "html_url": ep.get("html_url"),
        }
        for ep in items
    ]}


@with_retry()
def tool_get_escalation_policy(args: dict) -> dict:
    try:
        return pd_client.rget(f"escalation_policies/{args['policy_id']}")
    except Exception as e:
        return {"error": str(e)}


@with_retry()
def tool_create_escalation_policy(args: dict) -> dict:
    dry = _is_dry_run(f"create_escalation_policy name='{args['name']}'")
    if dry: return dry
    try:
        rules = []
        for rule in args.get("escalation_rules", []):
            targets = [{"id": tid, "type": rule.get("target_type","user_reference")} for tid in rule.get("target_ids",[])]
            rules.append({"escalation_delay_in_minutes": rule.get("escalation_delay_in_minutes",30), "targets": targets})
        body: dict = {"type": "escalation_policy", "name": args["name"], "escalation_rules": rules}
        if args.get("description"): body["description"] = args["description"]
        if args.get("num_loops"):   body["num_loops"] = args["num_loops"]
        if args.get("team_id"):     body["teams"] = [{"id": args["team_id"], "type": "team_reference"}]
        ep = pd_client.rpost("escalation_policies", json=body)
        return {"success": True, "escalation_policy": {"id": ep["id"], "name": ep["name"]}}
    except Exception as e:
        return {"error": str(e)}


@with_retry()
def tool_update_escalation_policy(args: dict) -> dict:
    """Fixed: clean payload."""
    dry = _is_dry_run(f"update_escalation_policy {args['policy_id']}")
    if dry: return dry
    try:
        pid = args["policy_id"]
        payload: dict = {"id": pid, "type": "escalation_policy"}
        if args.get("name"):              payload["name"] = args["name"]
        if args.get("description"):       payload["description"] = args["description"]
        if args.get("num_loops") is not None: payload["num_loops"] = args["num_loops"]
        if args.get("escalation_rules"):  payload["escalation_rules"] = args["escalation_rules"]
        updated = pd_client.rput(f"escalation_policies/{pid}", json=payload)
        return {"success": True, "escalation_policy": {"id": updated["id"], "name": updated["name"]}}
    except Exception as e:
        return {"error": str(e)}


@with_retry()
def tool_delete_escalation_policy(args: dict) -> dict:
    dry = _is_dry_run(f"delete_escalation_policy {args['policy_id']}")
    if dry: return dry
    try:
        pd_client.delete(f"escalation_policies/{args['policy_id']}")
        return {"success": True, "deleted_policy_id": args["policy_id"]}
    except Exception as e:
        return {"error": str(e)}


# ── Schedules ──────────────────────────────────────────

def tool_list_schedules(args: dict) -> dict:
    params = {}
    if args.get("query"): params["query"] = args["query"]
    raw = _safe_list("schedules", params)
    if isinstance(raw, dict) and "error" in raw: return raw
    items = _unwrap(raw) if isinstance(raw, dict) else raw
    return {"total": len(items), "schedules": [
        {"id": s["id"], "name": s["name"], "description": s.get("description",""),
         "time_zone": s.get("time_zone"), "html_url": s.get("html_url"),
         "users": [u.get("summary") for u in s.get("users",[])]}
        for s in items
    ]}


@with_retry()
def tool_get_schedule(args: dict) -> dict:
    try:
        params = {}
        if args.get("since"): params["since"] = args["since"]
        if args.get("until"): params["until"] = args["until"]
        return pd_client.rget(f"schedules/{args['schedule_id']}", params=params)
    except Exception as e:
        return {"error": str(e)}


def tool_list_schedule_overrides(args: dict) -> dict:
    raw = _safe_list(f"schedules/{args['schedule_id']}/overrides",
                     {"since": args["since"], "until": args["until"]})
    items = _unwrap(raw) if isinstance(raw, dict) and "items" in raw else (raw if isinstance(raw, list) else [])
    return {"schedule_id": args["schedule_id"], "overrides": [
        {"id": o["id"], "start": o["start"], "end": o["end"],
         "user": o.get("user",{}).get("summary")} for o in items
    ]}


@with_retry()
def tool_create_schedule_override(args: dict) -> dict:
    dry = _is_dry_run(f"create_schedule_override schedule={args['schedule_id']} user={args['user_id']}")
    if dry: return dry
    try:
        body = {"start": args["start"], "end": args["end"],
                "user": {"id": args["user_id"], "type": "user_reference"}}
        resp = pd_client.rpost(f"schedules/{args['schedule_id']}/overrides", json=[body])
        return {"success": True, "override": resp}
    except Exception as e:
        return {"error": str(e)}


@with_retry()
def tool_delete_schedule_override(args: dict) -> dict:
    dry = _is_dry_run(f"delete_schedule_override {args['override_id']}")
    if dry: return dry
    try:
        pd_client.delete(f"schedules/{args['schedule_id']}/overrides/{args['override_id']}")
        return {"success": True}
    except Exception as e:
        return {"error": str(e)}


def tool_list_schedule_users_on_call(args: dict) -> dict:
    params = {}
    if args.get("since"): params["since"] = args["since"]
    if args.get("until"): params["until"] = args["until"]
    raw = _safe_list(f"schedules/{args['schedule_id']}/users", params)
    items = _unwrap(raw) if isinstance(raw, dict) else raw
    return {"schedule_id": args["schedule_id"], "users": [
        {"id": u["id"], "name": u["name"], "email": u.get("email")} for u in items
    ]}


# ── On-Calls ───────────────────────────────────────────

def tool_list_oncalls(args: dict) -> dict:
    params = {}
    for k, pk in (("schedule_ids","schedule_ids[]"), ("escalation_policy_ids","escalation_policy_ids[]"),
                  ("user_ids","user_ids[]")):
        if args.get(k): params[pk] = args[k]
    for k in ("since","until"):
        if args.get(k): params[k] = args[k]
    if args.get("earliest"): params["earliest"] = args["earliest"]

    raw = _safe_list("oncalls", params)
    if isinstance(raw, dict) and "error" in raw: return raw
    items = _unwrap(raw) if isinstance(raw, dict) else raw
    return {"total": len(items), "oncalls": [
        {
            "user": oc.get("user",{}).get("summary"),
            "user_id": oc.get("user",{}).get("id"),
            "schedule": oc.get("schedule",{}).get("summary") if oc.get("schedule") else None,
            "escalation_policy": oc.get("escalation_policy",{}).get("summary"),
            "escalation_level": oc.get("escalation_level"),
            "start": oc.get("start"), "end": oc.get("end"),
        }
        for oc in items
    ]}


# ── Priorities ─────────────────────────────────────────

def tool_list_priorities(_args: dict) -> dict:
    raw = _safe_list("priorities")
    items = _unwrap(raw) if isinstance(raw, dict) else raw
    return {"priorities": [
        {"id": p["id"], "name": p["name"], "description": p.get("description",""), "order": p.get("order")}
        for p in items
    ]}


# ── Maintenance Windows ────────────────────────────────

def tool_list_maintenance_windows(args: dict) -> dict:
    params = {}
    if args.get("service_ids"): params["service_ids[]"] = args["service_ids"]
    if args.get("team_ids"):    params["team_ids[]"] = args["team_ids"]
    if args.get("filter"):      params["filter"] = args["filter"]
    raw = _safe_list("maintenance_windows", params)
    if isinstance(raw, dict) and "error" in raw: return raw
    items = _unwrap(raw) if isinstance(raw, dict) else raw
    return {"total": len(items), "maintenance_windows": [
        {
            "id": mw["id"], "description": mw.get("description",""),
            "start_time": mw.get("start_time"), "end_time": mw.get("end_time"),
            "services": [s.get("summary") for s in mw.get("services",[])],
            "created_by": mw.get("created_by",{}).get("summary") if mw.get("created_by") else None,
        }
        for mw in items
    ]}


@with_retry()
def tool_create_maintenance_window(args: dict) -> dict:
    dry = _is_dry_run(f"create_maintenance_window start={args['start_time']} end={args['end_time']}")
    if dry: return dry
    try:
        body = {
            "type": "maintenance_window",
            "start_time": args["start_time"], "end_time": args["end_time"],
            "services": [{"id": sid, "type": "service_reference"} for sid in args["service_ids"]]
        }
        if args.get("description"): body["description"] = args["description"]
        mw = pd_client.rpost("maintenance_windows", json=body)
        return {"success": True, "maintenance_window": {"id": mw["id"]}}
    except Exception as e:
        return {"error": str(e)}


@with_retry()
def tool_delete_maintenance_window(args: dict) -> dict:
    dry = _is_dry_run(f"delete_maintenance_window {args['window_id']}")
    if dry: return dry
    try:
        pd_client.delete(f"maintenance_windows/{args['window_id']}")
        return {"success": True, "deleted": args["window_id"]}
    except Exception as e:
        return {"error": str(e)}


# ── Log Entries ────────────────────────────────────────

def tool_list_log_entries(args: dict) -> dict:
    params = {}
    for k in ("since","until"):
        if args.get(k): params[k] = args[k]
    if args.get("is_overview"): params["is_overview"] = args["is_overview"]
    raw = _safe_list("log_entries", params)
    if isinstance(raw, dict) and "error" in raw: return raw
    items = _unwrap(raw) if isinstance(raw, dict) else raw
    return {"total": len(items), "log_entries": [
        {
            "type": l["type"], "created_at": l["created_at"],
            "summary": l.get("summary",""),
            "incident": l.get("incident",{}).get("summary") if l.get("incident") else None,
            "service": l.get("service",{}).get("summary") if l.get("service") else None,
            "agent": l.get("agent",{}).get("summary") if l.get("agent") else None,
        }
        for l in items
    ]}


# ── Notifications ──────────────────────────────────────

def tool_list_notifications(args: dict) -> dict:
    params = {"since": args["since"], "until": args["until"]}
    if args.get("filter"):  params["filter"] = args["filter"]
    if args.get("include"): params["include[]"] = args["include"]
    raw = _safe_list("notifications", params)
    if isinstance(raw, dict) and "error" in raw: return raw
    items = _unwrap(raw) if isinstance(raw, dict) else raw
    return {"total": len(items), "notifications": [
        {
            "type": n.get("type"), "started_at": n.get("started_at"),
            "address": n.get("address"),
            "user": n.get("user",{}).get("summary") if n.get("user") else None,
            "incident": {"id": n.get("incident",{}).get("id"), "summary": n.get("incident",{}).get("summary")} if n.get("incident") else None,
        }
        for n in items
    ]}


# ── Analytics ──────────────────────────────────────────

@with_retry()
def tool_get_analytics_incidents(args: dict) -> dict:
    try:
        filters: dict = {}
        if args.get("since"):        filters["created_at_start"] = args["since"]
        if args.get("until"):        filters["created_at_end"]   = args["until"]
        if args.get("service_ids"):  filters["service_ids"] = args["service_ids"]
        if args.get("team_ids"):     filters["team_ids"] = args["team_ids"]
        if args.get("urgencies") and len(args["urgencies"]) == 1:
            filters["urgency"] = args["urgencies"][0]

        results = []
        for item in pd_client.iter_analytics_raw_incidents(filters=filters):
            results.append({
                "incident_id": item.get("id"),
                "incident_number": item.get("incident_number"),
                "title": item.get("title", item.get("description","")),
                "urgency": item.get("urgency"), "status": item.get("status"),
                "service_name": item.get("service_name"),
                "created_at": item.get("created_at"), "resolved_at": item.get("resolved_at"),
                "seconds_to_first_ack": item.get("seconds_to_first_ack"),
                "seconds_to_resolve": item.get("seconds_to_resolve"),
                "seconds_to_engage": item.get("seconds_to_engage"),
                "engaged_seconds": item.get("engaged_seconds"),
                "escalation_count": item.get("escalation_count"),
                "assignment_count": item.get("assignment_count"),
                "engaged_user_count": item.get("engaged_user_count"),
            })
            if len(results) >= MAX_RESULTS:
                break
        return {"total": len(results), "analytics_incidents": results}
    except Exception as e:
        return {"error": str(e)}


@with_retry()
def tool_get_analytics_services(args: dict) -> dict:
    try:
        filters: dict = {}
        if args.get("since"):        filters["created_at_start"] = args["since"]
        if args.get("until"):        filters["created_at_end"]   = args["until"]
        if args.get("service_ids"):  filters["service_ids"] = args["service_ids"]
        if args.get("team_ids"):     filters["team_ids"] = args["team_ids"]
        resp = pd_client.post("analytics/metrics/incidents/services", json={"filters": filters})
        if resp.ok:
            return resp.json().get("data", [])
        return {"error": f"HTTP {resp.status_code}: {resp.text}"}
    except Exception as e:
        return {"error": str(e)}


@with_retry()
def tool_get_analytics_teams(args: dict) -> dict:
    try:
        filters: dict = {}
        if args.get("since"):    filters["created_at_start"] = args["since"]
        if args.get("until"):    filters["created_at_end"]   = args["until"]
        if args.get("team_ids"): filters["team_ids"] = args["team_ids"]
        resp = pd_client.post("analytics/metrics/incidents/teams", json={"filters": filters})
        if resp.ok:
            return resp.json().get("data", [])
        return {"error": f"HTTP {resp.status_code}: {resp.text}"}
    except Exception as e:
        return {"error": str(e)}


# ── Tags ───────────────────────────────────────────────

def tool_list_tags(args: dict) -> dict:
    params = {}
    if args.get("query"): params["query"] = args["query"]
    raw = _safe_list("tags", params)
    if isinstance(raw, dict) and "error" in raw: return raw
    items = _unwrap(raw) if isinstance(raw, dict) else raw
    return {"total": len(items), "tags": [{"id": t["id"], "label": t.get("label")} for t in items]}


@with_retry()
def tool_manage_tags(args: dict) -> dict:
    dry = _is_dry_run(f"manage_tags {args['resource_type']}/{args['resource_id']}")
    if dry: return dry
    try:
        body: dict = {}
        if args.get("add_tags"):    body["add"] = args["add_tags"]
        if args.get("remove_tags"): body["remove"] = args["remove_tags"]
        pd_client.post(f"{args['resource_type']}/{args['resource_id']}/change_tags", json=body)
        return {"success": True}
    except Exception as e:
        return {"error": str(e)}


# ── Vendors / Response Plays / Business Services ────────

def tool_list_vendors(args: dict) -> dict:
    params = {}
    if args.get("query"): params["query"] = args["query"]
    raw = _safe_list("vendors", params)
    items = _unwrap(raw) if isinstance(raw, dict) else raw
    return {"total": len(items), "vendors": [
        {"id": v["id"], "name": v["name"], "description": v.get("description","")[:200],
         "website_url": v.get("website_url")} for v in items
    ]}


def tool_list_response_plays(args: dict) -> dict:
    params = {}
    if args.get("query"): params["query"] = args["query"]
    raw = _safe_list("response_plays", params)
    items = _unwrap(raw) if isinstance(raw, dict) else raw
    return {"total": len(items), "response_plays": [
        {"id": p["id"], "name": p.get("name"), "description": p.get("description",""), "type": p.get("type")}
        for p in items
    ]}


def tool_list_business_services(_args: dict) -> dict:
    raw = _safe_list("business_services")
    items = _unwrap(raw) if isinstance(raw, dict) else raw
    return {"total": len(items), "business_services": [
        {"id": s["id"], "name": s["name"], "description": s.get("description",""),
         "point_of_contact": s.get("point_of_contact"), "html_url": s.get("html_url")}
        for s in items
    ]}


@with_retry()
def tool_get_business_service(args: dict) -> dict:
    try:
        return pd_client.rget(f"business_services/{args['business_service_id']}")
    except Exception as e:
        return {"error": str(e)}


@with_retry()
def tool_get_service_dependencies(args: dict) -> dict:
    try:
        stype = "business_services" if args["service_type"] == "business" else "services"
        resp = pd_client.get(f"service_dependencies/{stype}/{args['service_id']}")
        return resp.json().get("relationships", []) if resp.ok else {"error": f"HTTP {resp.status_code}"}
    except Exception as e:
        return {"error": str(e)}


def tool_list_status_dashboards(_args: dict) -> dict:
    raw = _safe_list("status_dashboards")
    items = _unwrap(raw) if isinstance(raw, dict) else raw
    return {"dashboards": [{"id": d["id"], "name": d.get("name"), "url_slug": d.get("url_slug")} for d in items]}


def tool_list_audit_records(args: dict) -> dict:
    params = {}
    if args.get("since"):  params["since"] = args["since"]
    if args.get("until"):  params["until"] = args["until"]
    if args.get("root_resource_types"): params["root_resource_types[]"] = args["root_resource_types"]
    try:
        records = []
        for r in pd_client.iter_cursor("audit/records", params=params):
            records.append({
                "id": r.get("id"), "execution_time": r.get("execution_time"),
                "method": r.get("method",{}).get("type") if r.get("method") else None,
                "actors": [a.get("name", a.get("id")) for a in r.get("actors",[])],
                "root_resource": r.get("root_resource",{}).get("type"),
                "root_resource_name": r.get("root_resource",{}).get("name"),
                "action": r.get("action"),
            })
            if len(records) >= MAX_RESULTS: break
        return {"total": len(records), "audit_records": records}
    except Exception as e:
        return {"error": str(e)}


def tool_list_event_orchestrations(_args: dict) -> dict:
    raw = _safe_list("event_orchestrations")
    items = _unwrap(raw) if isinstance(raw, dict) else raw
    return {"orchestrations": [{"id": o["id"], "name": o.get("name"), "description": o.get("description",""), "routes": o.get("routes")} for o in items]}


def tool_list_rulesets(_args: dict) -> dict:
    raw = _safe_list("rulesets")
    items = _unwrap(raw) if isinstance(raw, dict) else raw
    return {"rulesets": [{"id": r["id"], "name": r.get("name"), "type": r.get("type")} for r in items]}


def tool_list_webhook_subscriptions(_args: dict) -> dict:
    raw = _safe_list("webhook_subscriptions")
    items = _unwrap(raw) if isinstance(raw, dict) else raw
    return {"subscriptions": [
        {"id": s["id"], "description": s.get("description",""), "type": s.get("type"),
         "delivery_method": s.get("delivery_method"), "events": s.get("events"), "filter": s.get("filter")}
        for s in items
    ]}


@with_retry()
def tool_create_webhook_subscription(args: dict) -> dict:
    dry = _is_dry_run(f"create_webhook_subscription url={args['delivery_method_url']}")
    if dry: return dry
    try:
        body: dict = {
            "type": "webhook_subscription",
            "delivery_method": {"type": "http_delivery_method", "url": args["delivery_method_url"]},
            "events": args["events"],
            "filter": {"type": args["filter_type"]}
        }
        if args.get("filter_id"):   body["filter"]["id"] = args["filter_id"]
        if args.get("description"): body["description"] = args["description"]
        sub = pd_client.rpost("webhook_subscriptions", json=body)
        return {"success": True, "subscription": {"id": sub["id"]}}
    except Exception as e:
        return {"error": str(e)}


@with_retry()
def tool_list_abilities(_args: dict) -> dict:
    try:
        resp = pd_client.get("abilities")
        return {"abilities": resp.json().get("abilities", [])} if resp.ok else {"error": f"HTTP {resp.status_code}"}
    except Exception as e:
        return {"error": str(e)}


def tool_list_extensions(args: dict) -> dict:
    params = {}
    if args.get("query"):                params["query"] = args["query"]
    if args.get("extension_schema_id"):  params["extension_schema_id"] = args["extension_schema_id"]
    raw = _safe_list("extensions", params)
    items = _unwrap(raw) if isinstance(raw, dict) else raw
    return {"total": len(items), "extensions": [
        {"id": e["id"], "name": e.get("name"), "type": e.get("type"),
         "endpoint_url": e.get("endpoint_url"),
         "extension_schema": e.get("extension_schema",{}).get("summary") if e.get("extension_schema") else None}
        for e in items
    ]}


def tool_list_incident_workflows(args: dict) -> dict:
    params = {}
    if args.get("query"): params["query"] = args["query"]
    raw = _safe_list("incident_workflows", params)
    items = _unwrap(raw) if isinstance(raw, dict) else raw
    return {"total": len(items), "workflows": [
        {"id": w["id"], "name": w.get("name"), "description": w.get("description",""), "created_at": w.get("created_at")}
        for w in items
    ]}


# ── Full compound analysis ─────────────────────────────

def tool_full_incident_analysis(args: dict) -> dict:
    params = {
        "since": args["since"], "until": args["until"],
        "statuses[]": ["triggered","acknowledged","resolved"],
        "sort_by": "created_at:desc",
    }
    if args.get("service_ids"): params["service_ids[]"] = args["service_ids"]
    if args.get("team_ids"):    params["team_ids[]"] = args["team_ids"]

    raw = _safe_list("incidents", params)
    if isinstance(raw, dict) and "error" in raw: return raw
    incidents = _unwrap(raw) if isinstance(raw, dict) else raw

    service_count: Counter = Counter()
    urgency_count: Counter = Counter()
    status_count:  Counter = Counter()
    total_mtta, total_mttr = [], []
    total_escalations = 0
    results = []

    for inc in incidents:
        trigger = ack = resolve = None
        escalations = 0
        try:
            for log in pd_client.iter_all(f"incidents/{inc['id']}/log_entries", params={"is_overview": True}):
                lt = log["type"]
                if lt == "trigger_log_entry":           trigger = log["created_at"]
                elif lt == "acknowledge_log_entry" and not ack: ack = log["created_at"]
                elif lt == "resolve_log_entry":         resolve = log["created_at"]
                elif lt == "escalate_log_entry":        escalations += 1
        except Exception:
            pass

        mtta = _diff_minutes(trigger, ack)
        mttr = _diff_minutes(trigger, resolve)
        if mtta is not None: total_mtta.append(mtta)
        if mttr is not None: total_mttr.append(mttr)
        total_escalations += escalations

        svc = inc.get("service",{}).get("summary","Unknown")
        service_count[svc] += 1
        urgency_count[inc.get("urgency","unknown")] += 1
        status_count[inc.get("status","unknown")] += 1

        results.append({
            "id": inc["id"], "incident_number": inc.get("incident_number"),
            "title": inc["title"], "service": svc,
            "status": inc["status"], "urgency": inc.get("urgency"),
            "created_at": inc["created_at"],
            "mtta_minutes": mtta, "mttr_minutes": mttr, "escalations": escalations,
        })

    avg_mtta = round(sum(total_mtta)/len(total_mtta), 2) if total_mtta else None
    avg_mttr = round(sum(total_mttr)/len(total_mttr), 2) if total_mttr else None

    return {
        "time_window": {"since": args["since"], "until": args["until"]},
        "summary": {
            "total_incidents": len(results),
            "status_distribution": dict(status_count),
            "urgency_distribution": dict(urgency_count),
            "service_distribution": dict(service_count),
            "average_mtta_minutes": avg_mtta,
            "average_mttr_minutes": avg_mttr,
            "total_escalations": total_escalations,
        },
        "incidents": results,
    }


# ══════════════════════════════════════════════════════
# NEW TOOL IMPLEMENTATIONS
# ══════════════════════════════════════════════════════

# ── NEW 1: resolve_time ────────────────────────────────

def tool_resolve_time(args: dict) -> dict:
    """Convert natural-language time expression to ISO-8601 UTC."""
    expr = args.get("expression", "")
    iso  = parse_nl_time(expr)
    if iso:
        return {"expression": expr, "iso8601_utc": iso}
    return {"error": f"Could not parse time expression: '{expr}'",
            "hint": "Try something like 'yesterday', 'last Monday 9am', '3 hours ago', '2025-01-15'"}


# ── NEW 2: generate_postmortem ─────────────────────────

def tool_generate_postmortem(args: dict) -> dict:
    """
    Auto-generate a structured postmortem document for an incident.
    Gathers incident details, timeline, notes, and alerts, then calls
    the LLM to produce a formatted markdown postmortem.
    """
    iid = args["incident_id"]

    # Gather all data
    cprint(f"[dim]  📋 Gathering incident data for {iid}…[/dim]")
    inc      = tool_get_incident({"incident_id": iid})
    timeline = tool_get_incident_timeline({"incident_id": iid})
    notes    = tool_get_incident_notes({"incident_id": iid})
    alerts   = tool_get_incident_alerts({"incident_id": iid})

    if "error" in inc:
        return {"error": f"Could not fetch incident: {inc['error']}"}

    # Build context block for LLM
    context = json.dumps({
        "incident":  inc,
        "timeline":  timeline.get("timeline_entries", [])[:50],
        "notes":     notes.get("notes", []),
        "alerts":    alerts.get("alerts", [])[:20],
    }, indent=2, default=str)

    postmortem_prompt = """You are an expert SRE writing a postmortem.
Given the following incident data, generate a comprehensive postmortem in Markdown with these sections:

# Postmortem: [Incident Title]

## Incident Summary
- **ID**: ...
- **Status**: ...
- **Severity/Urgency**: ...
- **Service**: ...
- **Duration**: ... (from trigger to resolve)
- **Affected users/systems**: (infer from data)

## Timeline
(Chronological table of key events: trigger, first page, ack, escalations, resolve)

## Root Cause
(Your best hypothesis from the available data)

## Impact
(What was affected, estimated scope)

## Contributing Factors
(What conditions allowed this to occur)

## Detection
(How was this detected, was it timely?)

## Response
(Summary of response actions taken)

## Action Items
| # | Action | Owner | Priority | Due Date |
|---|--------|-------|----------|----------|
| 1 | ...    | TBD   | High     | TBD      |

## Lessons Learned
(Key takeaways for the team)

---
*Generated by PagerDuty SRE AI Assistant*
"""

    try:
        resp = groq_client.chat.completions.create(
            model=MODEL_PRIMARY,
            messages=[
                {"role": "system", "content": postmortem_prompt},
                {"role": "user",   "content": f"Incident data:\n\n{context[:12000]}"}
            ],
            max_tokens=2000,
        )
        postmortem_text = resp.choices[0].message.content or ""
    except Exception as e:
        return {"error": f"LLM postmortem generation failed: {e}"}

    # Save to file
    filename = args.get("output_file") or f"postmortem_{iid}.md"
    try:
        Path(filename).write_text(postmortem_text)
        cprint(f"[green]✅ Postmortem saved to {filename}[/green]")
    except Exception as e:
        cprint(f"[yellow]⚠  Could not save file: {e}[/yellow]")

    return {
        "success": True,
        "incident_id": iid,
        "output_file": filename,
        "postmortem_preview": postmortem_text[:800] + ("…" if len(postmortem_text) > 800 else ""),
    }


# ── NEW 3: analyze_patterns ────────────────────────────

def tool_analyze_patterns(args: dict) -> dict:
    """
    Find recurring incident patterns: noisiest services, repeated titles,
    time-of-day clusters, escalation hot spots.
    """
    top_n = args.get("top_n", 10)
    params = {
        "since": args["since"], "until": args["until"],
        "statuses[]": ["triggered","acknowledged","resolved"],
        "sort_by": "created_at:desc",
    }
    if args.get("service_ids"): params["service_ids[]"] = args["service_ids"]
    if args.get("team_ids"):    params["team_ids[]"] = args["team_ids"]

    raw = _safe_list("incidents", params, MAX_RESULTS)
    incidents = _unwrap(raw) if isinstance(raw, dict) else raw

    if not incidents:
        return {"message": "No incidents found in the specified window."}

    service_counts:   Counter = Counter()
    hour_counts:      Counter = Counter()
    day_counts:       Counter = Counter()
    urgency_counts:   Counter = Counter()
    titles = []

    for inc in incidents:
        svc = inc.get("service",{}).get("summary","Unknown")
        service_counts[svc] += 1
        urgency_counts[inc.get("urgency","unknown")] += 1

        ts = _fmt_ts(inc.get("created_at",""))
        if ts:
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            hour_counts[ts.hour] += 1
            day_counts[ts.strftime("%A")] += 1

        titles.append(inc.get("title",""))

    # Find similar/duplicate titles via sequence matching
    title_clusters: list = []
    seen: set = set()
    for i, t1 in enumerate(titles):
        if i in seen: continue
        cluster = [t1]
        for j, t2 in enumerate(titles[i+1:], start=i+1):
            if j in seen: continue
            ratio = difflib.SequenceMatcher(None, t1.lower(), t2.lower()).ratio()
            if ratio >= 0.6:
                cluster.append(t2)
                seen.add(j)
        if len(cluster) > 1:
            title_clusters.append({"representative": t1, "count": len(cluster), "similar_titles": cluster[:5]})
        seen.add(i)

    title_clusters.sort(key=lambda x: x["count"], reverse=True)

    # Peak hours (top 5)
    peak_hours = [
        {"hour_utc": f"{h:02d}:00", "incident_count": c}
        for h, c in hour_counts.most_common(5)
    ]

    return {
        "window": {"since": args["since"], "until": args["until"]},
        "total_incidents_analysed": len(incidents),
        "top_noisy_services": [
            {"service": svc, "incident_count": cnt}
            for svc, cnt in service_counts.most_common(top_n)
        ],
        "urgency_distribution": dict(urgency_counts),
        "peak_hours_utc": peak_hours,
        "busiest_days": [{"day": d, "count": c} for d, c in day_counts.most_common(3)],
        "recurring_title_clusters": title_clusters[:top_n],
        "insight": (
            f"Top noise source: {service_counts.most_common(1)[0][0]} "
            f"({service_counts.most_common(1)[0][1]} incidents). "
            f"Peak hour: {hour_counts.most_common(1)[0][0]:02d}:00 UTC."
        ) if service_counts else "Insufficient data for pattern analysis.",
    }


# ── NEW 4: check_sla_breaches ──────────────────────────

def tool_check_sla_breaches(args: dict) -> dict:
    """Find incidents that breached MTTA or MTTR SLA targets."""
    mtta_threshold = args.get("mtta_threshold_seconds", SLA_MTTA_SEC)
    mttr_threshold = args.get("mttr_threshold_seconds", SLA_MTTR_SEC)

    analytics = tool_get_analytics_incidents({
        "since":       args["since"],
        "until":       args["until"],
        "service_ids": args.get("service_ids"),
        "team_ids":    args.get("team_ids"),
    })

    if "error" in analytics:
        return analytics

    breaches = []
    mtta_breach_count = 0
    mttr_breach_count = 0

    for inc in analytics.get("analytics_incidents", []):
        mtta_sec = inc.get("seconds_to_first_ack")
        mttr_sec = inc.get("seconds_to_resolve")
        mtta_breach = mtta_sec is not None and mtta_sec > mtta_threshold
        mttr_breach = mttr_sec is not None and mttr_sec > mttr_threshold

        if mtta_breach: mtta_breach_count += 1
        if mttr_breach: mttr_breach_count += 1

        if mtta_breach or mttr_breach:
            breaches.append({
                "incident_id":      inc["incident_id"],
                "incident_number":  inc.get("incident_number"),
                "title":            inc.get("title"),
                "service":          inc.get("service_name"),
                "urgency":          inc.get("urgency"),
                "created_at":       inc.get("created_at"),
                "mtta_seconds":     mtta_sec,
                "mtta_breach":      mtta_breach,
                "mtta_overage_sec": round(mtta_sec - mtta_threshold, 0) if mtta_breach else None,
                "mttr_seconds":     mttr_sec,
                "mttr_breach":      mttr_breach,
                "mttr_overage_sec": round(mttr_sec - mttr_threshold, 0) if mttr_breach else None,
            })

    total_analysed = len(analytics.get("analytics_incidents", []))
    return {
        "window":            {"since": args["since"], "until": args["until"]},
        "sla_thresholds":    {"mtta_seconds": mtta_threshold, "mttr_seconds": mttr_threshold},
        "total_analysed":    total_analysed,
        "total_breaches":    len(breaches),
        "mtta_breach_count": mtta_breach_count,
        "mttr_breach_count": mttr_breach_count,
        "breach_rate_pct":   round(len(breaches)/total_analysed*100, 1) if total_analysed else 0,
        "breaches":          sorted(breaches, key=lambda x: (x.get("mtta_overage_sec") or 0) + (x.get("mttr_overage_sec") or 0), reverse=True),
    }


# ── NEW 5: oncall_load_report ──────────────────────────

def tool_oncall_load_report(args: dict) -> dict:
    """
    Analyse who is being paged most. Uses the notifications endpoint to
    count pages per user, surface after-hours pages, and flag burnout risk.
    """
    notif_raw = tool_list_notifications({
        "since":   args["since"],
        "until":   args["until"],
        "include": ["users"],
    })
    if "error" in notif_raw:
        return notif_raw

    notifications = notif_raw.get("notifications", [])

    user_pages:          Counter = Counter()
    user_after_hours:    Counter = Counter()
    user_weekend:        Counter = Counter()
    user_response_times: defaultdict = defaultdict(list)

    for n in notifications:
        user = (n.get("user") or "Unknown")
        user_pages[user] += 1

        ts_str = n.get("started_at","")
        if ts_str:
            ts = _fmt_ts(ts_str)
            if ts:
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                # After hours: before 8am or after 10pm UTC
                if ts.hour < 8 or ts.hour >= 22:
                    user_after_hours[user] += 1
                # Weekend
                if ts.weekday() >= 5:
                    user_weekend[user] += 1

    # Build report
    BURNOUT_PAGE_THRESHOLD = 10   # > 10 pages/week is a warning
    window_days = 1
    try:
        s = _fmt_ts(args["since"])
        e = _fmt_ts(args["until"])
        if s and e:
            if s.tzinfo is None: s = s.replace(tzinfo=timezone.utc)
            if e.tzinfo is None: e = e.replace(tzinfo=timezone.utc)
            window_days = max(1, (e - s).days)
    except Exception:
        pass

    report = []
    for user, pages in user_pages.most_common():
        pages_per_week = round(pages / window_days * 7, 1)
        after_hours    = user_after_hours[user]
        weekends       = user_weekend[user]
        burnout_risk   = "🔴 HIGH" if pages_per_week > BURNOUT_PAGE_THRESHOLD else \
            "🟡 MEDIUM" if pages_per_week > BURNOUT_PAGE_THRESHOLD * 0.6 else "🟢 LOW"

        report.append({
            "user":                user,
            "total_pages":         pages,
            "pages_per_week":      pages_per_week,
            "after_hours_pages":   after_hours,
            "weekend_pages":       weekends,
            "after_hours_pct":     round(after_hours/pages*100, 1) if pages else 0,
            "burnout_risk":        burnout_risk,
        })

    return {
        "window":              {"since": args["since"], "until": args["until"], "days": window_days},
        "total_notifications": len(notifications),
        "users_paged":         len(report),
        "burnout_threshold":   f"{BURNOUT_PAGE_THRESHOLD} pages/week",
        "on_call_load":        report,
        "high_risk_users":     [r["user"] for r in report if "HIGH" in r["burnout_risk"]],
    }


# =====================================================
# DYNAMIC TOOL SELECTION
# =====================================================
# The full TOOLS list is ~25 K tokens — 2.5× the kimi-k2 TPM budget.
# Sending all 65 tools every request guarantees a 413 error.
# Instead we route each query to a relevant 5-15 tool subset,
# keeping the whole request comfortably under 5 K tokens.

TOOL_GROUPS: Dict[str, List[str]] = {
    "incident": [
        "list_incidents", "get_incident", "get_incident_timeline",
        "get_incident_notes", "get_incident_alerts",
        "manage_incident", "create_incident",
    ],
    "service": [
        "list_services", "get_service", "create_service",
        "update_service", "delete_service",
        "list_service_integrations", "create_service_integration",
    ],
    "oncall": [
        "list_oncalls", "list_schedules", "get_schedule",
        "list_schedule_overrides", "create_schedule_override",
        "delete_schedule_override", "list_schedule_users_on_call",
    ],
    "user": [
        "list_users", "get_user", "create_user", "update_user", "delete_user",
        "get_user_contact_methods", "get_user_notification_rules",
    ],
    "team": [
        "list_teams", "get_team", "create_team", "update_team", "delete_team",
        "manage_team_membership", "list_team_members",
    ],
    "escalation": [
        "list_escalation_policies", "get_escalation_policy",
        "create_escalation_policy", "update_escalation_policy",
        "delete_escalation_policy",
    ],
    "analytics": [
        "get_analytics_incidents", "get_analytics_services", "get_analytics_teams",
        "full_incident_analysis", "analyze_patterns",
        "check_sla_breaches", "oncall_load_report",
    ],
    "maintenance": [
        "list_maintenance_windows", "create_maintenance_window",
        "delete_maintenance_window",
    ],
    "notification": ["list_notifications", "list_log_entries"],
    "audit":        ["list_audit_records"],
    "postmortem":   ["generate_postmortem"],
    "config": [
        "list_tags", "manage_tags", "list_vendors", "list_response_plays",
        "list_business_services", "get_business_service",
        "get_service_dependencies", "list_status_dashboards",
        "list_event_orchestrations", "list_rulesets",
        "list_webhook_subscriptions", "create_webhook_subscription",
        "list_abilities", "list_extensions", "list_incident_workflows",
        "list_priorities",
    ],
    "utility": ["resolve_time"],
}

# keyword sets → groups to activate
_KEYWORD_RULES: List[tuple] = [
    ({"postmortem", "post-mortem", "post mortem"},
     ["postmortem", "incident", "utility"]),
    ({"incident", "incidents", "page", "alert", "alerts", "ack", "acknowledge",
      "resolve", "trigger", "triggered", "rca", "root cause", "explain incident",
      "explain this", "what happened", "q0", "q1", "q2", "q3"},
     ["incident", "utility"]),
    ({"who is on call", "on call", "oncall", "on-call", "schedule", "override",
      "shift", "rotation"},
     ["oncall", "utility"]),
    ({"service", "services", "integration", "integrations", "maintenance", "maint"},
     ["service", "maintenance", "utility"]),
    ({"user", "users", "contact", "notification rule"},
     ["user", "utility"]),
    ({"team", "teams", "member", "members", "membership"},
     ["team", "utility"]),
    ({"escalation", "policy", "policies"},
     ["escalation", "utility"]),
    ({"analytics", "mtta", "mttr", "report", "summary", "sla", "breach",
      "pattern", "patterns", "burnout", "load report", "analysis", "last", "days",
      "hours", "week", "month"},
     ["analytics", "incident", "utility"]),
    ({"notification", "paged", "log entry", "log entries"},
     ["notification", "utility"]),
    ({"audit", "audit log", "config change", "configuration change"},
     ["audit", "utility"]),
    ({"webhook", "extension", "vendor", "orchestration", "ruleset",
      "workflow", "ability", "feature", "tag", "business service", "dependency"},
     ["config", "utility"]),
]

_TOOL_BY_NAME: Dict[str, dict] = {
    t["function"]["name"]: t for t in TOOLS
}


def select_tools_for_query(query: str) -> List[dict]:
    """
    Return the minimal subset of TOOLS relevant to this query.
    Keeps the request well under the 10K TPM limit.
    Falls back to incident + analytics + utility for ambiguous queries.
    """
    q = query.lower()
    selected_groups: set = {"utility"}

    for keywords, groups in _KEYWORD_RULES:
        if any(kw in q for kw in keywords):
            selected_groups.update(groups)

    # Nothing specific matched → broad default
    if selected_groups == {"utility"}:
        selected_groups.update(["incident", "analytics", "utility"])

    tool_names: set = set()
    for group in selected_groups:
        tool_names.update(TOOL_GROUPS.get(group, []))

    selected = [_TOOL_BY_NAME[n] for n in tool_names if n in _TOOL_BY_NAME]
    cprint(f"  [dim]Tool routing: {', '.join(sorted(selected_groups))} → {len(selected)} tools[/dim]")
    return selected


# =====================================================
# TOOL DISPATCHER
# =====================================================

TOOL_DISPATCH = {
    "list_incidents":              tool_list_incidents,
    "get_incident":                tool_get_incident,
    "get_incident_timeline":       tool_get_incident_timeline,
    "get_incident_notes":          tool_get_incident_notes,
    "get_incident_alerts":         tool_get_incident_alerts,
    "manage_incident":             tool_manage_incident,
    "create_incident":             tool_create_incident,
    "list_services":               tool_list_services,
    "get_service":                 tool_get_service,
    "create_service":              tool_create_service,
    "update_service":              tool_update_service,
    "delete_service":              tool_delete_service,
    "list_service_integrations":   tool_list_service_integrations,
    "create_service_integration":  tool_create_service_integration,
    "list_users":                  tool_list_users,
    "get_user":                    tool_get_user,
    "create_user":                 tool_create_user,
    "update_user":                 tool_update_user,
    "delete_user":                 tool_delete_user,
    "get_user_contact_methods":    tool_get_user_contact_methods,
    "get_user_notification_rules": tool_get_user_notification_rules,
    "list_teams":                  tool_list_teams,
    "get_team":                    tool_get_team,
    "create_team":                 tool_create_team,
    "update_team":                 tool_update_team,
    "delete_team":                 tool_delete_team,
    "manage_team_membership":      tool_manage_team_membership,
    "list_team_members":           tool_list_team_members,
    "list_escalation_policies":    tool_list_escalation_policies,
    "get_escalation_policy":       tool_get_escalation_policy,
    "create_escalation_policy":    tool_create_escalation_policy,
    "update_escalation_policy":    tool_update_escalation_policy,
    "delete_escalation_policy":    tool_delete_escalation_policy,
    "list_schedules":              tool_list_schedules,
    "get_schedule":                tool_get_schedule,
    "list_schedule_overrides":     tool_list_schedule_overrides,
    "create_schedule_override":    tool_create_schedule_override,
    "delete_schedule_override":    tool_delete_schedule_override,
    "list_schedule_users_on_call": tool_list_schedule_users_on_call,
    "list_oncalls":                tool_list_oncalls,
    "list_priorities":             tool_list_priorities,
    "list_maintenance_windows":    tool_list_maintenance_windows,
    "create_maintenance_window":   tool_create_maintenance_window,
    "delete_maintenance_window":   tool_delete_maintenance_window,
    "list_log_entries":            tool_list_log_entries,
    "list_notifications":          tool_list_notifications,
    "get_analytics_incidents":     tool_get_analytics_incidents,
    "get_analytics_services":      tool_get_analytics_services,
    "get_analytics_teams":         tool_get_analytics_teams,
    "list_tags":                   tool_list_tags,
    "manage_tags":                 tool_manage_tags,
    "list_vendors":                tool_list_vendors,
    "list_response_plays":         tool_list_response_plays,
    "list_business_services":      tool_list_business_services,
    "get_business_service":        tool_get_business_service,
    "get_service_dependencies":    tool_get_service_dependencies,
    "list_status_dashboards":      tool_list_status_dashboards,
    "list_audit_records":          tool_list_audit_records,
    "list_event_orchestrations":   tool_list_event_orchestrations,
    "list_rulesets":               tool_list_rulesets,
    "list_webhook_subscriptions":  tool_list_webhook_subscriptions,
    "create_webhook_subscription": tool_create_webhook_subscription,
    "list_abilities":              tool_list_abilities,
    "list_extensions":             tool_list_extensions,
    "list_incident_workflows":     tool_list_incident_workflows,
    "full_incident_analysis":      tool_full_incident_analysis,
    # New tools
    "resolve_time":                tool_resolve_time,
    "generate_postmortem":         tool_generate_postmortem,
    "analyze_patterns":            tool_analyze_patterns,
    "check_sla_breaches":          tool_check_sla_breaches,
    "oncall_load_report":          tool_oncall_load_report,
}


def execute_tool(name: str, args: dict) -> dict:
    """Dispatch a tool call, with safe JSON-arg handling and dry-run support."""
    fn = TOOL_DISPATCH.get(name)
    if not fn:
        return {"error": f"Unknown tool: {name}"}
    try:
        return fn(args)
    except Exception as e:
        return {"error": f"Tool '{name}' execution failed: {e}"}


# =====================================================
# SYSTEM PROMPT
# =====================================================

SYSTEM_PROMPT = """You are a PagerDuty SRE Intelligence Assistant with full access to the PagerDuty REST API v2.

You can perform ANY operation available in PagerDuty through function calls:

READ:  incidents, services, users, teams, schedules, escalation policies, on-call schedules,
       notifications, analytics (MTTA/MTTR), business services, service dependencies,
       status dashboards, audit records, tags, vendors, webhooks, event orchestrations, rulesets,
       incident workflows, response plays, abilities/features.

WRITE: create/update/delete incidents, services, users, teams, escalation policies;
       acknowledge/resolve/reassign/snooze/merge incidents; add notes; create maintenance windows;
       create schedule overrides; manage team membership; manage tags; create webhooks.

ANALYSIS: full_incident_analysis, analyze_patterns, check_sla_breaches, oncall_load_report,
          generate_postmortem.

UTILITY: resolve_time — converts natural language like "yesterday", "last Monday 9am", "3 hours ago"
         into ISO-8601 UTC. Always use this when the user gives a relative time.

RULES:
1. Always use tool/function calls to get real data. Never fabricate PagerDuty data.
2. For ANY relative time expression ("last 24 hours", "yesterday", "last week"), first call
   resolve_time to get the correct ISO-8601 timestamp before using it in other tools.
3. For destructive operations (delete, bulk-resolve), confirm intent is clear before proceeding.
4. Present results clearly with context and key metrics. Use markdown for structure.
5. If multiple tool calls are needed for a complete answer, make all of them.
6. If a result contains "truncated: true", note this to the user and suggest filtering.
7. DRY_RUN={dry_run_status}. If true, destructive operations are simulated, not executed.
8. Current UTC time: {current_time}
"""


# =====================================================
# LLM CONVERSATION  (non-streaming tool rounds → streaming final answer)
# =====================================================

def _call_llm(
        messages:  list,
        use_tools: bool = True,
        stream:    bool = False,
        model:     str  = None,
        tools:     list = None,          # pass select_tools_for_query() result here
):
    """
    Call Groq with optional tool subset and optional streaming.
    Falls back to MODEL_FALLBACK on primary failure.

    Pass `tools` explicitly to avoid sending the full 25K-token TOOLS list.
    If omitted and use_tools=True the full list is used (not recommended for
    models with low TPM limits like kimi-k2).
    """
    model = model or MODEL_PRIMARY
    kwargs = dict(
        model=model,
        messages=messages,
        max_tokens=4096,
    )
    if use_tools:
        kwargs["tools"]       = tools if tools is not None else TOOLS
        kwargs["tool_choice"] = "auto"
    if stream:
        kwargs["stream"] = True

    try:
        return groq_client.chat.completions.create(**kwargs)
    except (APIStatusError, APIConnectionError) as e:
        if model == MODEL_PRIMARY:
            cprint(f"[yellow]⚠  Primary model failed ({e}), falling back to {MODEL_FALLBACK}…[/yellow]")
            kwargs["model"] = MODEL_FALLBACK
            return groq_client.chat.completions.create(**kwargs)
        raise


def _stream_final_response(messages: list) -> str:
    """Stream the final LLM answer character by character. Returns full text."""
    cprint("\n[bold blue]🤖 Assistant:[/bold blue]")
    response = _call_llm(messages, use_tools=False, stream=True)
    full_text = ""
    for chunk in response:
        delta = chunk.choices[0].delta
        if delta and delta.content:
            print(delta.content, end="", flush=True)
            full_text += delta.content
    print()  # newline at end
    return full_text


def _msg_to_dict(msg) -> dict:
    """
    Convert a Groq ChatCompletionMessage (Pydantic object) to a plain dict
    so it can be safely serialised to JSON and sent back to the API.
    Using the raw Pydantic object in json.dumps falls back to default=str,
    which turns the whole message into a plain string — breaking the next call.
    """
    d: dict = {"role": msg.role}
    # content can legitimately be None when the turn only contains tool_calls
    if msg.content is not None:
        d["content"] = msg.content
    if getattr(msg, "tool_calls", None):
        d["tool_calls"] = [
            {
                "id":   tc.id,
                "type": "function",
                "function": {
                    "name":      tc.function.name,
                    "arguments": tc.function.arguments,
                },
            }
            for tc in msg.tool_calls
        ]
    return d


def run_conversation(user_query: str, conversation_history: list) -> tuple[str, list]:
    """
    Run one turn of the conversation.
    - Tool-call rounds are non-streaming (we need to see the full tool call payload).
    - The final answer to the user is streamed token-by-token.
    Returns (answer_text, updated_history).
    """
    now = now_utc()
    system = SYSTEM_PROMPT.format(
        current_time=now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        dry_run_status="ENABLED" if DRY_RUN else "disabled",
    )

    messages = [{"role": "system", "content": system}] + conversation_history
    messages.append({"role": "user", "content": user_query})

    # Select only the tools relevant to this query so we stay well under
    # the kimi-k2 10K TPM limit.  Typically reduces from 65 → 5-15 tools.
    active_tools = select_tools_for_query(user_query)

    rounds = 0
    while rounds < 10:
        rounds += 1

        response = _call_llm(messages, use_tools=True, stream=False, tools=active_tools)
        msg = response.choices[0].message

        if not msg.tool_calls:
            # No more tool calls — stream the final answer
            messages.append({"role": "assistant", "content": msg.content or ""})
            # Stream it
            cprint("\n[bold blue]🤖 Assistant:[/bold blue]")
            if msg.content:
                if RICH_AVAILABLE:
                    console.print(Markdown(msg.content))
                else:
                    print(msg.content)
            answer = msg.content or ""
            break

        # ── Process tool calls ──────────────────────────────
        # FIX: convert Pydantic object → plain dict before appending.
        # Appending the raw Pydantic msg causes json.dumps to serialise it
        # as a string via default=str, breaking history reload next session.
        messages.append(_msg_to_dict(msg))

        for tc in msg.tool_calls:
            fn_name = tc.function.name

            # Safe JSON parse
            try:
                fn_args = json.loads(tc.function.arguments)
            except (json.JSONDecodeError, TypeError) as e:
                cprint(f"[red]⚠  Could not parse args for {fn_name}: {e}[/red]")
                fn_args = {}

            preview = json.dumps(fn_args, default=str)[:120]
            cprint(f"  [cyan]⚙  {fn_name}[/cyan]([dim]{preview}…[/dim])")

            result = execute_tool(fn_name, fn_args)
            result_str = json.dumps(result, indent=2, default=str)

            # Truncate very large tool results to avoid context overflow
            if len(result_str) > 28000:
                result_str = result_str[:28000] + "\n…[result truncated to fit context]"

            messages.append({
                "role":         "tool",
                "tool_call_id": tc.id,
                "content":      result_str,
            })
    else:
        answer = "I reached the maximum number of tool-call rounds without a final answer. Please try a more specific question."

    # Return only user + final-assistant turns (no tool intermediates).
    # Storing raw tool results and tool-call assistant turns in history
    # costs thousands of tokens on every subsequent request and quickly
    # blows the 10K TPM limit on kimi-k2.  _sanitize_history() applies
    # the same filter on reload, so the two are consistent.
    updated_history = _sanitize_history(messages[1:])
    return answer, updated_history


# =====================================================
# PROACTIVE MONITORING DAEMON
# =====================================================

_monitoring_active = threading.Event()
_monitoring_active.set()   # will be cleared on shutdown

def _monitoring_daemon():
    """
    Background thread: polls PagerDuty every poll_interval seconds for new
    high-urgency triggered incidents and prints inline alerts.
    """
    poll_interval = CONFIG["monitoring"]["poll_interval_seconds"]
    urgency_filter = CONFIG["monitoring"]["urgency_filter"]
    seen_ids: set = set()

    cprint(f"\n[bold yellow]🔔 Monitoring daemon started (polling every {poll_interval}s for {urgency_filter}-urgency triggered incidents)[/bold yellow]")

    while _monitoring_active.is_set():
        try:
            window_start = iso_hours_ago(poll_interval / 3600 * 2)  # look back 2× the poll window
            window_end   = iso_now()

            params = {
                "since":       window_start,
                "until":       window_end,
                "statuses[]":  ["triggered"],
                "urgencies[]": [urgency_filter],
                "sort_by":     "created_at:desc",
            }
            raw = []
            try:
                for item in pd_client.iter_all("incidents", params=params):
                    raw.append(item)
                    if len(raw) >= 20: break
            except Exception:
                pass

            for inc in raw:
                if inc["id"] not in seen_ids:
                    seen_ids.add(inc["id"])
                    svc  = inc.get("service",{}).get("summary","?")
                    ts   = inc.get("created_at","?")
                    url  = inc.get("html_url","")
                    cprint(
                        f"\n[bold red]🚨 NEW INCIDENT [{urgency_filter.upper()}] {inc['id']}[/bold red] "
                        f"│ [white]{inc['title']}[/white] "
                        f"│ service=[cyan]{svc}[/cyan] "
                        f"│ [dim]{ts}[/dim] "
                        f"│ {url}"
                    )

            # Keep seen_ids from growing unbounded
            if len(seen_ids) > 500:
                seen_ids = set(list(seen_ids)[-200:])

        except Exception as e:
            cprint(f"[dim]Monitor error: {e}[/dim]")

        # Sleep in small chunks so shutdown is responsive
        for _ in range(int(poll_interval)):
            if not _monitoring_active.is_set():
                break
            time.sleep(1)

    cprint("[dim]Monitoring daemon stopped.[/dim]")


def start_monitoring():
    t = threading.Thread(target=_monitoring_daemon, daemon=True, name="pd-monitor")
    t.start()
    return t


# =====================================================
# HELP TEXT
# =====================================================

HELP_TEXT = """
╔══════════════════════════════════════════════════════════════════════════╗
║          PagerDuty SRE AI Assistant — Enhanced Edition                  ║
╠══════════════════════════════════════════════════════════════════════════╣
║  INCIDENTS                                                               ║
║    "show incidents from last 24 hours"                                   ║
║    "show high-urgency triggered incidents since yesterday"               ║
║    "details of incident P1ABC23"                                         ║
║    "acknowledge / resolve incident P1ABC23"                              ║
║    "add note to P1ABC23: investigating DB connection pool"               ║
║    "snooze P1ABC23 for 2 hours"                                          ║
║    "merge incidents P1ABC, P2DEF into P3GHI"                             ║
║    "timeline of incident P1ABC23"                                        ║
║    "generate postmortem for P1ABC23"                                     ║
║                                                                          ║
║  ANALYSIS  (new)                                                         ║
║    "analyze patterns for last 7 days"                                    ║
║    "check SLA breaches this week"                                        ║
║    "on-call burnout report for last month"                               ║
║    "operational summary for last 24 hours"                               ║
║    "MTTA/MTTR report this week"                                          ║
║    "which service had most incidents this month?"                        ║
║                                                                          ║
║  ON-CALL & SCHEDULES                                                     ║
║    "who is on call right now?"                                           ║
║    "show all schedules"                                                  ║
║    "create override: user PXYZ on schedule PABC tomorrow"                ║
║                                                                          ║
║  SERVICES                                                                ║
║    "list all services"                                                   ║
║    "put service PABC in maintenance for 2 hours"                         ║
║    "show service PABC with integrations"                                 ║
║                                                                          ║
║  USERS & TEAMS                                                           ║
║    "list all users" / "find user john@example.com"                       ║
║    "add user PXYZ to team PABC as responder"                             ║
║    "show team members of team PABC"                                      ║
║                                                                          ║
║  ESCALATION POLICIES                                                     ║
║    "list escalation policies"                                            ║
║    "show escalation policy PABC details"                                 ║
║                                                                          ║
║  AUDIT & NOTIFICATIONS                                                   ║
║    "who was paged in the last 6 hours?"                                  ║
║    "audit log for today"                                                 ║
║    "what config changes were made this week?"                            ║
║                                                                          ║
║  OTHER                                                                   ║
║    "list priorities" / "show business services"                          ║
║    "service dependencies for PABC"                                       ║
║    "list vendors" / "find Datadog vendor"                                ║
║    "list webhook subscriptions"                                          ║
║    "what features are enabled on this account?"                          ║
║                                                                          ║
║  COMMANDS                                                                ║
║    help        — show this help                                          ║
║    clear       — reset conversation history                              ║
║    status      — show config, dry-run, and cache status                  ║
║    cache clear — clear the TTL cache                                     ║
║    exit/quit   — exit                                                    ║
╚══════════════════════════════════════════════════════════════════════════╝
"""


# =====================================================
# MAIN CHAT LOOP
# =====================================================

def print_status():
    dry = "[bold red]ENABLED[/bold red]" if DRY_RUN else "[green]disabled[/green]"
    mon = "[green]active[/green]" if ARGS.monitor else "[dim]off[/dim]"
    cached = len(_cache_store)
    cprint(f"[bold]Config:[/bold] {ARGS.config} | [bold]Model:[/bold] {MODEL_PRIMARY} | "
           f"[bold]Dry-run:[/bold] {dry} | [bold]Monitor:[/bold] {mon} | "
           f"[bold]Cached entries:[/bold] {cached} | "
           f"[bold]History:[/bold] {'disabled' if ARGS.no_persist else str(_history_path())}")


def main():
    cprint("\n[bold blue]🚀 PagerDuty Full-Feature SRE AI Assistant — Enhanced Edition[/bold blue]")
    print_rule()
    cprint(f"  Model   : [cyan]{MODEL_PRIMARY}[/cyan] (fallback: {MODEL_FALLBACK})")
    cprint(f"  Dry-run : {'[bold red]ENABLED[/bold red]' if DRY_RUN else '[green]disabled[/green]'}")
    cprint(f"  Monitor : {'[bold yellow]active[/bold yellow]' if ARGS.monitor else '[dim]off[/dim]'}")
    cprint(f"  Config  : [dim]{ARGS.config}[/dim]")
    cprint(f"  History : [dim]{'disabled' if ARGS.no_persist else str(_history_path())}[/dim]")
    cprint("  Type [bold]help[/bold] for capabilities, [bold]exit[/bold] to quit\n")

    # Load persisted history
    conversation_history = load_history()

    # Start monitoring daemon if requested
    if ARGS.monitor:
        start_monitoring()

    # Graceful shutdown handler
    def _shutdown(sig, frame):
        cprint("\n[yellow]Shutting down…[/yellow]")
        _monitoring_active.clear()
        save_history(conversation_history)
        sys.exit(0)

    signal.signal(signal.SIGINT,  _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    while True:
        try:
            if RICH_AVAILABLE:
                query = console.input("\n[bold green]You:[/bold green] ").strip()
            else:
                query = input("\nYou: ").strip()
        except (EOFError, KeyboardInterrupt):
            _shutdown(None, None)
            break

        if not query:
            continue

        q_lower = query.lower()

        if q_lower in ("exit","quit","q"):
            cprint("[bold]Goodbye! 👋[/bold]")
            _monitoring_active.clear()
            save_history(conversation_history)
            break

        if q_lower in ("help","?","h"):
            cprint(HELP_TEXT)
            continue

        if q_lower == "clear":
            conversation_history = []
            save_history([])
            cprint("[green]Conversation history cleared.[/green]")
            continue

        if q_lower == "status":
            print_status()
            continue

        if q_lower == "cache clear":
            cache_clear()
            cprint("[green]Cache cleared.[/green]")
            continue

        # ── Run the conversation turn ──
        try:
            print_rule()
            answer, conversation_history = run_conversation(query, conversation_history)
            print_rule()

            # Compress context if needed
            conversation_history = compress_history(conversation_history)

            # Persist after each turn
            save_history(conversation_history)

        except KeyboardInterrupt:
            cprint("\n[yellow]Interrupted.[/yellow]")
        except Exception as e:
            cprint(f"\n[bold red]❌ Error:[/bold red] {e}")
            import traceback
            cprint(f"[dim]{traceback.format_exc()}[/dim]")


# =====================================================
# ENTRY POINT
# =====================================================

if __name__ == "__main__":
    main()
