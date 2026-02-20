# PagerDuty SRE AI Assistant — Feature Audit & Professional Refactor Guide

---

## 1. Current Feature Inventory

### READ Operations (26 tools)

| Category | Operations |
|---|---|
| **Incidents** | List (with filters), Get details, Timeline/log entries, Notes, Alerts |
| **Services** | List, Get, Integrations list |
| **Users** | List, Get, Contact methods, Notification rules |
| **Teams** | List, Get, Members list |
| **Escalation Policies** | List, Get |
| **Schedules** | List, Get, Overrides list, Users on-call |
| **On-Calls** | List current on-call entries |
| **Analytics** | Raw incident analytics, Per-service, Per-team |
| **Notifications** | List sent notifications |
| **Log Entries** | Account-wide audit trail |
| **Audit Records** | Configuration change audit (cursor-based) |
| **Priorities** | List priority levels |
| **Maintenance Windows** | List |
| **Tags** | List |
| **Vendors** | List |
| **Response Plays** | List |
| **Business Services** | List, Get |
| **Service Dependencies** | Get upstream/downstream |
| **Status Dashboards** | List |
| **Event Orchestrations** | List |
| **Rulesets** | List (legacy) |
| **Webhook Subscriptions** | List |
| **Extensions** | List |
| **Incident Workflows** | List |
| **Abilities** | List enabled features |

### WRITE Operations (22 tools)

| Category | Operations |
|---|---|
| **Incidents** | Create, Acknowledge, Resolve, Reassign, Change urgency, Snooze, Merge, Add note |
| **Services** | Create, Update, Delete |
| **Service Integrations** | Create |
| **Users** | Create, Update, Delete |
| **Teams** | Create, Update, Delete, Add/remove members |
| **Escalation Policies** | Create, Update, Delete |
| **Schedule Overrides** | Create, Delete |
| **Maintenance Windows** | Create, Delete |
| **Tags** | Add/Remove on resources |
| **Webhook Subscriptions** | Create |

### Analysis / Compound Tools (5 tools)

| Tool | What It Does |
|---|---|
| `full_incident_analysis` | Fetches incidents + log entries to compute MTTA, MTTR, escalation counts, service distribution |
| `analyze_patterns` | Noisy services, recurring title clusters (via SequenceMatcher), hour-of-day/day-of-week clustering |
| `check_sla_breaches` | Cross-references analytics data against configurable MTTA/MTTR SLA thresholds |
| `oncall_load_report` | Pages-per-user, after-hours pages, weekend pages, burnout risk scoring |
| `generate_postmortem` | Gathers incident data → calls LLM → writes structured markdown postmortem to file |

### Infrastructure Features

| Feature | Implementation |
|---|---|
| Streaming responses | Final answer streamed token-by-token; tool rounds are non-streaming |
| Retry with backoff | Decorator with exponential backoff on 429, connection errors |
| TTL cache | In-memory dict cache for services/users with configurable TTL |
| NL time parsing | `dateparser` library for "yesterday", "3 hours ago", etc. |
| Dry-run mode | CLI flag / env var / config — destructive ops print instead of executing |
| YAML config | Auto-creates `config.yml` with defaults; deep-merge with user overrides |
| Conversation persistence | JSON file, loads/saves each turn |
| Context compression | Summarises old conversation half via LLM when history gets long |
| Model fallback | Primary → fallback on transient failures |
| Dynamic tool selection | Keyword-based routing reduces 65 tools → 5-15 per request (TPM optimization) |
| Monitoring daemon | Background thread polls for new triggered incidents |
| Rich CLI output | Tables, panels, markdown rendering via `rich` library |

---

## 2. What's MISSING (per PD API v2 Docs)

### High-Value Missing Endpoints

| Endpoint / Feature | Impact | Difficulty |
|---|---|---|
| **Events API v2** (trigger/acknowledge/resolve/change events) | Can't send raw events or change events — only REST incidents | Medium |
| **Incident Custom Fields** (CRUD) | Can't read or set custom field values on incidents | Easy |
| **Service Custom Fields** (CRUD) | Can't manage custom field definitions or values on services | Easy |
| **Incident Types** (list, get, create) | Can't work with incident type categorization | Easy |
| **Automation Actions** (list, get, invoke) | Can't trigger runbook automations | Medium |
| **Status Updates** (create, list on incident) | Can't post subscriber status updates on incidents | Easy |
| **Status Pages** (list, get, create) | Can't manage external-facing status pages | Medium |
| **Notification Subscriptions** (subscribe/unsubscribe users to incidents) | Can't manage who gets notified | Easy |
| **Schedule CRUD** (create/update/delete schedules) | Can only read schedules and manage overrides — can't create new ones | Medium |
| **Paused Incident Reports** | Can't list/analyze auto-paused incidents | Easy |
| **Templates** (list, get, create) | Can't manage incident/status update templates | Easy |
| **Licenses** (list, allocate) | Can't manage license allocation | Easy |
| **Extension Schemas** (list) | Can't browse available extension types | Easy |
| **Standards** (list, check compliance) | Can't check service standards compliance | Easy |
| **Jira Cloud Integration API** (mappings, rules) | Can't manage Jira↔PD mappings | Medium |
| **Workflow Integrations & Connections** | Can't manage workflow integration configs | Medium |
| **OAuth Delegations / Session Configurations** | Can't manage OAuth or SSO config | Hard |
| **Alert Grouping Settings** (per-service) | Can't configure intelligent alert grouping | Medium |
| **Change Events** (send via Events API v2) | Can't track deployments/changes | Easy |

### Missing WRITE Operations on Existing Resources

| Resource | Missing Write Ops |
|---|---|
| **Schedules** | Create, Update, Delete (only overrides are writable today) |
| **Extensions** | Create, Update, Delete |
| **Service Integrations** | Update, Delete |
| **Webhook Subscriptions** | Update, Delete |
| **Business Services** | Create, Update, Delete |
| **Response Plays** | Run a response play on an incident |
| **Maintenance Windows** | Update (can only create/delete) |
| **Incident Workflows** | Create, Update, Delete, Run |
| **Event Orchestrations** | Create, Update, Delete, Get rules |

### Missing Analysis Capabilities

| Feature | Description |
|---|---|
| **Trend comparison** | Compare current week vs previous week (incidents, MTTA, MTTR) |
| **Service health score** | Composite score based on incidents, SLA, noise, escalations |
| **Flapping detection** | Find incidents that trigger → resolve → trigger repeatedly |
| **Correlation analysis** | Find incidents that frequently co-occur across services |
| **Capacity planning** | Predict on-call load based on historical patterns |
| **Noise reduction recommendations** | Suggest alert tuning based on pattern analysis |
| **Custom dashboards/reports** | Export analysis as HTML/PDF reports |

---

## 3. Professional Multi-File Refactor

The current ~2,800-line single file is hard to navigate, test, or maintain. Here's the recommended project structure:

```
pagerduty-sre-assistant/
├── pyproject.toml                  # Project metadata, dependencies, entry points
├── README.md
├── config.yml                      # Default config (auto-generated)
├── .env.example                    # Template for API keys
│
├── src/
│   └── pd_assistant/
│       ├── __init__.py             # Package version, exports
│       ├── __main__.py             # Entry point: `python -m pd_assistant`
│       │
│       ├── cli/
│       │   ├── __init__.py
│       │   ├── app.py              # Main chat loop, signal handlers
│       │   ├── args.py             # Argument parsing (parse_args)
│       │   └── output.py           # cprint, print_rule, render_markdown, HELP_TEXT
│       │
│       ├── config/
│       │   ├── __init__.py
│       │   ├── loader.py           # YAML config loading, deep merge, defaults
│       │   └── defaults.py         # DEFAULT_CONFIG dict
│       │
│       ├── clients/
│       │   ├── __init__.py
│       │   ├── groq_client.py      # Groq client init, _call_llm, streaming
│       │   └── pd_client.py        # PagerDuty client init, _safe_list, _unwrap
│       │
│       ├── tools/
│       │   ├── __init__.py         # TOOL_DISPATCH registry, execute_tool()
│       │   ├── schemas.py          # TOOLS list (all JSON schemas for function calling)
│       │   ├── selector.py         # TOOL_GROUPS, _KEYWORD_RULES, select_tools_for_query()
│       │   │
│       │   ├── incidents.py        # tool_list_incidents, tool_get_incident, etc.
│       │   ├── services.py         # tool_list_services, tool_create_service, etc.
│       │   ├── users.py            # tool_list_users, tool_create_user, etc.
│       │   ├── teams.py            # tool_list_teams, tool_manage_team_membership, etc.
│       │   ├── escalation.py       # tool_list_escalation_policies, etc.
│       │   ├── schedules.py        # tool_list_schedules, tool_create_schedule_override, etc.
│       │   ├── oncalls.py          # tool_list_oncalls
│       │   ├── analytics.py        # tool_get_analytics_*, tool_full_incident_analysis
│       │   ├── maintenance.py      # tool_list/create/delete_maintenance_window
│       │   ├── notifications.py    # tool_list_notifications, tool_list_log_entries
│       │   ├── audit.py            # tool_list_audit_records
│       │   ├── config_tools.py     # tags, vendors, response_plays, business_services, etc.
│       │   ├── webhooks.py         # tool_list/create_webhook_subscriptions
│       │   ├── events.py           # NEW: Events API v2 (trigger, ack, resolve, change)
│       │   ├── custom_fields.py    # NEW: Incident & service custom fields
│       │   └── automation.py       # NEW: Automation actions
│       │
│       ├── analysis/
│       │   ├── __init__.py
│       │   ├── patterns.py         # tool_analyze_patterns
│       │   ├── sla.py              # tool_check_sla_breaches
│       │   ├── burnout.py          # tool_oncall_load_report
│       │   ├── postmortem.py       # tool_generate_postmortem
│       │   └── trends.py           # NEW: week-over-week comparison, health scores
│       │
│       ├── conversation/
│       │   ├── __init__.py
│       │   ├── engine.py           # run_conversation() — the tool-calling loop
│       │   ├── history.py          # load_history, save_history, _sanitize_history
│       │   ├── compression.py      # compress_history
│       │   └── prompts.py          # SYSTEM_PROMPT template
│       │
│       ├── infra/
│       │   ├── __init__.py
│       │   ├── cache.py            # cache_get, cache_set, cache_clear
│       │   ├── retry.py            # with_retry decorator
│       │   ├── time_utils.py       # parse_nl_time, now_utc, iso_now, _fmt_ts, _diff_minutes
│       │   └── monitoring.py       # _monitoring_daemon, start_monitoring
│       │
│       └── models/
│           ├── __init__.py
│           └── types.py            # Pydantic/dataclass models for tool args & responses
│
├── tests/
│   ├── __init__.py
│   ├── conftest.py                 # Shared fixtures (mock PD client, mock Groq)
│   ├── test_tools/
│   │   ├── test_incidents.py
│   │   ├── test_services.py
│   │   ├── test_analytics.py
│   │   └── ...
│   ├── test_analysis/
│   │   ├── test_patterns.py
│   │   ├── test_sla.py
│   │   └── ...
│   ├── test_conversation/
│   │   ├── test_engine.py
│   │   ├── test_history.py
│   │   └── ...
│   └── test_infra/
│       ├── test_cache.py
│       ├── test_retry.py
│       └── test_time_utils.py
│
├── scripts/
│   ├── export_report.py            # Generate HTML/PDF reports
│   └── bulk_operations.py          # Batch acknowledge/resolve scripts
│
└── docs/
    ├── architecture.md             # System design, data flow diagrams
    ├── adding_tools.md             # Guide for adding new PD API tools
    └── deployment.md               # Docker, systemd, cloud deployment
```

### Key Refactoring Principles

**1. Dependency Injection over Globals**

```python
# BAD (current): globals everywhere
groq_client = Client(api_key=GROQ_API_KEY)
pd_client   = pagerduty.RestApiV2Client(...)

# GOOD: pass clients into classes/functions
class PDAssistant:
    def __init__(self, pd_client, llm_client, config):
        self.pd = pd_client
        self.llm = llm_client
        self.config = config
```

**2. Tool Registration via Decorators**

```python
# In tools/incidents.py
from pd_assistant.tools import register_tool

@register_tool(
    name="list_incidents",
    group="incident",
    schema={...}  # JSON schema
)
def list_incidents(pd_client, args: dict) -> dict:
    ...
```

```python
# In tools/__init__.py
_REGISTRY: dict[str, ToolEntry] = {}

def register_tool(name, group, schema):
    def decorator(fn):
        _REGISTRY[name] = ToolEntry(fn=fn, group=group, schema=schema)
        return fn
    return decorator
```

**3. Pydantic Models for Type Safety**

```python
# In models/types.py
from pydantic import BaseModel
from typing import Optional

class ListIncidentsArgs(BaseModel):
    since: str
    until: str
    statuses: list[str] = ["triggered", "acknowledged", "resolved"]
    urgencies: list[str] | None = None
    service_ids: list[str] | None = None
    limit: int = 25

class IncidentSummary(BaseModel):
    id: str
    title: str
    status: str
    urgency: str | None
    service: str | None
    created_at: str
```

**4. Abstract Client Interface (for testing)**

```python
# In clients/pd_client.py
from abc import ABC, abstractmethod

class PDClientBase(ABC):
    @abstractmethod
    def list_resources(self, endpoint, params, limit) -> list[dict]: ...
    @abstractmethod
    def get_resource(self, endpoint) -> dict: ...
    @abstractmethod
    def create_resource(self, endpoint, body) -> dict: ...

class PDClient(PDClientBase):
    """Real PagerDuty client."""
    ...

class MockPDClient(PDClientBase):
    """For tests — returns canned responses."""
    ...
```

**5. pyproject.toml**

```toml
[project]
name = "pd-sre-assistant"
version = "2.0.0"
description = "AI-powered PagerDuty SRE Assistant"
requires-python = ">=3.11"
dependencies = [
    "groq>=0.4",
    "python-pagerduty>=6.0",
    "python-dotenv>=1.0",
    "pyyaml>=6.0",
    "dateparser>=1.2",
    "rich>=13.0",
]

[project.optional-dependencies]
dev = ["pytest>=8.0", "pytest-asyncio", "ruff", "mypy"]

[project.scripts]
pd-assistant = "pd_assistant.cli.app:main"
```

---

## 4. Priority Additions (Ranked)

### Tier 1 — High impact, easy to add
1. **Events API v2** — trigger/resolve/ack events via routing key
2. **Incident Custom Fields** — read/set custom fields on incidents
3. **Status Updates** — post status updates to subscribers
4. **Schedule CRUD** — create/update/delete schedules (not just overrides)
5. **Trend comparison** — week-over-week incident/MTTA/MTTR deltas
6. **Run Response Play** — execute response plays on incidents

### Tier 2 — Medium impact
7. **Automation Actions** — list and invoke runbook automations
8. **Incident Types** — categorize incidents
9. **Business Service CRUD** — full write support
10. **Alert Grouping Settings** — configure intelligent grouping
11. **Flapping detection** — find trigger-resolve-trigger loops
12. **Service health scores** — composite reliability metrics

### Tier 3 — Nice to have
13. **Status Pages** — manage external status pages
14. **Templates** — manage incident/notification templates
15. **Licenses** — view/manage license allocation
16. **Standards compliance** — check services against org standards
17. **Jira Integration API** — manage PD↔Jira mappings
18. **HTML/PDF report export** — render analyses as shareable docs
