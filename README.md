# Hermes Analytics

<sup>Pre-release (v0.1.0) — not yet publicly released. API may change.</sup>

SQLite analytics plugin for [Hermes Agent](https://github.com/NousResearch/hermes-agent). Captures LLM calls, tool usage, context pressure, session summaries, kanban events, approval events, API request metrics, inbound message volume, and verification quality per profile.

## Features

- **Zero external dependencies** — uses stdlib `sqlite3`, only requires `pyyaml` for pricing config
- **Cost tracking** — three-tier pricing resolution (core pricing → OpenRouter API → HF Router API) with provider cost passthrough and user-configurable overrides
- **Agent-callable tools** — 5 tools that let agents query their own telemetry: fleet report, per-profile digest, targeted queries, pricing config, CSV/JSON export
- **Fleet reporting** — cross-profile aggregation across all agent profiles
- **Context pressure** — track context window utilization and compression events
- **Verification quality** — track file edits and verification attempts
- **Inbound message analytics** — platform, chat ID, message volume
- **Configurable retention** — automatic purging of old records
- **Standalone mode** — runs outside Hermes with env var overrides

## Installation

```bash
hermes plugins install dyiapanis/hermes-analytics --enable
```

Or add to your profile's `config.yaml`:

```yaml
plugins:
  enabled:
    - hermes-analytics
```

Then restart your gateway. The database (`analytics.db`) is created automatically on first LLM call. No configuration required.

## Configuration

### Retention

```yaml
# In plugin.yaml (default: 365 days)
config:
  retention_days: 365
```

Or via environment variable:

```bash
export ANALYTICS_RETENTION_DAYS=90
```

### Pricing

Edit `pricing_config.yaml` to set custom rates or reorder pricing sources:

```yaml
source_priority:
  - override      # user-supplied rates (highest priority)
  - core          # agent.usage_pricing
  - openrouter    # OpenRouter API (cache-aware)
  - hf_router     # HF Router API (fallback)

overrides:
  my-model:
    input: 0.50
    output: 2.00
    cache_read: 0.10
```

### Standalone mode

```bash
ANALYTICS_DB_PATH=/path/to/analytics.db
ANALYTICS_PROFILES=profile1,profile2
ANALYTICS_RETENTION_DAYS=90
ANALYTICS_CONTEXT_LENGTH=128000
```

## Tools

| Tool | Description |
|------|-------------|
| `analytics_fleet_report` 📊 | Fleet-wide summary across all profiles |
| `analytics_digest` 📋 | Per-profile deep dive (models, tools, context, trends) |
| `analytics_query` 🔍 | Targeted single-query access (10 query types) |
| `analytics_pricing_config` 💰 | Read/write pricing configuration |
| `analytics_export` 📤 | Export raw rows to CSV or JSON |

## Database

SQLite in WAL mode. One file per profile at `~/.hermes/profiles/<profile>/analytics.db`.

Timestamps stored as integer unixepoch seconds. Schema versioned via `_schema_meta` table.

## License

MIT
