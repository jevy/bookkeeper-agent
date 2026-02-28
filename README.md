# Bookkeeper Agent

Automated transaction categorization for bookkeeping spreadsheets. Reads uncategorized transactions from your Google Sheet, uses an AI agent to determine the correct category, and writes the result back. Supports [Tiller](https://www.tillerhq.com/) spreadsheets out of the box.

## Architecture

Three services packaged as a single Docker image with different entrypoints, connected via Kafka (Redpanda):

```
Google Sheets
     │ read                    write category
     ▼                              ▲
 ┌──────────┐                ┌──────┴───────┐
 │ Producer │                │    Writer    │
 └────┬─────┘                └──────▲───────┘
      │ publish                     │ consume
      ▼                             │
 ┌──────────────────────────────────┴──┐
 │             Redpanda                │
 │  transactions.uncategorized         │
 │  transactions.categorized           │
 └──────────────┬──────────────────────┘
        consume │ ▲ publish
                ▼ │
       ┌──────────┴──────┐
       │   Categorizer   │
       │   (AI Agent)    │
       └─────────────────┘
```

**Producer** — Polls the Google Sheet for uncategorized transactions and publishes them to Kafka.

**Categorizer** — AI agent that consumes uncategorized transactions, researches the correct category using tool calls (sheet history lookup, AutoCat rules, web search), and publishes the result.

**Writer** — Consumes categorized transactions and writes the category back to the sheet.

## Prerequisites

- A [Tiller](https://www.tillerhq.com/) spreadsheet with the standard Transactions tab
- A Google Cloud service account with access to the spreadsheet
- A Kafka-compatible broker (tested with [Redpanda](https://redpanda.com/))
- An [OpenRouter API key](https://openrouter.ai/keys)
- (Optional) A [Brave Search API key](https://brave.com/search/api/) for web search

## Configuration

All configuration is via environment variables:

| Variable | Required By | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | All | Kafka/Redpanda broker address |
| `SCHEMA_REGISTRY_URL` | All | Schema registry URL |
| `GOOGLE_SHEET_ID` | All | Spreadsheet ID from the Google Sheets URL |
| `GOOGLE_CREDENTIALS_JSON` | All | Path to service account JSON key file |
| `OPENROUTER_API_KEY` | Categorizer | OpenRouter API key |
| `MODEL` | Categorizer | Model to use (default: `anthropic/claude-sonnet-4-6`) |
| `BRAVE_API_KEY` | Categorizer | Brave Search API key (optional) |
| `ADDITIONAL_CONTEXT_PROMPT` | Categorizer | Extra context about the user for the agent (optional) |
| `METRICS_PORT` | Categorizer, Writer | Prometheus metrics HTTP port (default: `9091`) |
| `PUSHGATEWAY_URL` | Producer | Pushgateway URL for one-shot metrics (optional) |
| `MAX_TRANSACTION_AGE_DAYS` | Producer | Skip transactions older than N days (default: 365) |
| `MAX_TRANSACTIONS` | Producer | Limit transactions per run, 0 = unlimited (default: 0) |

## Running Locally

Start the infrastructure and all three services with Docker Compose:

```bash
cp .env.example .env  # fill in your API keys and sheet ID

docker compose up
```

Or run individual services:

```bash
docker compose up producer
docker compose up categorizer
docker compose up writer
```

## Deployment

Container images are published to GitHub Container Registry on tagged releases:

```bash
docker pull ghcr.io/jevy/bookkeeper-agent:0.7.0
```

Kubernetes manifests are provided in `k8s/` using Kustomize:

```bash
kubectl apply -k k8s/
```

You'll need to create a `bookkeeper` Secret in your namespace with the required API keys and credentials.

## Building

Requires JDK 21.

```bash
./gradlew build          # compile + test
./gradlew installDist    # build distributable
docker build -t bookkeeper-agent .
```

## Releasing

Tag a version and push to trigger a release build:

```bash
git tag v0.7.0
git push --tags
```

This produces image tags: `0.7.0`, `0.7`, and `latest`.

## License

MIT
