# Tiller Categorizer Agent

Automated transaction categorization for Tiller (Yodlee -> Google Sheets) bookkeeping.

## Overview

Three services packaged as a single Docker image with different entrypoints. Polls a Google Sheet for uncategorized transactions, uses an AI agent to determine the correct category, and writes the result back.

## Architecture

```
┌─────────────────────────┐
│     Google Sheets       │
│   (Transactions tab)    │
└────┬───────────────▲────┘
     │ read           │ write category
     │                │
┌────▼──────┐   ┌────┴──────────────┐
│ Service 1 │   │    Service 3      │
│ Producer  │   │  Category Writer  │
└────┬──────┘   └────▲──────────────┘
     │               │
     │ publish       │ consume
     │               │
┌────▼───────────────┴─────────────────┐
│                Redpanda              │
│                                      │
│  transactions.uncategorized          │
│  (compacted, keyed by Transaction ID)│
│                                      │
│  transactions.categorized            │
│  (compacted, keyed by Transaction ID)│
└────┬───────────────▲─────────────────┘
     │               │
     │ consume       │ publish
     │               │
┌────▼───────────────┴──┐
│      Service 2        │
│   Categorizer Agent   │
│  (Sonnet 4.5 via      │
│   Anthropic SDK)      │
└───────────────────────┘
```

## Services

### Service 1: Producer

Polls the Google Sheet on an interval and publishes uncategorized transactions to Kafka.

- **Input**: Google Sheets API (Transactions tab)
- **Output**: `transactions.uncategorized` topic
- **Polling interval**: Configurable (default 5 minutes)
- **Logic**:
  1. Read all rows from the Transactions tab
  2. Filter to rows where Category column is blank
  3. For each uncategorized row, publish to `transactions.uncategorized` with the Transaction ID as the Kafka message key
  4. Avro-serialized values registered with the schema registry
- **Deduplication strategy**: Stateless. Publishes all uncategorized rows on every poll cycle. Kafka log compaction on the topic ensures only the latest message per Transaction ID is retained. Re-publishing the same uncategorized transaction is harmless — the consumer is idempotent.
- **Error handling**: Log and skip individual rows that fail to serialize/publish. Continue processing remaining rows.

### Service 2: Categorizer Agent

Consumes uncategorized transactions and uses an AI agent to determine the correct category.

- **Input**: `transactions.uncategorized` topic
- **Output**: `transactions.categorized` topic
- **AI model**: Anthropic Sonnet 4.5 via Anthropic Java SDK
- **Agent tools**:
  1. **Sheet lookup** — Query the Google Sheet for past transactions with similar descriptions that already have categories assigned. Used to learn from historical categorization patterns (e.g., "COSTCO WHOLESAL" was previously categorized as "Groceries").
  2. **Web search** — For unfamiliar merchants, search the web to understand what the business is (e.g., "what is TRNSFR TO SAV 4832?"). Implementation TBD (Brave Search API, SerpAPI, or similar).
- **Logic**:
  1. Consume a transaction from `transactions.uncategorized`
  2. Invoke the agent with the transaction details
  3. Agent uses tools to gather context (historical categories, web search)
  4. Agent decides on a category from the set of categories already present in the sheet
  5. Publish the transaction (now with category populated) to `transactions.categorized`
- **Idempotency**: If the agent receives a transaction that was already categorized (race condition with re-publishing), it should detect this and skip/no-op.
- **Error handling**: If the agent cannot determine a category with reasonable confidence, publish to a dead-letter topic (`transactions.categorization-failed`) or log for manual review.

### Service 3: Category Writer

Consumes categorized transactions and writes the category back to the Google Sheet.

- **Input**: `transactions.categorized` topic
- **Output**: Google Sheets API (writes to Category column)
- **Logic**:
  1. Consume a categorized transaction from `transactions.categorized`
  2. Find the matching row in the Google Sheet by Transaction ID
  3. Write the category value to the Category column for that row
  4. Optionally write to a "Categorized Date" column
- **Idempotency**: Writing the same category to the same row is a no-op. If the row already has a category (manually categorized between publish and write), skip it.
- **Error handling**: If the row is not found (deleted from sheet?), log and commit the offset. Do not retry indefinitely.

## Data Model

### Google Sheet Columns (Transactions Tab)

| Column | Header              | Type     | Notes                                      |
|--------|---------------------|----------|--------------------------------------------|
| A      | Date                | Date     | Transaction date                           |
| B      | Description         | String   | Short merchant description                 |
| C      | Category            | String   | **Target field** — blank when uncategorized |
| D      | Amount              | Currency | Negative for debits                        |
| E      | Account             | String   | Account name                               |
| F      | Account #           | String   | Masked account number (e.g., xxxx6404)     |
| G      | Institution         | String   | Bank name                                  |
| H      | Month               | Date     | Month start                                |
| I      | Week                | Date     | Week start                                 |
| J      | Transaction ID      | String   | Yodlee-generated unique ID — **Kafka key** |
| K      | Check Number        | String   | Check number if applicable                 |
| L      | Full Description    | String   | Full merchant description from Yodlee      |
| M      | Note                | String   | User notes                                 |
| N      | Receipt             | String   | Receipt attachment                         |
| O      | Source              | String   | Data source (e.g., "Yodlee")              |
| P      | Categorized Date    | Date     | When the category was assigned             |
| Q      | Date Added          | DateTime | When Tiller imported the transaction       |
| R      | Metadata            | String   | Additional metadata                        |
| S      | Categorized         | String   | Categorization flag                        |
| T      | Tags                | String   | User tags                                  |
| U      | Migration Notes     | String   | Migration notes                            |

### Avro Schema: Transaction

```avsc
{
  "type": "record",
  "name": "Transaction",
  "namespace": "org.jevy.tiller_categorizer_agent",
  "fields": [
    {"name": "transaction_id",   "type": "string",            "doc": "Yodlee-generated unique ID (Kafka message key)"},
    {"name": "date",             "type": "string",            "doc": "Transaction date (M/d/yyyy)"},
    {"name": "description",      "type": "string",            "doc": "Short merchant description"},
    {"name": "category",         "type": ["null", "string"],  "default": null, "doc": "Category — null when uncategorized"},
    {"name": "amount",           "type": "string",            "doc": "Amount as string (e.g., '-$384.91')"},
    {"name": "account",          "type": "string",            "doc": "Account name"},
    {"name": "account_number",   "type": ["null", "string"],  "default": null, "doc": "Masked account number"},
    {"name": "institution",      "type": ["null", "string"],  "default": null, "doc": "Bank/institution name"},
    {"name": "month",            "type": ["null", "string"],  "default": null, "doc": "Month start date"},
    {"name": "week",             "type": ["null", "string"],  "default": null, "doc": "Week start date"},
    {"name": "check_number",     "type": ["null", "string"],  "default": null, "doc": "Check number if applicable"},
    {"name": "full_description", "type": ["null", "string"],  "default": null, "doc": "Full merchant description from Yodlee"},
    {"name": "note",             "type": ["null", "string"],  "default": null, "doc": "User notes"},
    {"name": "source",           "type": ["null", "string"],  "default": null, "doc": "Data source (e.g., Yodlee)"},
    {"name": "date_added",       "type": ["null", "string"],  "default": null, "doc": "When Tiller imported the transaction"},
    {"name": "sheet_row_number", "type": "int",               "doc": "Row number in the Google Sheet (for write-back)"}
  ]
}
```

**Notes on the schema:**
- `category` is nullable — null on `transactions.uncategorized`, populated on `transactions.categorized`
- `sheet_row_number` is included so the writer knows which row to update without re-scanning the sheet
- `amount` is kept as a string to preserve the currency formatting from the sheet. Parsing to a numeric type can happen in the consumer if needed.
- Fields like Receipt, Tags, Metadata, Categorized, Migration Notes are omitted — not needed for categorization.

### Kafka Topics

| Topic                               | Key             | Value Schema | Cleanup Policy | Notes                                   |
|--------------------------------------|-----------------|--------------|----------------|-----------------------------------------|
| `transactions.uncategorized`         | Transaction ID  | Transaction  | compact        | Uncategorized transactions from sheet   |
| `transactions.categorized`           | Transaction ID  | Transaction  | compact        | Transactions with agent-assigned category|
| `transactions.categorization-failed` | Transaction ID  | Transaction  | delete (7d)    | Transactions the agent couldn't categorize|

## Tech Stack

| Component           | Choice                          | Notes                                           |
|---------------------|---------------------------------|-------------------------------------------------|
| Language            | Kotlin                          |                                                 |
| Build               | Gradle (Kotlin DSL)             | Single project, single module                   |
| Kafka client        | `org.apache.kafka:kafka-clients`| Official Apache Kafka client                    |
| Avro                | `org.apache.avro:avro`          | With Gradle Avro plugin for code generation     |
| Schema Registry     | Confluent Avro serializer/deserializer | Compatible with Redpanda's built-in registry |
| Google Sheets       | `com.google.api-client` + `com.google.apis:google-api-services-sheets` | Service account auth |
| AI Agent            | Sonnet 4.5 via Anthropic Java SDK | `com.anthropic:anthropic-java`                |
| HTTP client         | OkHttp                          | For Brave web search API calls                  |
| Container           | Single Docker image             | Different entrypoints per service               |
| Deployment          | Kubernetes (existing homelab)   | Three Deployments, same image, different args   |

## Project Structure

```
tiller-categorizer-agent/
├── build.gradle.kts
├── settings.gradle.kts
├── gradle.properties
├── Dockerfile
├── src/
│   └── main/
│       ├── avro/
│       │   └── Transaction.avsc
│       ├── kotlin/org/jevy/tiller/categorizer/
│       │   ├── Main.kt                          # CLI: producer | categorizer | writer
│       │   ├── config/
│       │   │   └── AppConfig.kt                 # Environment variable config
│       │   ├── sheets/
│       │   │   └── SheetsClient.kt              # Google Sheets read/write
│       │   ├── kafka/
│       │   │   ├── KafkaFactory.kt              # Producer/consumer factory with Avro serde
│       │   │   └── TopicNames.kt                # Topic name constants
│       │   ├── producer/
│       │   │   └── TransactionProducer.kt       # Polls sheet, publishes uncategorized
│       │   ├── categorizer/
│       │   │   ├── CategorizerAgent.kt          # Orchestrates agent loop
│       │   │   ├── (uses Anthropic Java SDK)     # Claude API via SDK
│       │   │   └── tools/
│       │   │       ├── SheetLookupTool.kt       # Query historical transactions
│       │   │       └── WebSearchTool.kt         # Web search for unknown merchants
│       │   └── writer/
│       │       └── CategoryWriter.kt            # Writes category to sheet
│       └── resources/
│           └── logback.xml
├── k8s/
│   ├── deployment-producer.yaml
│   ├── deployment-categorizer.yaml
│   ├── deployment-writer.yaml
│   └── kustomization.yaml
└── .github/
    └── workflows/
        └── build.yaml                           # Build + push image
```

## Configuration (Environment Variables)

| Variable                    | Required By          | Description                              |
|-----------------------------|----------------------|------------------------------------------|
| `KAFKA_BOOTSTRAP_SERVERS`   | All                  | Redpanda broker address                  |
| `SCHEMA_REGISTRY_URL`       | All                  | Redpanda schema registry URL             |
| `GOOGLE_SHEET_ID`           | Producer, Writer, Categorizer | The spreadsheet ID from the sheet URL |
| `GOOGLE_CREDENTIALS_JSON`   | Producer, Writer, Categorizer | Service account JSON key (as string or file path) |
| `ANTHROPIC_API_KEY`         | Categorizer          | Anthropic API key                        |
| `POLL_INTERVAL_SECONDS`     | Producer             | How often to poll (default: 300)         |

## Kafka Cluster Details (Existing)

- **Broker**: `redpanda.apps.svc.cluster.local:9093`
- **Schema Registry**: `http://redpanda.apps.svc.cluster.local:8081`
- **Console**: `https://redpanda.jevy.org`
- **Replication factor**: 1 (single-node dev setup)
- **TLS**: Disabled on internal listeners

## Agent Design (Service 2)

The categorizer uses Sonnet 4.5 via the Anthropic Java SDK with function calling / tool use.

### System Prompt

```
You are a bookkeeping assistant that categorizes financial transactions.
You have access to the user's transaction history in a Google Sheet.
Your job is to determine the correct category for a given transaction.

Rules:
- Use ONLY categories that already exist in the user's sheet. Never invent new categories.
- Look at past transactions with similar descriptions to determine the category.
- If a merchant is unfamiliar, use web search to understand what the business is.
- If you cannot determine a category with confidence, respond with null.
```

### Tool Definitions

**sheet_lookup**
```json
{
  "name": "sheet_lookup",
  "description": "Search past transactions in the Google Sheet by description. Returns rows that have been previously categorized with similar merchant names.",
  "parameters": {
    "query": "Search term to match against the Description or Full Description columns"
  }
}
```

**web_search**
```json
{
  "name": "web_search",
  "description": "Search the web to identify an unfamiliar merchant or transaction description.",
  "parameters": {
    "query": "Search query"
  }
}
```

### Agent Flow

```
1. Receive transaction: { description: "COSTCO WHOLESAL", amount: "-$384.91", ... }
2. Call sheet_lookup("COSTCO WHOLESAL")
   → Returns: [{ description: "Costco Wholesal", category: "Groceries", amount: "-$250.00" }, ...]
3. High confidence match → Return category: "Groceries"

--- OR for unknown merchants: ---

1. Receive transaction: { description: "TRNSFR TO SAV 4832", amount: "-$500.00", ... }
2. Call sheet_lookup("TRNSFR TO SAV")
   → Returns: [] (no matches)
3. Call web_search("TRNSFR TO SAV bank transaction meaning")
   → Returns: "TRNSFR TO SAV is a bank transfer to savings account"
4. Call sheet_lookup("Transfer")
   → Returns: [{ description: "E-Transfer", category: "Transfers", ... }]
5. Return category: "Transfers"
```

## Row Number Stability

**Important consideration**: Google Sheets row numbers can shift when Tiller inserts new transactions. The `sheet_row_number` in the Avro message may become stale between publish and write-back.

**Mitigation in the writer (Service 3)**:
1. When writing back, look up the row by Transaction ID, not by stored row number
2. Use the stored row number as a hint/optimization — check that row first, fall back to a scan if the Transaction ID doesn't match
3. This makes the writer resilient to row insertions between categorization and write-back
