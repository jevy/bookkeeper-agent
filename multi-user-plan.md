# Multi-User SaaS Architecture for Bookkeeper Agent

## Context

The app currently runs as a single-user system: one Google Sheet, one set of credentials, one Kafka deployment. To offer this as a monthly service, we need tenant isolation, composite Kafka keys, usage metering, a tenant registry, and OAuth-based onboarding — all while keeping things simple for a solo developer at dozens-to-low-hundreds scale.

---

## 1. Kafka Message Key: Composite `{tenant_id}:{transaction_id}`

**Problem**: Yodlee transaction IDs are scoped per-Yodlee-user, not globally unique. Two users could have the same `txn-12345`, causing log compaction to silently overwrite one user's data.

**Solution**: Prefix every Kafka key with a deterministic 8-char hex hash of the sheet ID:

```kotlin
// New file: src/main/kotlin/.../kafka/TenantKey.kt
object TenantKey {
    fun tenantId(sheetId: String): String =
        MessageDigest.getInstance("SHA-256")
            .digest(sheetId.toByteArray())
            .take(4)
            .joinToString("") { "%02x".format(it) }  // e.g. "a3f8b2c1"

    fun build(tenantId: String, transactionId: String) = "$tenantId:$transactionId"
    fun parseTenantId(key: String) = key.substringBefore(':')
    fun parseTransactionId(key: String) = key.substringAfter(':')
}
```

**Impact**: All `ProducerRecord` key usages in `TransactionProducer.kt`, `CategorizerAgent.kt`, and `CategoryWriter.kt` change from `transactionId` to `TenantKey.build(tenantId, transactionId)`. Compaction, tombstones, and partitioning all work correctly with this scheme.

---

## 2. Avro Schema Evolution

**`Transaction.avsc`** — add one field (backward-compatible, nullable with default):

```json
{"name": "tenant_id", "type": ["null", "string"], "default": null, "doc": "8-char hex tenant ID derived from sheet ID hash"}
```

Keep the existing `owner` field (raw sheet ID) — the writer and categorizer need it to connect to the correct Google Sheet. `tenant_id` is the short key prefix used in Kafka keys, logging, and metering.

---

## 3. Topic Strategy: Shared Topics (no per-tenant topics)

Keep the existing 4 topics shared across all tenants. At dozens-to-hundreds scale, per-tenant topics (400+ topics) would be operational overhead for no benefit.

No new Kafka topics needed for tenant management — that moves to Postgres (see section 4).

The composite key ensures no cross-tenant collisions on shared topics. Default hash partitioning distributes tenants across partitions naturally.

---

## 4. Tenant Registry: Postgres

**Source of truth**: Postgres database. Stores tenant metadata AND OAuth refresh tokens (secrets can't live in a Kafka topic).

### Schema

```sql
CREATE TABLE tenants (
    tenant_id       VARCHAR(8) PRIMARY KEY,  -- SHA-256 hash prefix of sheet_id
    sheet_id        TEXT NOT NULL UNIQUE,
    display_name    TEXT,
    email           TEXT,
    tier            TEXT NOT NULL DEFAULT 'free',
    max_txns_month  INT NOT NULL DEFAULT 100,
    active          BOOLEAN NOT NULL DEFAULT true,
    additional_context_prompt TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ
);

CREATE TABLE tenant_credentials (
    tenant_id           VARCHAR(8) PRIMARY KEY REFERENCES tenants(tenant_id),
    google_refresh_token TEXT NOT NULL,  -- OAuth2 refresh token
    google_access_token  TEXT,           -- cached, auto-refreshed
    token_expiry         TIMESTAMPTZ,
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE usage (
    tenant_id   VARCHAR(8) REFERENCES tenants(tenant_id),
    month       DATE NOT NULL,           -- first of month (2026-02-01)
    txn_count   INT NOT NULL DEFAULT 0,
    PRIMARY KEY (tenant_id, month)
);
```

### TenantRegistry class

```kotlin
// New file: src/main/kotlin/.../tenant/TenantRegistry.kt
class TenantRegistry(private val dataSource: DataSource) {
    fun getActiveTenants(): List<Tenant>
    fun getTenant(tenantId: String): Tenant?
    fun getCredentials(tenantId: String): GoogleCredentials  // returns UserCredentials from refresh token
    fun incrementUsage(tenantId: String, month: YearMonth)
    fun getUsage(tenantId: String, month: YearMonth): Int
}
```

### Credential loading

The existing `google-auth-library-oauth2-http` already supports `UserCredentials`. The `SheetsClient` changes minimally — instead of loading from a JSON file, it receives a `GoogleCredentials` from the registry:

```kotlin
// Per-tenant credential creation
UserCredentials.newBuilder()
    .setClientId(config.googleOAuthClientId)      // global app credential
    .setClientSecret(config.googleOAuthClientSecret) // global app credential
    .setRefreshToken(tenant.refreshToken)           // per-tenant from Postgres
    .build()
```

`UserCredentials` handles access token refresh automatically.

### Management CLI (for now, until OAuth UI exists)

```
bookkeeper-agent tenant add --sheet-id=<id> --refresh-token=<token> --email=<email> --tier=basic
bookkeeper-agent tenant list
bookkeeper-agent tenant deactivate --tenant-id=<id>
```

### New dependencies

```kotlin
// build.gradle.kts
implementation("org.postgresql:postgresql:42.7.4")
// Optional: Exposed (Kotlin SQL library) or just raw JDBC
```

---

## 5. Service Changes for Multi-Tenancy

### Producer (`TransactionProducer.kt`)
- Iterate over `tenantRegistry.getActiveTenants()` instead of one hardcoded sheet
- Get per-tenant `GoogleCredentials` from registry (OAuth refresh token)
- Create `SheetsClient` per tenant per poll cycle
- Set `tenant_id` on every Transaction record, use composite key
- Cap per-tenant output (e.g., 50 transactions per cycle) to prevent noisy tenants
- Check usage count before publishing; skip tenant if at limit

### Categorizer (`CategorizerAgent.kt`) — highest isolation risk
- Create a per-tenant `TenantContext` (cached 15min) holding:
  - Scoped `SheetsClient` (using that tenant's OAuth credentials)
  - Scoped system prompt (that tenant's categories)
- All tool calls (`sheet_lookup`, `category_lookup`, `autocat_lookup`) use the scoped `SheetsClient`
- Fair scheduling: group polled records by tenant, process round-robin
- MDC logging with `tenant_id` for traceability

### Writer (`CategoryWriter.kt`)
- Get per-tenant credentials from registry using `owner` field on the Transaction record
- Use a per-tenant cached `SheetsClient` for writes
- Per-tenant column letter resolution cache
- Increment usage count in Postgres after successful write

### Credential Model: Google OAuth2 (all users, including yourself)
- Create a Google Cloud project with OAuth consent screen
- Request `https://www.googleapis.com/auth/spreadsheets` scope
- **Google app review required**: `spreadsheets` is a "restricted" scope — needs privacy policy + security assessment for production mode. In testing mode, refresh tokens expire after 7 days and max 100 test users. Plan for review early.
- Store OAuth client ID + secret as global app credentials (K8s secret)
- Per-user refresh tokens stored in Postgres `tenant_credentials` table
- `UserCredentials` handles access token refresh automatically
- `SheetsClient` modified to accept `GoogleCredentials` directly (no more service account JSON loading)
- OAuth onboarding UI deferred to a later phase (use CLI + Google OAuth Playground for early tenants)

---

## 6. KStreams Usage Metering (New Service)

**New service**: `UsageMeter` — a standalone KStreams app consuming `transactions.categorized`.

```
transactions.categorized → [re-key by tenant_id] → [windowed count per 30-day window]
                                                         → state store "tenant-usage-counts"
                                                         → HTTP endpoint /usage/{tenant_id}
```

- Tumbling 30-day window, counted per `tenant_id`
- Queryable via KStreams Interactive Queries (lightweight HTTP endpoint reusing the `Metrics.kt` HTTP server pattern)
- **Also writes to Postgres `usage` table** for durable billing records
- New entry point: `bookkeeper-agent meter`
- New K8s deployment (1 replica)
- New dependency: `org.apache.kafka:kafka-streams`

### Rate Limiting
- **Enforcement point**: Producer (before publishing) checks usage from Postgres
- **When exceeded**: Skip that tenant's sheet, log warning, emit metric
- Transactions stay in their Google Sheet (no data loss) until the next month

---

## 7. Deployment Changes

Single multi-tenant deployment (no per-tenant K8s resources):

```
CronJob:    producer       (iterates all tenants from Postgres registry)
Deployment: categorizer    (1-3 replicas, consumer group distributes partitions)
Deployment: writer         (1-2 replicas)
Deployment: usage-meter    (1 replica, KStreams)  [NEW]
Deployment: postgres       (1 replica, PVC)       [NEW]
Job:        topic-init     (unchanged)
```

K8s Secret changes:
- Remove per-user `GOOGLE_SHEET_ID` from secret
- Remove `GOOGLE_CREDENTIALS_JSON` (no more service account)
- Add `GOOGLE_OAUTH_CLIENT_ID`, `GOOGLE_OAUTH_CLIENT_SECRET`
- Keep `OPENROUTER_API_KEY`, `BRAVE_API_KEY`
- Add `POSTGRES_URL`, `POSTGRES_USER`, `POSTGRES_PASSWORD`
- Per-tenant data (sheet IDs, refresh tokens, tiers) lives in Postgres

---

## 8. Implementation Phases

| Phase | Scope | Deployable? |
|-------|-------|-------------|
| **1. Key + Schema** | Add `tenant_id` to Avro, create `TenantKey.kt`, update all Kafka key usages | Yes (single-user still works, just with new key format) |
| **2. Postgres + Registry** | Add Postgres, create tables, `TenantRegistry.kt`, CLI commands, register yourself | Yes (you become tenant #1) |
| **3. OAuth Credentials** | Create Google Cloud OAuth app, modify `SheetsClient` to accept `UserCredentials`, store refresh token in Postgres | Yes (your own OAuth token) |
| **4. Multi-tenant Producer** | Refactor producer to iterate tenants, per-tenant credentials from registry | Yes (can add second user) |
| **5. Multi-tenant Categorizer** | Per-tenant `TenantContext`, scoped tools, fair scheduling, MDC logging | Yes |
| **6. Multi-tenant Writer** | Per-tenant `SheetsClient`, usage counting | Yes |
| **7. Usage Meter** | KStreams service, HTTP endpoint, Postgres sync, K8s deployment | Yes |
| **8. Rate Limiting** | Producer checks usage before publishing | Yes |
| **9. OAuth Onboarding UI** | Web endpoint for user self-service signup (deferred) | Future |

---

## Files to Modify

| File | Changes |
|------|---------|
| `src/main/avro/Transaction.avsc` | Add `tenant_id` field |
| `src/main/kotlin/.../kafka/TenantKey.kt` | **New** — composite key utility |
| `src/main/kotlin/.../kafka/TopicNames.kt` | No changes needed (no new topics) |
| `src/main/kotlin/.../kafka/TopicInitializer.kt` | No changes needed |
| `src/main/kotlin/.../config/AppConfig.kt` | Add OAuth client ID/secret, Postgres URL; make `sheetId` optional |
| `src/main/kotlin/.../producer/TransactionProducer.kt` | Multi-tenant iteration, composite keys, per-tenant credentials |
| `src/main/kotlin/.../categorizer/CategorizerAgent.kt` | Per-tenant context/tools scoping, composite keys |
| `src/main/kotlin/.../categorizer/tools/SheetLookupTool.kt` | Accept `SheetsClient` param |
| `src/main/kotlin/.../writer/CategoryWriter.kt` | Per-tenant `SheetsClient`, composite keys, usage increment |
| `src/main/kotlin/.../sheets/SheetsClient.kt` | Accept `GoogleCredentials` directly (not just from config JSON); expose `sheetId` |
| `src/main/kotlin/.../Main.kt` | Add `tenant` and `meter` commands |
| `src/main/kotlin/.../tenant/TenantRegistry.kt` | **New** — Postgres-backed registry |
| `src/main/kotlin/.../meter/UsageMeter.kt` | **New** — KStreams metering service |
| `build.gradle.kts` | Add `kafka-streams`, `postgresql` dependencies |
| `k8s/app/deployment-*.yaml` | Update env vars (remove sheet ID, add OAuth + Postgres) |
| `k8s/app/deployment-usage-meter.yaml` | **New** — meter deployment |
| `k8s/app/deployment-postgres.yaml` | **New** — Postgres with PVC |
| `docker-compose.yml` | Add Postgres service for local dev |

## Verification

- **Phase 1**: Run existing tests, deploy, verify single-user flow still works with new key format
- **Phase 2**: `tenant add`, `tenant list`, verify Postgres has your record
- **Phase 3**: Generate OAuth refresh token manually (via Google OAuth Playground or CLI), verify `SheetsClient` works with `UserCredentials`
- **Phase 4**: Add a test sheet as tenant #2, verify producer publishes for both tenants with distinct composite keys
- **Phase 5**: Verify categorizer uses correct categories/history per tenant (check logs with MDC tenant_id)
- **Phase 6**: Verify writer routes categories to the correct sheet, usage count increments in Postgres
- **Phase 7**: Deploy meter, hit `/usage/{tenant_id}`, verify counts match Postgres
- **Phase 8**: Set a tenant's limit to 1, verify producer skips after 1 transaction
