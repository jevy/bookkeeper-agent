# Sure Writer — Design Spec

**Date:** 2026-09-25
**Status:** Draft, awaiting review
**Scope:** Restructure the writer as two independent sink modules, Sheets and
Sure, each optional, and add the Sure sink so categorizations mirror into a
self-hosted [Sure](https://github.com/we-promise/sure) instance.

---

## 1. Intent

### The problem

Sure is a good personal-finance UI, but its built-in categorization is a
single zero-shot LLM call over `{amount, description, merchant}` against a
flat list of category names. No history, no tools, no retrieval. Transactions
that arrive through Sure's own bank-sync connections (SimpleFIN, Plaid, and
so on) stay uncategorized, or get categorized badly.

The bookkeeper agent already categorizes well, against a Google Sheet.

### The outcome

Every categorization the bookkeeper agent makes also lands in Sure, so Sure
becomes a useful UI over a correctly-categorized ledger without owning any
categorization logic. Corrections made by email reply flow through the same
path, so Sure tracks corrections too.

### Decisions

| Decision | Choice |
|---|---|
| Transaction source | **The Google Sheet, always.** Not optional. It owns transaction IDs, feeds the producer and the categorizer's `sheet_lookup` tool, and anchors the correction loop. |
| Write targets | **Two sink modules, each optional:** `SheetsSink` and `SureSink`. Same consumer skeleton, separate processes and consumer groups. |
| Ordering | **Chained.** Sure mirrors what actually landed in the Sheet. The Sheets sink publishes to `transactions.written` after a successful write; the Sure sink consumes that topic, not `transactions.categorized`. |
| Sure's role | **Sink only.** Nothing reads from Sure into the pipeline. |
| Corrections | Email-reply corrections republish through `transactions.categorized`, land in the Sheet, and then flow on to Sure. |
| Match failure | **DLQ topic**, mirroring `transactions.write-failed`. |
| Historical range | **No cutoff date.** Events for dates Sure does not hold will fail to match and land in the DLQ. Once that data is imported into Sure, the DLQ is replayed. |
| Backfill | **Consumer-group reset** on the Sheets writer, which re-emits history onto `transactions.written`. See §4. |
| Tenancy | Single user, configuration by environment variables. |

### Explicit non-goals

- Sure does not become a transaction source.
- The Sheets path's observable behaviour does not change: same writes, same
  tombstones, same DLQ topic, same metric names. Its internals move into the
  shared skeleton (§2), which is a refactor, not a behaviour change.
- The digest and email-reply loop stays pointed at the Sheet.
- No changes to the categorizer, its prompt, or its tools.
- Transactions present in Sure but absent from the Sheet stay uncategorized.
- Categories edited by hand directly in the Sheet do not flow to Sure. Only
  agent categorizations and email corrections pass through Kafka.

### Success criteria

1. A transaction categorized by the agent appears with that category in Sure
   within one polling interval, without manual action.
2. On first deploy, the full categorized history replays into Sure with no
   separate backfill tooling beyond a consumer-group reset.
3. A categorization that cannot be matched to a Sure transaction is visible in
   a DLQ topic and in metrics, never silently dropped.
4. A subsequent bank sync in Sure does not overwrite a category the agent
   wrote.
5. A Sure outage at any point, startup or mid-replay, never drains the topic
   into the DLQ. The pod restarts and resumes where it left off.
6. `CategoryWriterTest` passes unchanged against the Sheets sink, and Sheets
   writer metrics are unchanged throughout the rollout.

---

## 2. Architecture

```
            transactions.categorized
            (compact, keyed by transaction_id)
                       │
               group: category-writer
                       │
                  SinkWriter (shared loop)
                       │
                  SheetsSink
                       │
                Google Sheets (source of truth)
                       │
     ┌─────────────────┼──────────────────────┐
     │                 │                      │
  Written or        Rejected               Rejected
  already there        │                      │
     │            tombstone →            DLQ →
     │            transactions.          transactions.
     │            uncategorized          write-failed
     ▼
transactions.written                       ← NEW, compact
(landed in the Sheet)
     │
group: sure-writer                         ← NEW
     │
SinkWriter (shared loop)
     │
  SureSink
     │
Sure REST API  PATCH /api/v1/transactions/:id
     │
  on unmatched / 4xx → transactions.sure-write-failed
  on 5xx / timeout   → exit loop, liveness restarts pod
```

The Sure writer never sees a categorization that did not land in the Sheet.
If the Sheet write is rejected, Sure is untouched. There is no window where
Sure is ahead of the Sheet.

### The shared skeleton

`CategoryWriter.run` today owns polling, commit, DLQ routing, metrics, and
health callbacks, tangled with Sheets logic. Extract that loop into
`SinkWriter`, parameterised by a `CategorySink`:

```kotlin
interface CategorySink {
    val name: String                       // "writer" | "sure"; metric prefix
    val consumerGroup: String              // "category-writer" | "sure-writer"
    val sourceTopic: String                // CATEGORIZED for Sheets, WRITTEN for Sure
    val dlqTopic: String
    fun write(tx: Transaction): SinkResult
}

sealed interface SinkResult {
    object Written : SinkResult
    /** landed = true means the target already holds this category (the write is effectively done). */
    data class Skipped(val reason: String, val landed: Boolean = false) : SinkResult
    data class Rejected(val reason: String, val cause: Throwable? = null) : SinkResult  // → DLQ, continue
    data class Unavailable(val cause: Throwable) : SinkResult                            // → exit loop, no commit
}
```

`SinkWriter(config, sink, meterRegistry, tombstoneUncategorized = false,
publishOnDone: String? = null)` per record:

| Result | Action |
|---|---|
| `Written` | increment `written`, tombstone if enabled, publish to `publishOnDone` if set, commit |
| `Skipped(landed = true)` | increment `skipped`, tombstone if enabled, publish to `publishOnDone` if set, commit |
| `Skipped(landed = false)` | increment `skipped`, tombstone if enabled, commit |
| `Rejected` | send to `sink.dlqTopic`, tombstone if enabled, increment `errors` and `rejected`, commit |
| `Unavailable` | do **not** commit, `onAlive(false)`, rethrow. Pod restarts and resumes from the last committed offset. |

Before every commit the loop flushes its producers, so a committed offset can
never outrun its `transactions.written` publish, DLQ send or tombstone.

The Sheets writer runs with `tombstoneUncategorized = true` and
`publishOnDone = transactions.written`. The Sure writer runs with neither.
Publishing on `Skipped(landed = true)` is what makes a Sheets consumer-group
reset re-emit the whole history for Sure (§4) without rewriting the Sheet.

Tombstoning `transactions.uncategorized` is a Sheets-only concern. Today
`CategoryWriter` tombstones on every non-throwing outcome, including the
already-categorized skip, and on failure (the transaction is done from the
pipeline's point of view once it is in the DLQ). The flag therefore applies to
`Written`, `Skipped` and `Rejected` alike. The Sure sink never writes to
`transactions.uncategorized`.

**Why still two processes and two consumer groups:** the sinks must fail
independently. A Sure outage must not stall Sheet writes, must not produce
spurious `transactions.write-failed` records, and must not block tombstones.
"Module" means a code unit, not one loop writing to both targets.

### New topic

`transactions.written`: `cleanup.policy=compact`, keyed by `transaction_id`,
`retention.ms` in `deleteConfigs` so the compacted history is kept
indefinitely. It holds the latest category that is confirmed to be in the
Sheet, one record per transaction.

**Why not a sink connector:** the matching logic in §5 is application logic
with a fallback ladder, not a field mapping.

### Package layout

```
src/main/kotlin/org/jevy/bookkeeper/writer/
    SinkWriter.kt          the shared consumer loop (extracted from CategoryWriter.run)
    CategorySink.kt        interface + SinkResult
    CategoryWriter.kt      becomes a thin wrapper: SinkWriter(SheetsSink(...), tombstoneUncategorized = true)
    SheetsSink.kt          writeCategory + findRow, moved verbatim from CategoryWriter
src/main/kotlin/org/jevy/bookkeeper/sure/
    SureSink.kt            implements CategorySink; orchestrates match → resolve → patch
    SureClient.kt          HTTP: auth, retry, rate limit, pagination
    TransactionMatcher.kt  the fallback ladder in §5
    CategoryResolver.kt    Sure category name → id, cached
    AmountConvention.kt    the one sign-flip function (§5)
```

`Main.kt` gains a `sure-writer` command. The `writer` command keeps its name
and consumer group so no offsets move.

Follow existing conventions: constructor injection with defaults, Micrometer
counters registered at construction, SLF4J, `onActivity`/`onAlive` callbacks.

---

## 3. Configuration

All read through `AppConfig`. `AppConfig.fromEnv()` currently requires
`GOOGLE_SHEET_ID` and `GOOGLE_CREDENTIALS_JSON`; the `sure-writer` process
does not need them but gets them anyway from the shared secret. Do not loosen
`requireEnv`; the Sheet is mandatory by decision.

| Variable | Required | Notes |
|---|---|---|
| `SURE_API_URL` | for `sure-writer` | Base URL of the Sure instance. If Sure sits behind an auth proxy, point this at the internal service, not the public ingress. |
| `SURE_API_KEY` | for `sure-writer` | A Sure API key with read and write access to transactions. |
| `SURE_ENABLED` | no, default `true` | Kill switch. When false the process consumes and commits without matching or writing. Nothing is lost: compaction keeps the latest event per key, and a consumer group reset replays it. |
| `SURE_MAX_API_CALLS_PER_SEC` | no, default `5` | Throttles **all** Sure API calls, GET and PATCH. Matching costs one to three GETs per record, so throttling only writes would not protect Sure during replay. |
| `SURE_DRY_RUN` | no, default `false` | Match and resolve, log the intended PATCH, perform none. Commits offsets. |
| `SURE_ACCOUNT_MAP` | for `sure-writer` | `"Sheet account name=sure-account-uuid;..."`. Sure account **IDs**, not names. |

`SHEETS_ENABLED` is deliberately absent. The Sheets sink is toggled by
deploying or not deploying the `writer` process, which is already the case.

---

## 4. Backfill via replay

`transactions.written` starts empty on first deploy. The backfill is a
consumer-group reset on the **Sheets** writer:

```
kafka-consumer-groups --bootstrap-server ... --group category-writer \
  --topic transactions.categorized --reset-offsets --to-earliest --execute
```

(Scale the `writer` Deployment to zero first; the reset is refused while the
group has members. Scale it back up afterwards.)

`transactions.categorized` is `cleanup.policy=compact`, so it holds the latest
categorization for every transaction the agent has ever categorized. The
Sheets writer re-walks it, finds each row already carries the category,
returns `Skipped("already_categorized", landed = true)`, and the loop
publishes the record to `transactions.written`. No Sheet cell is rewritten.
The Sure writer, on a fresh group with `auto.offset.reset=earliest` (the
`KafkaFactory` default), then consumes the whole of `transactions.written`.

Consequences:

1. **Sheets cost.** Each replayed record is one full-sheet read plus one cell
   read, the same as normal processing. A thousand events is roughly ten
   minutes against the Sheets API read quota.
2. **Sure burst.** Throttled by `SURE_MAX_API_CALLS_PER_SEC`. At the default
   5/s and roughly two calls per record, a thousand events replay in about
   seven minutes.
3. **Coverage.** Replay only covers transactions that passed through the
   Sheet. Events for dates Sure does not hold cannot match and will DLQ.
   After that data is imported into Sure, `dlq-replay sure` (§7) pushes them
   through.
4. **Poll settings.** `KafkaFactory.createConsumer` sets
   `max.poll.records=1` and `max.poll.interval.ms=360000`. Bounded retries in
   `SureClient` must complete well inside six minutes per record.

### Required guard

`TopicInitializer` sets `deleteConfigs = listOf(RETENTION_MS_CONFIG)` on every
compacted topic except `CATEGORIZED`, which is left with the broker default
`retention.ms`. Compact-only topics ignore `retention.ms` today, but the
Sheets-side replay rests on that. **Add `deleteConfigs =
listOf(TopicConfig.RETENTION_MS_CONFIG)` to the `CATEGORIZED` TopicSpec** with a
comment that replay-from-zero is load-bearing, and give `WRITTEN` the same
treatment from the start.

---

## 5. Matching

`Transaction.transaction_id` is the Sheet's identifier (Yodlee via Tiller).
Sure's transactions carry provider ids (exposed as `external_id`) or
import-assigned ids. They share no key.

`TransactionMatcher` is an explicit ladder, stopping at the first unambiguous
hit. Each rung is recorded as a metric tag.

| Rung | Strategy | Notes |
|---|---|---|
| 1 | **In-memory cache** | `transaction_id → sure_transaction_id`, process lifetime only. Saves API calls on redelivery. **Not persisted.** Sure cannot hold it (`external_id` is create-only and synced rows own theirs), the PATCH is idempotent, and rung 2 is a single GET, so durability buys nothing. |
| 2 | **Exact triple** | `GET /api/v1/transactions?account_id=&start_date=&end_date=&min_amount=&max_amount=&per_page=100` with date and amount both pinned. One result → match. |
| 3 | **Triple with date window** | Same, ±3 days, for posting-date drift between the Sheet's provider and Sure's. One result → match. |
| 4 | **Triple + description similarity** | If rungs 2–3 return several candidates, disambiguate on normalized description. Require a clear winner. |
| — | **No match, or still ambiguous** | `Rejected` → DLQ. Never guess. |

Verified against Sure's source: the index supports `account_id`,
`start_date`, `end_date`, `min_amount`, `max_amount`, `search`, and `per_page`
up to 100. The response includes each transaction's current `category` and
`external_id`, so the rung 2 query also serves the read-before-write check in
§6 at no extra cost.

### Amount and account normalization

- **Amount** arrives as a display string (`"-$384.91"`). Parse to a signed
  decimal. **Sure's database sign is inverted relative to the Sheet's:**

  | | Expense | Income |
  |---|---|---|
  | Sheet / Avro `amount` | negative | positive |
  | Sure `entries.amount` (DB, what `min_amount`/`max_amount` filter on) | positive | negative |
  | Sure API `signed_amount_cents` (response) | negative | positive |

  `sure_entry_amount = -sheet_amount`, used **only** for the query
  parameters. Responses are compared on `signed_amount_cents`, which already
  matches the Sheet's sign. One function in `AmountConvention.kt`, a test per
  direction. An inverted comparison fails silently by matching nothing, so
  this must not be scattered.
- **Account.** Sheet account names and Sure account names differ, and a Sure
  instance that has both a CSV import and a bank-sync connection may hold
  duplicate accounts. Do not infer. `SURE_ACCOUNT_MAP` maps each Sheet
  account name to one Sure account **ID**. Merge duplicate accounts in Sure
  before writing the map; that is an operator prerequisite, not code.

---

## 6. Writing

`SureSink.write` in order:

1. Match (§5). Unmatched → `Rejected("no_match")` or `Rejected("ambiguous")`.
2. Resolve category name → id. Unresolved → `Rejected("category_unresolved")`.
3. **Read-before-write.** If the matched Sure transaction's `category.id`
   already equals the resolved id → `Skipped("already_categorized")`. The Sheets
   sink has the same check. Without it a replay re-PATCHes every row.
4. PATCH:

```http
PATCH /api/v1/transactions/:id
X-Api-Key: <key>
{ "transaction": { "category_id": "<uuid>" } }
```

**Do not send `user_modified`.** It is permitted by `transaction_params` but
only `create` acts on it (`mark_user_modified!`); `update` ignores it.
Criterion 4 holds by a different mechanism, verified in Sure's source:
`update` calls `@entry.lock_saved_attributes!`, which locks `category_id` on
the transaction, and the provider import path writes categories through
`enrich_attribute(:category_id, ...)`, which skips locked attributes. Sure's
own auto-categorizer additionally only fills a blank category. So a category
written by PATCH survives both bank sync and Sure enrichment.

The `X-Api-Key` header is how `Api::V1::BaseController` authenticates API
keys.

### Response shape the matcher relies on

`GET /api/v1/transactions` returns `{ "transactions": [...], "pagination":
{ "page", "per_page", "total_count", "total_pages" } }`. Each transaction has
`id`, `date` (ISO), `name`, `external_id`, `signed_amount_cents` (integer,
income positive, expense negative), `account { id, name }`, and
`category { id, name } | null`.

### Category resolution

`CategoryResolver` fetches `GET /api/v1/categories` once, caches by
case-folded name, and refreshes on a miss.

The assumption is that Sure's categories were created with the same names
the Sheet uses (for example by importing the Sheet's history into Sure). No
mapping table.

Hazards:

- **Case-duplicate rows:** if Sure holds both `groceries` and `Groceries` as
  separate records, a folded name resolves to more than one id →
  `Rejected("category_ambiguous")`. Silently splitting a category across two
  ids corrupts reporting. Merge them in Sure.
- **Unknown name** → `Rejected("category_unresolved")`. Do not auto-create
  categories. Silent taxonomy drift is worse than a visible failure.

---

## 7. Failure handling

| Condition | `SinkResult` | Effect |
|---|---|---|
| No match / ambiguous | `Rejected` | DLQ, continue, commit |
| Unresolvable or ambiguous category | `Rejected` | DLQ, continue, commit |
| Sure 4xx that is about this transaction (400, 404, 422) | `Rejected` | DLQ, continue, commit |
| Sure 401 or 403 | `Unavailable` immediately, no retry | **No commit.** A rotated API key or a misrouted URL must never drain the backlog into the DLQ. Loop exits, pod restarts. |
| Sure 408, 429, 5xx, timeout, connection refused, at startup **or mid-run** | `Unavailable` after bounded retry (3 attempts, backoff 1s then 2s) | **No commit.** Loop exits, liveness fails, pod restarts, resumes at last committed offset. |
| `SURE_ENABLED=false` | `Skipped("disabled")` | commit |
| `SURE_DRY_RUN=true` | `Skipped("dry_run")` after logging the intended PATCH | commit |

The `Unavailable` row matters. Without it a mid-replay outage would convert
the rest of the backlog into DLQ records at throttle speed. Distinguishing
"Sure is down" from "this transaction cannot be matched" is the sink's job,
and it is expressed in the result type rather than in the loop.

### DLQ topic

`transactions.sure-write-failed`, same TopicSpec shape as `WRITE_FAILED`:
`cleanup.policy=compact`, `delete.retention.ms=86400000`,
`RETENTION_MS_CONFIG` in `deleteConfigs`. Add a `TopicNames` constant and a
TypeStream view alongside `pipelines/write-failed-view.typestream.json`.

### DLQ replay

The existing `dlq-replay` command republishes to `transactions.uncategorized`
so the categorizer runs again. That is wrong for Sure failures: the category is
already known and correct, and re-categorizing costs LLM calls. Add a mode:

```
bookkeeper-agent dlq-replay            # existing: categorization-failed + write-failed → uncategorized
bookkeeper-agent dlq-replay sure       # new: sure-write-failed → written, unchanged
```

The Sure mode tombstones the DLQ entry and republishes the record **as-is** to
`transactions.written`. The category is already in the Sheet, so the Sheets
writer is not involved; only the Sure sink retries the match.

**No tombstones.** The Sure sink never writes to `transactions.uncategorized`.

---

## 8. Observability

Metrics under `bookkeeper.sure.*`, parallel to `bookkeeper.writer.*`. The
Sheets sink keeps its exact existing metric names; `SinkWriter` takes the
prefix from `CategorySink.name` and the Sheets prefix stays `writer`.

| Metric | Type | Tags |
|---|---|---|
| `bookkeeper.sure.transactions.written` | counter | — |
| `bookkeeper.sure.transactions.skipped` | counter | — |
| `bookkeeper.sure.transactions.skipped.reasons` | counter | `reason` |
| `bookkeeper.sure.transactions.rejected` | counter | — |
| `bookkeeper.sure.transactions.rejected.reasons` | counter | `reason` |
| `bookkeeper.sure.match.rung` | counter | `match_rung` |
| `bookkeeper.sure.errors` | counter | — |
| `bookkeeper.sure.duration` | timer | — |
| `bookkeeper.sure.api.duration` | timer | `endpoint`, `status` |

The untagged counters are registered at startup so every series exists at
zero; the alert expressions still guard with `or vector(0)`. The per-reason
breakdowns use separate names because the Prometheus registry rejects one
name registered with two different tag-key sets. The Sheets sink gets the
same shape under `bookkeeper.writer.*`; its pre-existing untagged `written`,
`skipped`, `errors` and `duration` names are unchanged, and `skipped` no
longer counts row-not-found, which now lands in `rejected` (and `errors`, as
before).

`match_rung` is the one to watch: steady state should sit on rungs 1 and 2. A
drift toward 3–4 means the account map or the amount convention needs
attention.

`PrometheusRule` additions alongside `k8s/app/prometheusrule.yaml`:

- `SureWriterRejectRateHigh`: `rejected` > 20% of processed over 1h. Expect
  it to fire during an initial replay if Sure is missing date ranges the
  Sheet has. Silence it for the replay window rather than weakening the rule.
- `SureWriterStalled`: no activity for 24h while the Sheets writer is active.
- `SureWriterConsumerGroupMembersLow`: no members in `sure-writer` for 5m.

---

## 9. Operator prerequisites

Before enabling writes against a real Sure instance:

1. Merge any duplicate accounts in Sure (common when both a CSV import and a
   bank-sync connection created the same account).
2. Merge any case-duplicate categories.
3. Write `SURE_ACCOUNT_MAP` using the surviving Sure account IDs.
4. Create an API key in Sure with transaction read and write scope.

---

## 10. Testing

Follow `src/test/kotlin/org/jevy/bookkeeper/writer/CategoryWriterTest.kt` and
the broker-gated integration style from `3db4d2b`.

**Refactor safety**
- `CategoryWriterTest` passes with no assertion changes after
  `writeCategory`/`findRow` move into `SheetsSink`. Test-only constructor
  shims are acceptable; assertion changes are not.

**Unit**
- `SinkWriter` with a fake sink: each `SinkResult` variant produces exactly the
  action in the §2 table; `Unavailable` leaves the offset uncommitted and calls
  `onAlive(false)`; tombstoning fires for `Written` and `Rejected` only when
  enabled; `publishOnDone` fires for `Written` and `Skipped(landed = true)`
  only, and only when set; the loop subscribes to `sink.sourceTopic`.
- `TransactionMatcher`: one test per rung; ambiguity yields no match; the
  ±3-day window includes boundaries; unmapped account, bad amount and bad
  date reject before any API call.
- `AmountConvention`: `"-$384.91"`, `"$1,865.61"`, `"$0.00"`, plain decimal,
  malformed input; the sign flip in both directions.
- `CategoryResolver`: case-insensitive hit; duplicate folded name is
  ambiguous; unknown name refreshes once then reports unknown.
- `SureClient`: auth header, filters, pagination, 4xx no retry, 5xx retry then
  unavailable, connection failure, rate limiting.
- `SureSink`: already-categorized → `Skipped`; unmatched → `Rejected`;
  unavailable anywhere → `Unavailable`; disabled and dry run skip with zero
  PATCHes.

**Integration (broker-gated, stubbed Sure)**
- An event on `transactions.written` produces exactly one PATCH.
- Match failure produces one DLQ record and no PATCH.
- Sure unreachable: loop exits, no offset commit, no DLQ records.
- `SURE_DRY_RUN=true` performs zero writes.

**Manual gate before enabling writes**
Deploy with `SURE_DRY_RUN=true`, let the full replay run, review the rung
distribution and would-be reject reasons.

---

## 11. Rollout

1. **Refactor only.** Extract `SinkWriter` and `SheetsSink`. Ship. Confirm
   `bookkeeper.writer.*` metrics and behaviour are unchanged.
2. Do the operator prerequisites in §9.
3. Run `init` to add the `written` and `sure-write-failed` topics and the
   `CATEGORIZED` guard.
4. Backfill `transactions.written`: scale `writer` to zero, reset the
   `category-writer` group to earliest (§4), scale it back up, wait for lag
   to reach zero.
5. Deploy `sure-writer` with `SURE_ENABLED=true`, `SURE_DRY_RUN=true`. Inspect
   rung distribution and reject reasons across the full replay.
6. Fix the account map until rung 2 dominates.
7. Set `SURE_DRY_RUN=false`. Reset the `sure-writer` consumer group to
   earliest on `transactions.written` to re-replay.
8. Confirm Sure's uncategorized count drops and Sheets writer metrics are
   unchanged throughout.
9. If Sure was missing date ranges and they are later imported:
   `dlq-replay sure`.

Rollback is `SURE_ENABLED=false`, or scaling `sure-writer` to zero. Nothing in
the Sheets path depends on the Sure process.
