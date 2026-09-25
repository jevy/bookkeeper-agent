# Sure Writer — Design Spec

**Date:** 2026-09-25 (revised after review, same day)
**Status:** Draft, awaiting review
**Scope:** Restructure the writer as two independent sink modules, Sheets and
Sure, each optional, and add the Sure sink so categorizations mirror into the
self-hosted [Sure](https://github.com/we-promise/sure) instance.

---

## 1. Intent

### The problem

Sure (`sure.jevy.org`) holds 8,576 transactions. 7,475 are categorized, all of
them inherited from a one-off Tiller CSV import on 2026-09-10. Of the 911
transactions that arrived via Sure's own SimpleFIN connection, **zero are
categorized**, and nothing in Sure will ever categorize them well:

- `Family#data_enrichment_enabled = false` (auto-categorization is off)
- `Rule.count = 0` (no rules exist)
- Sure's `Family::AutoCategorizer` is a single zero-shot LLM call over
  `{id, amount, classification, description, merchant}` against a flat list of
  category names. No history, no tools, no retrieval.

The bookkeeper agent already solves this problem well.

### The outcome

Every categorization the bookkeeper agent makes also lands in Sure, so Sure
becomes a useful UI over a correctly-categorized ledger without owning any
categorization logic. Corrections made by email reply flow through the same
path, so Sure tracks corrections too.

### Decisions taken (confirmed with Jevin, 2026-09-25)

| Decision | Choice |
|---|---|
| Transaction source | **The Google Sheet, always.** Not optional. It owns transaction IDs, feeds the producer and the categorizer's `sheet_lookup` tool, and anchors the correction loop. |
| Write targets | **Two sink modules, each optional:** `SheetsSink` and `SureSink`. Same consumer skeleton, separate processes and consumer groups. |
| Sure's role | **Sink only.** Nothing reads from Sure into the pipeline. |
| Corrections | Email-reply corrections republish through `transactions.categorized` and therefore reach both sinks. Desired. |
| Match failure | **DLQ topic**, mirroring `transactions.write-failed`. |
| Sure data gap | Sure has no data for 2025-01 through 2026-04. Events in that range will DLQ. Jevin will import the gap into Sure later; the DLQ is then replayed. **No cutoff date in the writer.** |
| Backfill | **Free**, falls out of consumer-group replay. See §4. |
| Multi-tenant | Not a driver. Homelab, single user. Config is environment variables. |

### Explicit non-goals

- Sure does not become a transaction source.
- The Sheets path's observable behaviour does not change: same writes, same
  tombstones, same DLQ topic, same metric names. Its internals move into the
  shared skeleton (§2), which is a refactor, not a behaviour change.
- The digest and email-reply loop stays pointed at the Sheet.
- No changes to the categorizer, its prompt, or its tools.
- Transactions present in Sure but absent from the Sheet stay uncategorized.

### Success criteria

1. A transaction categorized by the agent appears with that category in Sure
   within one polling interval, without manual action.
2. On first deploy, the existing ~925 categorized events replay into Sure and
   the uncategorized count drops materially from 911.
3. A categorization that cannot be matched to a Sure transaction is visible in
   a DLQ topic and in Grafana, never silently dropped.
4. A subsequent SimpleFIN sync does not overwrite a category the agent wrote.
5. A Sure outage at any point, startup or mid-replay, never drains the topic
   into the DLQ. The pod restarts and resumes where it left off.
6. `CategoryWriterTest` passes unchanged against the Sheets sink, and Sheets
   writer metrics are unchanged in Grafana throughout the rollout.

---

## 2. Architecture

```
                        transactions.categorized
                        (compact, keyed by transaction_id)
                                   │
                    ┌──────────────┴──────────────┐
                    │                             │
            group: category-writer        group: sure-writer   ← NEW
                    │                             │
               SinkWriter                    SinkWriter
              (shared loop)                 (shared loop)
                    │                             │
               SheetsSink                     SureSink
                    │                             │
             Google Sheets                   Sure REST API
             (source of truth)               PATCH /api/v1/transactions/:id
                    │                             │
   on success: tombstone →               on unmatched / 4xx ↓
   transactions.uncategorized            transactions.sure-write-failed
   on failure: DLQ →                     on 5xx / timeout: exit loop,
   transactions.write-failed             liveness restarts pod
```

### The shared skeleton

`CategoryWriter.run` today owns polling, commit, DLQ routing, metrics, and
health callbacks, tangled with Sheets logic. Extract that loop into
`SinkWriter`, parameterised by a `CategorySink`:

```kotlin
interface CategorySink {
    val name: String                       // "sheets" | "sure"; metric prefix
    val consumerGroup: String              // "category-writer" | "sure-writer"
    val dlqTopic: String
    fun write(tx: Transaction): SinkResult
}

sealed interface SinkResult {
    object Written : SinkResult
    data class Skipped(val reason: String) : SinkResult
    data class Rejected(val reason: String, val cause: Throwable? = null) : SinkResult  // → DLQ, continue
    data class Unavailable(val cause: Throwable) : SinkResult                            // → exit loop, no commit
}
```

`SinkWriter` per record:

| Result | Action |
|---|---|
| `Written` | `onSuccess(tx)` hook, increment `written`, commit |
| `Skipped` | increment `skipped{reason}`, commit |
| `Rejected` | send to `sink.dlqTopic`, `onSuccess(tx)` hook **only for Sheets** (see below), increment `errors`, commit |
| `Unavailable` | do **not** commit, `onAlive(false)`, rethrow. Pod restarts and resumes from the last committed offset. |

The `onSuccess` hook is how tombstoning stays a Sheets-only concern.
`SheetsSink` is constructed with a hook that tombstones
`transactions.uncategorized`; `SureSink` gets a no-op. Today `CategoryWriter`
tombstones on failure as well as success (the transaction is done from the
pipeline's point of view once it is in the DLQ), so the Sheets hook is invoked
for both `Written` and `Rejected`. The Sure sink never writes to
`transactions.uncategorized`.

**Why still two processes and two consumer groups:** the sinks must fail
independently. A Sure outage must not stall Sheet writes, must not produce
spurious `transactions.write-failed` records, and must not block tombstones.
Separate groups also give the Sure path its own offsets, which is what makes
replay-as-backfill work (§4). "Module" means a code unit, not one loop writing
to both targets.

**Why not a sink connector:** the matching logic in §5 is application logic
with a fallback ladder, not a field mapping.

### Package layout

```
src/main/kotlin/org/jevy/bookkeeper/writer/
    SinkWriter.kt          the shared consumer loop (extracted from CategoryWriter.run)
    CategorySink.kt        interface + SinkResult
    CategoryWriter.kt      becomes a thin factory: SinkWriter(SheetsSink(...), tombstoneHook)
    sheets/SheetsSink.kt   writeCategory + findRow, moved verbatim from CategoryWriter
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

| Variable | Source | Required | Notes |
|---|---|---|---|
| `SURE_API_URL` | plain value | for `sure-writer` | `http://sure-web.apps.svc.cluster.local:3000`. In-cluster service, bypasses Authentik forward-auth. |
| `SURE_API_KEY` | `secretKeyRef: sure-writer` | for `sure-writer` | Sure `ApiKey`, read+write. |
| `SURE_ENABLED` | plain value | no, default `true` | Kill switch. When false the process consumes and commits without matching or writing. Nothing is lost: compaction keeps the latest event per key, and a consumer group reset replays it. |
| `SURE_MAX_API_CALLS_PER_SEC` | plain value | no, default `5` | Throttles **all** Sure API calls, GET and PATCH. Matching costs one to three GETs per record, so throttling only writes would not protect Sure during replay. |
| `SURE_DRY_RUN` | plain value | no, default `false` | Match and resolve, log the intended PATCH, perform none. Commits offsets. |
| `SURE_ACCOUNT_MAP` | plain value | for `sure-writer` | `"Sheet account name=sure-account-uuid;..."`. Sure account **IDs**, not names, written after the duplicate-account merge (§5). |

`SHEETS_ENABLED` is deliberately absent. The Sheets sink is toggled by
deploying or not deploying the `writer` process, which is already the case.

---

## 4. Backfill via replay

`transactions.categorized` is `cleanup.policy=compact` and all three partitions
report `LOG-START-OFFSET=0`. The log holds the latest categorization for every
transaction the agent has ever categorized, about 925 as of 2026-09-25.

A new consumer group with `auto.offset.reset=earliest` (already the
`KafkaFactory` default) reads that entire history on first start. **This is
the backfill.** No separate job.

Consequences:

1. **Burst.** Throttled by `SURE_MAX_API_CALLS_PER_SEC`. At 5/s and roughly
   two calls per record, the replay takes about six minutes. Sure runs on
   `optiplex-tower`, the node with the least headroom.
2. **Coverage.** Replay only covers transactions that passed through the
   Sheet. Events dated in Sure's data gap (2025-01 through 2026-04) will not
   match and will DLQ. That is expected. After Jevin imports the gap into Sure,
   the DLQ is replayed (§7) and they match.
3. **Poll settings.** `KafkaFactory.createConsumer` sets
   `max.poll.records=1` and `max.poll.interval.ms=360000`. Bounded retries in
   `SureClient` must complete well inside six minutes per record.

### Required guard

`TopicInitializer` sets `deleteConfigs = listOf(RETENTION_MS_CONFIG)` on every
compacted topic except `CATEGORIZED`, which is left with the broker default
`retention.ms`. Compact-only topics ignore `retention.ms` today, but the whole
backfill property rests on that. **Add `deleteConfigs =
listOf(TopicConfig.RETENTION_MS_CONFIG)` to the `CATEGORIZED` TopicSpec** with a
comment that replay-from-zero is load-bearing.

---

## 5. Matching

`Transaction.transaction_id` is a Yodlee/Tiller identifier from the Sheet.
Sure's transactions carry SimpleFIN ids (`TRN-...`, exposed as `external_id`)
or import-assigned ids. They share no key.

`TransactionMatcher` is an explicit ladder, stopping at the first unambiguous
hit. Each rung is recorded as a metric tag.

| Rung | Strategy | Notes |
|---|---|---|
| 1 | **In-memory cache** | `transaction_id → sure_transaction_id`, process lifetime only. Saves API calls on redelivery. **Not persisted.** Sure cannot hold it (`external_id` is create-only and SimpleFIN rows own theirs), the PATCH is idempotent, and rung 2 is a single GET, so durability buys nothing. |
| 2 | **Exact triple** | `GET /api/v1/transactions?account_id=&start_date=&end_date=&min_amount=&max_amount=&per_page=100` with date and amount both pinned. One result → match. |
| 3 | **Triple with date window** | Same, ±3 days, for posting-date drift between Tiller and SimpleFIN. One result → match. |
| 4 | **Triple + description similarity** | If rungs 2–3 return several candidates, disambiguate on normalized description. Require a clear winner. |
| — | **No match, or still ambiguous** | `Rejected` → DLQ. Never guess. |

Verified against the running `sure-web` pod: the index supports `account_id`,
`start_date`, `end_date`, `min_amount`, `max_amount`, `search`, and `per_page`
up to 100. The response includes each transaction's current `category` and
`external_id`, so the rung 2 query also serves the read-before-write check in
§6 at no extra cost.

### Amount and account normalization

- **Amount** arrives as a display string (`"-$384.91"`). Parse to a signed
  decimal. **Sure's sign is inverted relative to the Sheet's**, verified on
  live data:

  | | Expense | Income |
  |---|---|---|
  | Sheet / Avro `amount` | `-$384.91` | `$1,371.24` |
  | Sure `entry.amount` | `140.82` | `-1371.24` |

  `sure_amount = -sheet_amount`. One function in `AmountConvention.kt`, a
  test per direction. An inverted comparison fails silently by matching
  nothing, so this must not be scattered.
- **Account.** Sure holds four duplicate account pairs (Tiller import and
  SimpleFIN each created e.g. `TD ALL-INCLUSIVE BANKING PLAN (6404)`). Tiller-era
  transactions live in one half, SimpleFIN-era in the other, so a name-based
  map that prefers one half is wrong for the other era. Therefore:

  > **Prerequisite:** merge the four pairs in Sure first. Then write
  > `SURE_ACCOUNT_MAP` against the surviving account **IDs**. Not part of this
  > spec's code, but the rollout (§11) blocks on it.

---

## 6. Writing

`SureSink.write` in order:

1. Match (§5). Unmatched → `Rejected("no_match")` or `Rejected("ambiguous")`.
2. Resolve category name → id. Unresolved → `Rejected("category_unresolved")`.
3. **Read-before-write.** If the matched Sure transaction's `category.id`
   already equals the resolved id → `Skipped("already_categorized")`. The Sheets
   sink has the same check. Without it a replay re-PATCHes every row and
   re-stamps `user_modified` for nothing.
4. PATCH:

```http
PATCH /api/v1/transactions/:id
{ "transaction": { "category_id": "<uuid>", "user_modified": true } }
```

Both fields are permitted (`entry_params_for_update`, `transaction_params`).
`user_modified: true` calls `mark_user_modified!`, which makes
`Entry#protected_from_sync?` true, so the next SimpleFIN sync links its
`external_id` rather than overwriting the category. Criterion 4. The update
path also calls `lock_saved_attributes!`, protecting the write from Sure's own
enrichment.

### Category resolution

`CategoryResolver` fetches `GET /api/v1/categories` once, caches by
case-folded name, and refreshes on a miss.

Sure's taxonomy already matches the Sheet's 40 Tiller categories. No mapping
table.

Hazards:

- **Case-duplicate rows:** `groceries` and `Groceries`, `restaurants` and
  `Restaurants` are separate records. When a folded name resolves to more than
  one id → `Rejected("category_ambiguous")`. Silently splitting a category
  across two ids corrupts reporting.
- **Unknown name** → `Rejected("category_unresolved")`. Do not auto-create
  categories. Silent taxonomy drift is worse than a visible failure.

---

## 7. Failure handling

| Condition | `SinkResult` | Effect |
|---|---|---|
| No match / ambiguous | `Rejected` | DLQ, continue, commit |
| Unresolvable or ambiguous category | `Rejected` | DLQ, continue, commit |
| Sure 4xx | `Rejected` | DLQ, continue, commit |
| Sure 5xx, timeout, connection refused, at startup **or mid-run** | `Unavailable` after bounded retry (3 attempts, exponential backoff, total under 60 s) | **No commit.** Loop exits, liveness fails, pod restarts, resumes at last committed offset. |
| `SURE_ENABLED=false` | `Skipped("disabled")` | commit |
| `SURE_DRY_RUN=true` | `Skipped("dry_run")` after logging the intended PATCH | commit |

The `Unavailable` row is the change from the first draft. Previously only a
startup outage was protected; a mid-replay outage would have converted the
rest of the backlog into DLQ records at throttle speed. Distinguishing "Sure is
down" from "this transaction cannot be matched" is the sink's job, and it is
expressed in the result type rather than in the loop.

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
bookkeeper-agent dlq-replay sure       # new: sure-write-failed → categorized, unchanged
```

The Sure mode tombstones the DLQ entry and republishes the record **as-is** to
`transactions.categorized`. Both sinks see it again. The Sheets sink skips
(category already matches, one Sheets read), the Sure sink retries the match.
This is the path for the data-gap events once the gap is imported.

**No tombstones.** The Sure sink never writes to `transactions.uncategorized`.

---

## 8. Observability

Metrics under `bookkeeper.sure.*`, parallel to `bookkeeper.writer.*`. The
Sheets sink keeps its exact existing metric names; `SinkWriter` takes the
prefix from `CategorySink.name` and the Sheets prefix stays `writer`.

| Metric | Type | Tags |
|---|---|---|
| `bookkeeper.sure.transactions.written` | counter | `match_rung` |
| `bookkeeper.sure.transactions.skipped` | counter | `reason` |
| `bookkeeper.sure.transactions.rejected` | counter | `reason` |
| `bookkeeper.sure.errors` | counter | `kind` |
| `bookkeeper.sure.duration` | timer | — |
| `bookkeeper.sure.api.duration` | timer | `endpoint`, `status` |

`match_rung` is the one to watch: steady state should sit on rungs 1 and 2. A
drift toward 3–4 means the account map or the amount convention needs
attention.

`PrometheusRule` additions alongside `k8s/app/prometheusrule.yaml`:

- `SureWriterRejectRateHigh`: `rejected` > 20% of processed over 1h. **Will
  fire during the initial replay** because of the data gap. Silence it for the
  replay window rather than weakening the rule; after the gap import it is a
  real signal.
- `SureWriterStalled`: no activity for 24h while the Sheets writer is active.
- `SureWriterDLQGrowing`: `sure-write-failed` compacted size increasing over 6h.

---

## 9. Out of scope, worth tracking separately

Found while investigating. File each as a `bd` issue.

1. **Sure's SimpleFIN feed is stale.** Nothing newer than 2026-09-14; TD stops
   at 2026-09-08. Sure persists everything SimpleFIN returns, so the staleness
   is upstream at MX. Needs re-authentication.
2. **Four duplicate account pairs.** Prerequisite to §5 and §11.
3. **Two credit cards typed `Depository`** (`TD AEROPLAN VISA`,
   `TD REWARDS VISA`), so net worth is wrong.
4. **Data gap 2025-01 through 2026-04.** The Sheet holds 18,898 rows against
   7,665 imported. Jevin will import; then `dlq-replay sure`.
5. **Case-duplicate categories** should be merged in Sure.
6. **Future option, not planned:** Sure's create endpoint has an idempotent
   `source` + `external_id` path. A bookkeeper-owned ledger in Sure keyed by our
   `transaction_id` would remove matching entirely. That is the route if the
   Sheet ever stops being the source. Recorded here so nobody rediscovers it.

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
  `onAlive(false)`; the success hook fires for `Written` and `Rejected`, never
  for `Skipped` or `Unavailable`.
- `TransactionMatcher`: one test per rung; ambiguity yields no match; the
  ±3-day window includes boundaries and excludes day 4.
- `AmountConvention`: `"-$384.91"`, `"$1,865.61"`, `"$0.00"`, malformed input;
  the sign flip in both directions.
- `CategoryResolver`: case-insensitive hit; duplicate folded name rejects;
  unknown name rejects.
- Account map parsing and the ID-based lookup.
- `SureSink`: already-categorized → `Skipped`; unmatched → `Rejected`;
  5xx after retries → `Unavailable`; dry run → `Skipped` with zero PATCHes.

**Integration (broker-gated, stubbed Sure)**
- A categorized event produces exactly one PATCH.
- Match failure produces one DLQ record and no PATCH.
- Sure unreachable mid-stream: loop exits, no offset commit, no DLQ records.
- `SURE_DRY_RUN=true` performs zero writes and still commits offsets.
- Replay from offset 0 respects `SURE_MAX_API_CALLS_PER_SEC` counting GETs.
- `dlq-replay sure` tombstones the DLQ entry and republishes to `categorized`
  unchanged.

**Manual gate before enabling writes**
Deploy with `SURE_DRY_RUN=true`, let the full replay run, review the rung
distribution and would-be reject reasons.

---

## 11. Rollout

1. **Refactor only.** Extract `SinkWriter` and `SheetsSink`. Ship. Confirm
   `bookkeeper.writer.*` metrics and behaviour are unchanged for a day.
2. Merge the four duplicate account pairs in Sure. Fix the two miscategorized
   credit cards. Record the surviving account IDs in `SURE_ACCOUNT_MAP`.
3. Add the `sure-write-failed` topic and the `CATEGORIZED` guard via `init`.
4. Deploy `sure-writer` with `SURE_ENABLED=true`, `SURE_DRY_RUN=true`. Inspect
   rung distribution and reject reasons across the full replay.
5. Fix the account map until rung 2 dominates for post-2026-05 events.
6. Set `SURE_DRY_RUN=false`. Reset the `sure-writer` consumer group to
   earliest to re-replay.
7. Confirm Sure's uncategorized count drops from 911 and Sheets writer metrics
   are unchanged throughout.
8. Later, after the data-gap import: `dlq-replay sure`.

Rollback is `SURE_ENABLED=false`, or scaling `sure-writer` to zero. Nothing in
the Sheets path depends on the Sure process.
