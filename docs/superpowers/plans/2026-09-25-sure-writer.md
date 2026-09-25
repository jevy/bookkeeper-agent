# Sure Writer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Restructure the writer into a shared consumer loop with two independent sink modules (Sheets, Sure), then add the Sure sink so every categorization also lands in the self-hosted Sure instance.

**Architecture:** `SinkWriter` owns the Kafka consume/commit/DLQ loop and is parameterised by a `CategorySink`. `SheetsSink` is the existing Sheets logic moved out of `CategoryWriter` unchanged. `SureSink` matches a Sheet transaction to a Sure transaction by account, date and amount, resolves the category name to a Sure id, and PATCHes it. Each sink runs as its own process with its own consumer group, so they fail independently.

**Tech Stack:** Kotlin 2.1 / JVM 21, Gradle, kafka-clients 3.9 with Confluent Avro serde, OkHttp 4.12, Gson 2.12, Micrometer, JUnit 5, MockK 1.13, OkHttp MockWebServer (new test dependency).

**Spec:** `docs/superpowers/specs/2026-09-25-sure-writer-design.md`

## Global Constraints

- The Google Sheet stays mandatory. `AppConfig.fromEnv()` keeps requiring `GOOGLE_SHEET_ID` and `GOOGLE_CREDENTIALS_JSON`.
- The Sheets writer keeps its command name `writer`, consumer group `category-writer`, DLQ topic `transactions.write-failed`, and metric prefix `bookkeeper.writer.`.
- `src/test/kotlin/org/jevy/bookkeeper/writer/CategoryWriterTest.kt` passes with **no assertion changes**. Test-only shims in `CategoryWriter` are allowed.
- The Sure sink never produces to `transactions.uncategorized`.
- The Sure sink never creates categories in Sure.
- Sure 5xx, timeout or connection failure must never result in a DLQ record or an offset commit.
- All Sure API calls (GET and PATCH) are rate limited by `SURE_MAX_API_CALLS_PER_SEC`, default `5`.
- Bounded Sure retries: 3 attempts, backoff 1s, 2s, total well under the 360 s `max.poll.interval.ms`.
- Compare Sure amounts on `signed_amount_cents` (income positive, expense negative, same as the Sheet). The sign flip applies only to `min_amount`/`max_amount` query parameters.
- Do not send `user_modified` on PATCH. Protection from sync comes from Sure's `lock_saved_attributes!` on update.
- New topic `transactions.sure-write-failed`: `cleanup.policy=compact`, `delete.retention.ms=86400000`, `retention.ms` in `deleteConfigs`.
- No em-dashes in any prose, comments, or commit messages.
- Run tests with `./gradlew test --console=plain`. Run one class with `./gradlew test --tests 'org.jevy.bookkeeper.sure.AmountConventionTest' --console=plain`.

## Review Focus

Inputs the spec implies but that are easy to get wrong. Each has a pinning test in the task named.

1. **Sheet amount `"$0.00"` or an amount with no `$`** (Task 5): parsing must return zero and plain decimals rather than throwing, and a malformed string like `"abc"` must throw, never silently match nothing.
2. **A Sure transaction already carrying the same category** (Task 9): must be `Skipped("already_categorized")` with zero PATCH calls, or every replay rewrites every row Sure already has right.
3. **Sure returning more than 100 matches for one query** (Task 6): the client must follow `pagination.total_pages`, or rung 2 could miss the true match and fall through to a wrong rung-3 candidate.
4. **A transaction whose account name is not in `SURE_ACCOUNT_MAP`** (Task 8): must be `Rejected("account_unmapped")`, not an exception that the loop treats as fatal.
5. **Connection refused on the very first Sure call after startup** (Task 9): must surface as `Unavailable`, and `SinkWriter` must then exit without committing (Task 1 pins the loop side).

---

## File structure

| File | Responsibility |
|---|---|
| `src/main/kotlin/org/jevy/bookkeeper/writer/CategorySink.kt` | `CategorySink` interface and `SinkResult` sealed type |
| `src/main/kotlin/org/jevy/bookkeeper/writer/SinkWriter.kt` | Shared consumer loop: poll, dispatch on `SinkResult`, DLQ, optional tombstone, commit, metrics, health |
| `src/main/kotlin/org/jevy/bookkeeper/writer/SheetsSink.kt` | Sheets logic moved from `CategoryWriter` (`writeCategory`, `findRow`) |
| `src/main/kotlin/org/jevy/bookkeeper/writer/CategoryWriter.kt` | Thin wrapper: `SinkWriter(SheetsSink)` with tombstoning on. Keeps `run`, `writeCategory`, `findRow` for the existing test |
| `src/main/kotlin/org/jevy/bookkeeper/sure/AmountConvention.kt` | Parse Sheet amount strings, convert to cents, flip sign for Sure query params |
| `src/main/kotlin/org/jevy/bookkeeper/sure/SureClient.kt` | HTTP to Sure: auth header, rate limit, retry, pagination, three endpoints |
| `src/main/kotlin/org/jevy/bookkeeper/sure/CategoryResolver.kt` | Sure category name to id, case-insensitive, cached, ambiguity detection |
| `src/main/kotlin/org/jevy/bookkeeper/sure/TransactionMatcher.kt` | The rung ladder |
| `src/main/kotlin/org/jevy/bookkeeper/sure/SureSink.kt` | Implements `CategorySink`: match, resolve, read-before-write, PATCH |
| `src/main/kotlin/org/jevy/bookkeeper/config/AppConfig.kt` | New `sure*` fields and `SURE_ACCOUNT_MAP` parsing |
| `src/main/kotlin/org/jevy/bookkeeper/kafka/TopicNames.kt`, `TopicInitializer.kt` | New DLQ topic, `CATEGORIZED` retention guard |
| `src/main/kotlin/org/jevy/bookkeeper/replay/DlqReplayer.kt` | `sure` mode: `sure-write-failed` back to `categorized` unchanged |
| `src/main/kotlin/org/jevy/bookkeeper/Main.kt` | `sure-writer` command, `dlq-replay sure` |
| `k8s/app/deployment-sure-writer.yaml`, `kustomization.yaml`, `prometheusrule.yaml` | Deployment and alerts |
| `pipelines/sure-write-failed-view.typestream.json` | DLQ materialized view |
| `README.md` | Architecture diagram and service list |

---

### Task 1: `CategorySink`, `SinkResult`, and `SinkWriter`

**Files:**
- Create: `src/main/kotlin/org/jevy/bookkeeper/writer/CategorySink.kt`
- Create: `src/main/kotlin/org/jevy/bookkeeper/writer/SinkWriter.kt`
- Test: `src/test/kotlin/org/jevy/bookkeeper/writer/SinkWriterTest.kt`

**Interfaces:**
- Produces:
  ```kotlin
  sealed interface SinkResult {
      object Written : SinkResult
      data class Skipped(val reason: String) : SinkResult
      data class Rejected(val reason: String, val cause: Throwable? = null) : SinkResult
      data class Unavailable(val cause: Throwable) : SinkResult
  }
  interface CategorySink {
      val name: String            // metric prefix segment: "writer" | "sure"
      val consumerGroup: String
      val dlqTopic: String
      fun write(tx: Transaction): SinkResult
  }
  class SinkUnavailableException(cause: Throwable) : RuntimeException("Sink unavailable", cause)
  class SinkWriter(
      config: AppConfig,
      sink: CategorySink,
      meterRegistry: MeterRegistry = SimpleMeterRegistry(),
      tombstoneUncategorized: Boolean = false,
  ) { fun run(onActivity: () -> Unit = {}, onAlive: (Boolean) -> Unit = {}) }
  ```

- [ ] **Step 1: Write the failing test**

Create `src/test/kotlin/org/jevy/bookkeeper/writer/SinkWriterTest.kt`:

```kotlin
package org.jevy.bookkeeper.writer

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.mockk.*
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.TopicPartition
import org.jevy.bookkeeper.config.AppConfig
import org.jevy.bookkeeper.kafka.KafkaFactory
import org.jevy.bookkeeper.kafka.TopicNames
import org.jevy.bookkeeper_agent.Transaction
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.Duration
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class SinkWriterTest {

    private val config = AppConfig(
        kafkaBootstrapServers = "localhost:9092",
        schemaRegistryUrl = "http://localhost:8081",
        googleSheetId = "test",
        googleCredentialsJson = "{}",
        openrouterApiKey = "",
        maxTransactionAgeDays = 365,
        maxTransactions = 0,
        additionalContextPrompt = null,
        model = "",
    )

    private val consumer = mockk<KafkaConsumer<String, Transaction>>(relaxed = true)
    private val dlqProducer = mockk<KafkaProducer<String, Transaction>>(relaxed = true)
    private val tombstoneProducer = mockk<KafkaProducer<String, ByteArray?>>(relaxed = true)
    private val registry = SimpleMeterRegistry()

    private class FakeSink(private val result: SinkResult) : CategorySink {
        override val name = "fake"
        override val consumerGroup = "fake-group"
        override val dlqTopic = "fake.dlq"
        val seen = mutableListOf<String>()
        override fun write(tx: Transaction): SinkResult {
            seen += tx.getTransactionId().toString()
            return result
        }
    }

    @BeforeEach
    fun setUp() {
        mockkObject(KafkaFactory)
        every { KafkaFactory.createConsumer(any(), any()) } returns consumer
        every { KafkaFactory.createProducer(any()) } returns dlqProducer
        every { KafkaFactory.createTombstoneProducer(any()) } returns tombstoneProducer
    }

    @AfterEach
    fun tearDown() = unmockkAll()

    private fun tx(id: String): Transaction = Transaction.newBuilder()
        .setTransactionId(id).setDate("1/1/2026").setDescription("T")
        .setCategory("Groceries").setAmount("-\$10.00").setAccount("Visa").build()

    /** First poll returns one record for [id]; second poll interrupts to stop the loop. */
    private fun pollOnce(id: String) {
        val tp = TopicPartition(TopicNames.CATEGORIZED, 0)
        val records = ConsumerRecords(mapOf(tp to listOf(ConsumerRecord(TopicNames.CATEGORIZED, 0, 0L, id, tx(id)))))
        var polls = 0
        every { consumer.poll(any<Duration>()) } answers {
            polls++
            if (polls == 1) records else throw InterruptedException("stop")
        }
    }

    private fun runUntilStopped(writer: SinkWriter, alive: MutableList<Boolean>) {
        try { writer.run(onAlive = { alive += it }) } catch (_: InterruptedException) {}
    }

    @Test
    fun `Written commits, increments written, and tombstones only when enabled`() {
        pollOnce("txn-1")
        val sink = FakeSink(SinkResult.Written)
        val alive = mutableListOf<Boolean>()

        runUntilStopped(SinkWriter(config, sink, registry, tombstoneUncategorized = true), alive)

        assertEquals(listOf("txn-1"), sink.seen)
        verify(exactly = 1) { consumer.commitSync() }
        verify { tombstoneProducer.send(match { it.topic() == TopicNames.UNCATEGORIZED && it.key() == "txn-1" && it.value() == null }) }
        verify(exactly = 0) { dlqProducer.send(any()) }
        assertEquals(1.0, registry.counter("bookkeeper.fake.transactions.written").count())
        assertEquals(listOf(true, false), alive)
    }

    @Test
    fun `Written with tombstoning off never touches the tombstone producer`() {
        pollOnce("txn-1")
        runUntilStopped(SinkWriter(config, FakeSink(SinkResult.Written), registry, tombstoneUncategorized = false), mutableListOf())
        verify(exactly = 0) { tombstoneProducer.send(any()) }
        verify(exactly = 0) { KafkaFactory.createTombstoneProducer(any()) }
    }

    @Test
    fun `Skipped commits and increments skipped with reason, no DLQ, no tombstone`() {
        pollOnce("txn-1")
        runUntilStopped(SinkWriter(config, FakeSink(SinkResult.Skipped("already_categorized")), registry, tombstoneUncategorized = true), mutableListOf())
        verify(exactly = 1) { consumer.commitSync() }
        verify(exactly = 0) { dlqProducer.send(any()) }
        verify(exactly = 0) { tombstoneProducer.send(any()) }
        assertEquals(1.0, registry.counter("bookkeeper.fake.transactions.skipped", "reason", "already_categorized").count())
    }

    @Test
    fun `Rejected sends to sink DLQ, commits, tombstones when enabled`() {
        pollOnce("txn-1")
        runUntilStopped(SinkWriter(config, FakeSink(SinkResult.Rejected("no_match")), registry, tombstoneUncategorized = true), mutableListOf())
        verify { dlqProducer.send(match { it.topic() == "fake.dlq" && it.key() == "txn-1" && it.value() != null }) }
        verify { tombstoneProducer.send(match { it.topic() == TopicNames.UNCATEGORIZED && it.key() == "txn-1" }) }
        verify(exactly = 1) { consumer.commitSync() }
        assertEquals(1.0, registry.counter("bookkeeper.fake.transactions.rejected", "reason", "no_match").count())
        assertEquals(1.0, registry.counter("bookkeeper.fake.errors").count())
    }

    @Test
    fun `Unavailable exits the loop without committing or DLQing and marks not alive`() {
        pollOnce("txn-1")
        val alive = mutableListOf<Boolean>()
        val writer = SinkWriter(config, FakeSink(SinkResult.Unavailable(RuntimeException("down"))), registry, tombstoneUncategorized = true)

        assertThrows<SinkUnavailableException> { writer.run(onAlive = { alive += it }) }

        verify(exactly = 0) { consumer.commitSync() }
        verify(exactly = 0) { dlqProducer.send(any()) }
        verify(exactly = 0) { tombstoneProducer.send(any()) }
        assertEquals(listOf(true, false), alive)
    }

    @Test
    fun `subscribes with the sink consumer group and categorized topic`() {
        pollOnce("txn-1")
        runUntilStopped(SinkWriter(config, FakeSink(SinkResult.Written), registry), mutableListOf())
        verify { KafkaFactory.createConsumer(config, "fake-group") }
        verify { consumer.subscribe(listOf(TopicNames.CATEGORIZED)) }
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew test --tests 'org.jevy.bookkeeper.writer.SinkWriterTest' --console=plain`
Expected: compilation FAILS with unresolved references `SinkResult`, `CategorySink`, `SinkWriter`.

- [ ] **Step 3: Write the interface**

Create `src/main/kotlin/org/jevy/bookkeeper/writer/CategorySink.kt`:

```kotlin
package org.jevy.bookkeeper.writer

import org.jevy.bookkeeper_agent.Transaction

/**
 * Outcome of one sink write. [SinkWriter] decides what to do with each variant:
 * Written and Skipped commit; Rejected goes to the sink's DLQ then commits;
 * Unavailable aborts the loop without committing so the pod restarts and resumes.
 */
sealed interface SinkResult {
    object Written : SinkResult
    data class Skipped(val reason: String) : SinkResult
    data class Rejected(val reason: String, val cause: Throwable? = null) : SinkResult
    data class Unavailable(val cause: Throwable) : SinkResult
}

interface CategorySink {
    /** Metric prefix segment, e.g. "writer" gives bookkeeper.writer.* */
    val name: String
    val consumerGroup: String
    val dlqTopic: String
    fun write(tx: Transaction): SinkResult
}

class SinkUnavailableException(cause: Throwable) : RuntimeException("Sink unavailable: ${cause.message}", cause)
```

- [ ] **Step 4: Write the loop**

Create `src/main/kotlin/org/jevy/bookkeeper/writer/SinkWriter.kt`:

```kotlin
package org.jevy.bookkeeper.writer

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.jevy.bookkeeper.config.AppConfig
import org.jevy.bookkeeper.kafka.KafkaFactory
import org.jevy.bookkeeper.kafka.TopicNames
import org.slf4j.LoggerFactory
import java.time.Duration

/**
 * Shared consumer loop for every category sink. Consumes transactions.categorized
 * with the sink's own consumer group and dispatches on [SinkResult].
 *
 * [tombstoneUncategorized] is true only for the Sheets sink: once the Sheet has
 * the category (or the record is in the DLQ), the transaction is done from the
 * pipeline's point of view and is removed from transactions.uncategorized.
 */
class SinkWriter(
    private val config: AppConfig,
    private val sink: CategorySink,
    private val meterRegistry: MeterRegistry = SimpleMeterRegistry(),
    private val tombstoneUncategorized: Boolean = false,
) {
    private val logger = LoggerFactory.getLogger("${SinkWriter::class.java.name}.${sink.name}")

    private val writtenCounter = meterRegistry.counter("bookkeeper.${sink.name}.transactions.written")
    private val errorsCounter = meterRegistry.counter("bookkeeper.${sink.name}.errors")
    private val durationTimer = meterRegistry.timer("bookkeeper.${sink.name}.duration")

    private fun skipped(reason: String) =
        meterRegistry.counter("bookkeeper.${sink.name}.transactions.skipped", "reason", reason).increment()

    private fun rejected(reason: String) =
        meterRegistry.counter("bookkeeper.${sink.name}.transactions.rejected", "reason", reason).increment()

    fun run(onActivity: () -> Unit = {}, onAlive: (Boolean) -> Unit = {}) {
        val consumer = KafkaFactory.createConsumer(config, sink.consumerGroup)
        val dlqProducer = KafkaFactory.createProducer(config)
        val tombstoneProducer: KafkaProducer<String, ByteArray?>? =
            if (tombstoneUncategorized) KafkaFactory.createTombstoneProducer(config) else null

        consumer.subscribe(listOf(TopicNames.CATEGORIZED))
        logger.info("Sink '{}' subscribed to {} as group {}", sink.name, TopicNames.CATEGORIZED, sink.consumerGroup)
        onAlive(true)

        try {
            while (true) {
                val records = consumer.poll(Duration.ofSeconds(5))
                onActivity()
                for (record in records) {
                    val transactionId = record.key()
                    val result = durationTimer.recordCallable { sink.write(record.value()) }!!
                    when (result) {
                        is SinkResult.Written -> {
                            writtenCounter.increment()
                            tombstoneProducer?.tombstone(transactionId)
                        }
                        is SinkResult.Skipped -> {
                            skipped(result.reason)
                            logger.info("Skipped transaction {} ({})", transactionId, result.reason)
                        }
                        is SinkResult.Rejected -> {
                            errorsCounter.increment()
                            rejected(result.reason)
                            logger.error("Rejected transaction {} ({}), sending to {}", transactionId, result.reason, sink.dlqTopic, result.cause)
                            dlqProducer.send(ProducerRecord(sink.dlqTopic, transactionId, record.value()))
                            tombstoneProducer?.tombstone(transactionId)
                        }
                        is SinkResult.Unavailable -> {
                            logger.error("Sink '{}' unavailable while handling {}; exiting without commit", sink.name, transactionId, result.cause)
                            throw SinkUnavailableException(result.cause)
                        }
                    }
                }
                consumer.commitSync()
            }
        } finally {
            onAlive(false)
            logger.error("Consumer loop exited, marking unhealthy")
        }
    }

    private fun KafkaProducer<String, ByteArray?>.tombstone(transactionId: String) {
        send(ProducerRecord(TopicNames.UNCATEGORIZED, transactionId, null))
        logger.debug("Tombstoned transaction {} from uncategorized", transactionId)
    }
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `./gradlew test --tests 'org.jevy.bookkeeper.writer.SinkWriterTest' --console=plain`
Expected: PASS, 6 tests.

- [ ] **Step 6: Commit**

```bash
git add src/main/kotlin/org/jevy/bookkeeper/writer/CategorySink.kt src/main/kotlin/org/jevy/bookkeeper/writer/SinkWriter.kt src/test/kotlin/org/jevy/bookkeeper/writer/SinkWriterTest.kt
git commit -m "Add CategorySink interface and shared SinkWriter loop"
```

---

### Task 2: Extract `SheetsSink`, make `CategoryWriter` a thin wrapper

**Files:**
- Create: `src/main/kotlin/org/jevy/bookkeeper/writer/SheetsSink.kt`
- Modify: `src/main/kotlin/org/jevy/bookkeeper/writer/CategoryWriter.kt` (whole file)
- Test: existing `src/test/kotlin/org/jevy/bookkeeper/writer/CategoryWriterTest.kt` (unchanged)

**Interfaces:**
- Consumes: `CategorySink`, `SinkResult`, `SinkWriter` from Task 1.
- Produces:
  ```kotlin
  class SheetsSink(config: AppConfig, sheetsClient: SheetsClient = SheetsClient(config)) : CategorySink
      internal fun writeCategory(tx: Transaction): SinkResult   // throws RowNotFoundException
      internal fun findRow(target: Transaction, rows: List<SheetTransaction>): Int?
  class CategoryWriter(config, sheetsClient = SheetsClient(config), meterRegistry = SimpleMeterRegistry())
      fun run(onActivity, onAlive)
      internal fun writeCategory(tx: Transaction)              // throws RowNotFoundException, for the existing test
      internal fun findRow(target, rows): Int?
  ```

- [ ] **Step 1: Confirm the existing test is green before touching anything**

Run: `./gradlew test --tests 'org.jevy.bookkeeper.writer.CategoryWriterTest' --console=plain`
Expected: PASS, 12 tests. This is the contract for the refactor.

- [ ] **Step 2: Create `SheetsSink`**

Create `src/main/kotlin/org/jevy/bookkeeper/writer/SheetsSink.kt`. The bodies of `writeCategory` and `findRow` are the current `CategoryWriter` bodies with three changes: counters are gone (the loop counts now), `writeCategory` returns a `SinkResult`, and the "no category" early return becomes `Skipped("no_category")`.

```kotlin
package org.jevy.bookkeeper.writer

import org.jevy.bookkeeper.DurableTransactionId
import org.jevy.bookkeeper.config.AppConfig
import org.jevy.bookkeeper.kafka.TopicNames
import org.jevy.bookkeeper.sheets.SheetTransaction
import org.jevy.bookkeeper.sheets.SheetsClient
import org.jevy.bookkeeper.sheets.TransactionMapper
import org.jevy.bookkeeper_agent.Transaction
import org.slf4j.LoggerFactory
import java.time.LocalDate
import java.time.format.DateTimeFormatter

class RowNotFoundException(message: String) : RuntimeException(message)

/** Writes categories back to the Google Sheet. The Sheet is the source of truth. */
class SheetsSink(
    private val config: AppConfig,
    private val sheetsClient: SheetsClient = SheetsClient(config),
) : CategorySink {

    private val logger = LoggerFactory.getLogger(SheetsSink::class.java)

    override val name = "writer"
    override val consumerGroup = "category-writer"
    override val dlqTopic = TopicNames.WRITE_FAILED

    // Resolve column letters from header row on first use
    private val columnLetters: Map<String, String> by lazy {
        val header = sheetsClient.readAllRows("Transactions!1:1").firstOrNull()?.map { it.toString() } ?: emptyList()
        header.withIndex().associate { (i, name) -> name to indexToColumnLetter(i) }.also {
            logger.info("Resolved column letters: Category={}, Transaction ID={}, Categorized Date={}",
                it["Category"], it["Transaction ID"], it["Categorized Date"])
        }
    }

    private fun indexToColumnLetter(index: Int): String {
        var result = ""
        var i = index
        while (i >= 0) {
            result = ('A' + i % 26) + result
            i = i / 26 - 1
        }
        return result
    }

    override fun write(tx: Transaction): SinkResult = try {
        writeCategory(tx)
    } catch (e: RowNotFoundException) {
        SinkResult.Rejected("row_not_found", e)
    } catch (e: Exception) {
        SinkResult.Rejected("exception", e)
    }

    /** Throws [RowNotFoundException] when the transaction has no row in the Sheet. */
    internal fun writeCategory(transaction: Transaction): SinkResult {
        val category = transaction.getCategory()?.toString()
        if (category.isNullOrBlank()) {
            logger.warn("Transaction {} has no category, skipping", transaction.getTransactionId())
            return SinkResult.Skipped("no_category")
        }

        val transactionId = transaction.getTransactionId().toString()

        val allRows = sheetsClient.readAllRows()
        if (allRows.isEmpty()) throw RowNotFoundException("No rows found for transaction $transactionId")

        val header = allRows.first().map { it.toString() }
        val colIndex = header.withIndex().associate { (i, name) -> name to i }
        val indexed = allRows.drop(1).mapIndexed { i, row ->
            SheetTransaction(i + 2, TransactionMapper.fromSheetRow(row, colIndex, config.googleSheetId))
        }

        val rowNumber = findRow(transaction, indexed)
            ?: throw RowNotFoundException("Could not find row for transaction $transactionId")

        val categoryCol = columnLetters["Category"] ?: "C"
        val categorizedDateCol = columnLetters["Categorized Date"] ?: "P"

        // Check if already categorized. Skip only if the existing category matches.
        val rows = sheetsClient.readAllRows("Transactions!${categoryCol}$rowNumber:${categoryCol}$rowNumber")
        val existing = rows.firstOrNull()?.firstOrNull()?.toString() ?: ""
        if (existing.isNotBlank() && existing == category) {
            logger.info("Transaction {} already has category '{}', skipping", transactionId, existing)
            return SinkResult.Skipped("already_categorized")
        }

        if (existing.isNotBlank() && existing != category) {
            logger.info("Transaction {} category changing from '{}' to '{}'", transactionId, existing, category)
        }

        sheetsClient.writeCell("Transactions!${categoryCol}$rowNumber", category)
        val categorizedDate = transaction.getCategorizationDate()?.toString()
            ?: LocalDate.now().format(DateTimeFormatter.ofPattern("M/d/yyyy"))
        sheetsClient.writeCell("Transactions!${categorizedDateCol}$rowNumber", categorizedDate)

        val note = transaction.getNote()?.toString()
        if (!note.isNullOrBlank()) {
            val noteCol = columnLetters["Note"] ?: "N"
            sheetsClient.writeCell("Transactions!${noteCol}$rowNumber", note)
            logger.info("Wrote note to row {} for transaction {}", rowNumber, transactionId)
        }

        logger.info("Wrote category '{}' to row {} for transaction {}", category, rowNumber, transactionId)
        return SinkResult.Written
    }

    internal fun findRow(target: Transaction, rows: List<SheetTransaction>): Int? {
        val targetId = target.getTransactionId().toString()
        val isDurable = targetId.startsWith("durable-")

        // Tier 1: match by Transaction ID attribute
        if (!isDurable) {
            rows.find { it.transaction.getTransactionId().toString() == targetId }
                ?.let { return it.rowNumber }
        }

        // Tier 2: match by content-based durable ID
        val expectedId = DurableTransactionId.generate(target)
        rows.find { DurableTransactionId.generate(it.transaction) == expectedId }
            ?.let { return it.rowNumber }

        return null
    }
}
```

- [ ] **Step 3: Replace `CategoryWriter` with the wrapper**

Overwrite `src/main/kotlin/org/jevy/bookkeeper/writer/CategoryWriter.kt`:

```kotlin
package org.jevy.bookkeeper.writer

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.jevy.bookkeeper.config.AppConfig
import org.jevy.bookkeeper.sheets.SheetTransaction
import org.jevy.bookkeeper.sheets.SheetsClient
import org.jevy.bookkeeper_agent.Transaction

/**
 * The Sheets writer process: [SinkWriter] over [SheetsSink] with tombstoning on.
 * Command name, consumer group and metric prefix are unchanged from before the
 * sink split. The internal delegates exist for CategoryWriterTest.
 */
class CategoryWriter(
    config: AppConfig,
    sheetsClient: SheetsClient = SheetsClient(config),
    meterRegistry: MeterRegistry = SimpleMeterRegistry(),
) {
    private val sink = SheetsSink(config, sheetsClient)
    private val writer = SinkWriter(config, sink, meterRegistry, tombstoneUncategorized = true)

    fun run(onActivity: () -> Unit = {}, onAlive: (Boolean) -> Unit = {}) = writer.run(onActivity, onAlive)

    /** Throws [RowNotFoundException] when no row matches. */
    internal fun writeCategory(transaction: Transaction) {
        sink.writeCategory(transaction)
    }

    internal fun findRow(target: Transaction, rows: List<SheetTransaction>): Int? = sink.findRow(target, rows)
}
```

- [ ] **Step 4: Run the untouched test**

Run: `./gradlew test --tests 'org.jevy.bookkeeper.writer.*' --console=plain`
Expected: PASS. `CategoryWriterTest` 12 tests, `SinkWriterTest` 6 tests. If `CategoryWriterTest` fails, the refactor is wrong; do not edit the test.

- [ ] **Step 5: Run the whole suite**

Run: `./gradlew test --console=plain`
Expected: PASS, 136 tests (130 baseline plus 6).

- [ ] **Step 6: Commit**

```bash
git add src/main/kotlin/org/jevy/bookkeeper/writer/
git commit -m "Extract SheetsSink; CategoryWriter becomes SinkWriter over SheetsSink"
```

---

### Task 3: New DLQ topic and the `CATEGORIZED` retention guard

**Files:**
- Modify: `src/main/kotlin/org/jevy/bookkeeper/kafka/TopicNames.kt`
- Modify: `src/main/kotlin/org/jevy/bookkeeper/kafka/TopicInitializer.kt:27-33` and the topic list
- Test: `src/test/kotlin/org/jevy/bookkeeper/kafka/TopicInitializerTest.kt`

**Interfaces:**
- Produces: `TopicNames.SURE_WRITE_FAILED = "transactions.sure-write-failed"`, `TopicInitializer.topics` visible as `internal`.

- [ ] **Step 1: Write the failing test**

Create `src/test/kotlin/org/jevy/bookkeeper/kafka/TopicInitializerTest.kt`:

```kotlin
package org.jevy.bookkeeper.kafka

import org.apache.kafka.common.config.TopicConfig
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class TopicInitializerTest {

    private fun spec(name: String): TopicSpec =
        assertNotNull(TopicInitializer.topics.find { it.name == name }, "missing TopicSpec for $name")

    @Test
    fun `categorized topic resets retention so replay-from-zero is guaranteed`() {
        val s = spec(TopicNames.CATEGORIZED)
        assertEquals(TopicConfig.CLEANUP_POLICY_COMPACT, s.config[TopicConfig.CLEANUP_POLICY_CONFIG])
        assertTrue(TopicConfig.RETENTION_MS_CONFIG in s.deleteConfigs)
    }

    @Test
    fun `sure-write-failed mirrors write-failed`() {
        val sure = spec(TopicNames.SURE_WRITE_FAILED)
        val sheets = spec(TopicNames.WRITE_FAILED)
        assertEquals("transactions.sure-write-failed", sure.name)
        assertEquals(sheets.config, sure.config)
        assertEquals(sheets.deleteConfigs, sure.deleteConfigs)
        assertEquals(sheets.partitions, sure.partitions)
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew test --tests 'org.jevy.bookkeeper.kafka.TopicInitializerTest' --console=plain`
Expected: compilation FAILS on `TopicNames.SURE_WRITE_FAILED` and `TopicInitializer.topics` (private).

- [ ] **Step 3: Implement**

In `TopicNames.kt` add after `WRITE_FAILED`:

```kotlin
    const val SURE_WRITE_FAILED = "transactions.sure-write-failed"
```

In `TopicInitializer.kt` change `private val topics` to `internal val topics`, replace the `CATEGORIZED` spec with:

```kotlin
        TopicSpec(
            // Replay-from-zero is load bearing: a new consumer group on this topic is how
            // a new sink backfills. Keep retention.ms at the broker default so the
            // compact-only policy is the only thing governing what is kept.
            name = TopicNames.CATEGORIZED,
            config = mapOf(TopicConfig.CLEANUP_POLICY_CONFIG to TopicConfig.CLEANUP_POLICY_COMPACT),
            deleteConfigs = listOf(TopicConfig.RETENTION_MS_CONFIG),
        ),
```

and add after the `WRITE_FAILED` spec:

```kotlin
        TopicSpec(
            name = TopicNames.SURE_WRITE_FAILED,
            config = mapOf(
                TopicConfig.CLEANUP_POLICY_CONFIG to TopicConfig.CLEANUP_POLICY_COMPACT,
                TopicConfig.DELETE_RETENTION_MS_CONFIG to "86400000", // tombstones retained 24h
            ),
            deleteConfigs = listOf(TopicConfig.RETENTION_MS_CONFIG),
        ),
```

- [ ] **Step 4: Run test to verify it passes**

Run: `./gradlew test --tests 'org.jevy.bookkeeper.kafka.TopicInitializerTest' --console=plain`
Expected: PASS, 2 tests.

- [ ] **Step 5: Commit**

```bash
git add src/main/kotlin/org/jevy/bookkeeper/kafka/ src/test/kotlin/org/jevy/bookkeeper/kafka/
git commit -m "Add sure-write-failed topic and guard categorized retention"
```

---

### Task 4: Sure configuration in `AppConfig`

**Files:**
- Modify: `src/main/kotlin/org/jevy/bookkeeper/config/AppConfig.kt`
- Test: `src/test/kotlin/org/jevy/bookkeeper/config/AppConfigTest.kt`

**Interfaces:**
- Produces on `AppConfig`:
  ```kotlin
  val sureApiUrl: String = ""
  val sureApiKey: String = ""
  val sureEnabled: Boolean = true
  val sureMaxApiCallsPerSec: Int = 5
  val sureDryRun: Boolean = false
  val sureAccountMap: Map<String, String> = emptyMap()   // Sheet account name -> Sure account id
  companion fun parseAccountMap(raw: String?): Map<String, String>
  ```

- [ ] **Step 1: Write the failing tests**

Append to `AppConfigTest`:

```kotlin
    @Test
    fun `sure fields default to disabled-safe values`() {
        val config = AppConfig(
            kafkaBootstrapServers = "localhost:9092",
            schemaRegistryUrl = "http://localhost:8081",
            googleSheetId = "sheet-123",
            googleCredentialsJson = "{}",
            openrouterApiKey = "",
            maxTransactionAgeDays = 365,
            maxTransactions = 0,
            additionalContextPrompt = null,
            model = "",
        )
        assertEquals("", config.sureApiUrl)
        assertEquals("", config.sureApiKey)
        assertEquals(true, config.sureEnabled)
        assertEquals(5, config.sureMaxApiCallsPerSec)
        assertEquals(false, config.sureDryRun)
        assertEquals(emptyMap(), config.sureAccountMap)
    }

    @Test
    fun `parseAccountMap splits name=id pairs on semicolons and trims`() {
        val map = AppConfig.parseAccountMap(" Chequing (1234) = 11111111-aaaa ; Visa=22222222-bbbb;")
        assertEquals(
            mapOf("Chequing (1234)" to "11111111-aaaa", "Visa" to "22222222-bbbb"),
            map,
        )
    }

    @Test
    fun `parseAccountMap of null or blank is empty`() {
        assertEquals(emptyMap(), AppConfig.parseAccountMap(null))
        assertEquals(emptyMap(), AppConfig.parseAccountMap("   "))
    }

    @Test
    fun `parseAccountMap rejects an entry without an equals sign`() {
        assertThrows<IllegalArgumentException> { AppConfig.parseAccountMap("Visa") }
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew test --tests 'org.jevy.bookkeeper.config.AppConfigTest' --console=plain`
Expected: compilation FAILS on `sureApiUrl` and `parseAccountMap`.

- [ ] **Step 3: Implement**

In `AppConfig.kt` add these constructor fields after `reportToAddresses`:

```kotlin
    val sureApiUrl: String = "",
    val sureApiKey: String = "",
    val sureEnabled: Boolean = true,
    val sureMaxApiCallsPerSec: Int = 5,
    val sureDryRun: Boolean = false,
    /** Sheet account name to Sure account id. Written after duplicate accounts are merged in Sure. */
    val sureAccountMap: Map<String, String> = emptyMap(),
```

In `fromEnv()` add after `reportToAddresses = ...`:

```kotlin
            sureApiUrl = System.getenv("SURE_API_URL")?.trimEnd('/') ?: "",
            sureApiKey = System.getenv("SURE_API_KEY") ?: "",
            sureEnabled = System.getenv("SURE_ENABLED")?.toBooleanStrictOrNull() ?: true,
            sureMaxApiCallsPerSec = System.getenv("SURE_MAX_API_CALLS_PER_SEC")?.toIntOrNull() ?: 5,
            sureDryRun = System.getenv("SURE_DRY_RUN")?.toBooleanStrictOrNull() ?: false,
            sureAccountMap = parseAccountMap(System.getenv("SURE_ACCOUNT_MAP")),
```

In the companion add:

```kotlin
        /** Parses "Sheet Account Name=sure-account-uuid;Other=uuid". Blank entries are ignored. */
        fun parseAccountMap(raw: String?): Map<String, String> {
            if (raw.isNullOrBlank()) return emptyMap()
            return raw.split(';')
                .map { it.trim() }
                .filter { it.isNotEmpty() }
                .associate { entry ->
                    val idx = entry.indexOf('=')
                    require(idx > 0) { "SURE_ACCOUNT_MAP entry '$entry' must be 'Sheet account name=sure account id'" }
                    entry.substring(0, idx).trim() to entry.substring(idx + 1).trim()
                }
        }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `./gradlew test --tests 'org.jevy.bookkeeper.config.AppConfigTest' --console=plain`
Expected: PASS, 6 tests.

- [ ] **Step 5: Commit**

```bash
git add src/main/kotlin/org/jevy/bookkeeper/config/AppConfig.kt src/test/kotlin/org/jevy/bookkeeper/config/AppConfigTest.kt
git commit -m "Add Sure configuration to AppConfig"
```

---

### Task 5: `AmountConvention`

**Files:**
- Create: `src/main/kotlin/org/jevy/bookkeeper/sure/AmountConvention.kt`
- Test: `src/test/kotlin/org/jevy/bookkeeper/sure/AmountConventionTest.kt`

**Interfaces:**
- Produces:
  ```kotlin
  object AmountConvention {
      fun parseSheetAmount(raw: String): BigDecimal          // "-$384.91" -> -384.91; throws IllegalArgumentException
      fun toCents(amount: BigDecimal): Long                  // -384.91 -> -38491
      fun toSureEntryAmount(sheetAmount: BigDecimal): BigDecimal   // negation, for min_amount/max_amount only
  }
  ```

- [ ] **Step 1: Write the failing test**

Create `src/test/kotlin/org/jevy/bookkeeper/sure/AmountConventionTest.kt`:

```kotlin
package org.jevy.bookkeeper.sure

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.math.BigDecimal
import kotlin.test.assertEquals

class AmountConventionTest {

    @Test
    fun `parses negative dollar expense`() {
        assertEquals(BigDecimal("-384.91"), AmountConvention.parseSheetAmount("-\$384.91"))
    }

    @Test
    fun `parses positive income with thousands separator`() {
        assertEquals(BigDecimal("1865.61"), AmountConvention.parseSheetAmount("\$1,865.61"))
    }

    @Test
    fun `parses zero`() {
        assertEquals(0, BigDecimal.ZERO.compareTo(AmountConvention.parseSheetAmount("\$0.00")))
    }

    @Test
    fun `parses a plain decimal without a dollar sign`() {
        assertEquals(BigDecimal("10"), AmountConvention.parseSheetAmount("10"))
    }

    @Test
    fun `parses with surrounding whitespace`() {
        assertEquals(BigDecimal("-12.50"), AmountConvention.parseSheetAmount("  -\$12.50 "))
    }

    @Test
    fun `malformed input throws rather than returning zero`() {
        assertThrows<IllegalArgumentException> { AmountConvention.parseSheetAmount("abc") }
        assertThrows<IllegalArgumentException> { AmountConvention.parseSheetAmount("") }
    }

    @Test
    fun `toCents keeps sign and rounds half up`() {
        assertEquals(-38491L, AmountConvention.toCents(BigDecimal("-384.91")))
        assertEquals(137124L, AmountConvention.toCents(BigDecimal("1371.24")))
        assertEquals(1000L, AmountConvention.toCents(BigDecimal("10")))
    }

    @Test
    fun `sure entry amount is the negation of the sheet amount in both directions`() {
        assertEquals(BigDecimal("384.91"), AmountConvention.toSureEntryAmount(BigDecimal("-384.91")))
        assertEquals(BigDecimal("-1371.24"), AmountConvention.toSureEntryAmount(BigDecimal("1371.24")))
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew test --tests 'org.jevy.bookkeeper.sure.AmountConventionTest' --console=plain`
Expected: compilation FAILS, `AmountConvention` unresolved.

- [ ] **Step 3: Implement**

Create `src/main/kotlin/org/jevy/bookkeeper/sure/AmountConvention.kt`:

```kotlin
package org.jevy.bookkeeper.sure

import java.math.BigDecimal
import java.math.RoundingMode

/**
 * The single place that knows how Sheet amounts and Sure amounts relate.
 *
 * Sheet / Avro: expense negative ("-$384.91"), income positive ("$1,371.24").
 * Sure API response `signed_amount_cents`: same sign convention as the Sheet.
 * Sure database column `entries.amount`, which the `min_amount`/`max_amount`
 * query parameters filter on: inverted (expense positive, income negative).
 *
 * So responses are compared with [toCents] directly, and only the query
 * parameters go through [toSureEntryAmount].
 */
object AmountConvention {

    fun parseSheetAmount(raw: String): BigDecimal {
        val cleaned = raw.trim().replace("$", "").replace(",", "")
        require(cleaned.isNotEmpty()) { "Blank amount" }
        return try {
            BigDecimal(cleaned)
        } catch (e: NumberFormatException) {
            throw IllegalArgumentException("Unparseable amount '$raw'", e)
        }
    }

    fun toCents(amount: BigDecimal): Long =
        amount.setScale(2, RoundingMode.HALF_UP).movePointRight(2).longValueExact()

    fun toSureEntryAmount(sheetAmount: BigDecimal): BigDecimal = sheetAmount.negate()
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `./gradlew test --tests 'org.jevy.bookkeeper.sure.AmountConventionTest' --console=plain`
Expected: PASS, 8 tests.

- [ ] **Step 5: Commit**

```bash
git add src/main/kotlin/org/jevy/bookkeeper/sure/AmountConvention.kt src/test/kotlin/org/jevy/bookkeeper/sure/AmountConventionTest.kt
git commit -m "Add AmountConvention for Sheet to Sure amount handling"
```

---

### Task 6: `SureClient`

**Files:**
- Modify: `build.gradle.kts` (add `testImplementation("com.squareup.okhttp3:mockwebserver:$okhttpVersion")`)
- Create: `src/main/kotlin/org/jevy/bookkeeper/sure/SureClient.kt`
- Test: `src/test/kotlin/org/jevy/bookkeeper/sure/SureClientTest.kt`

**Interfaces:**
- Consumes: `AmountConvention` (Task 5) is not used here; the client takes already-converted decimals.
- Produces:
  ```kotlin
  data class SureTransaction(val id: String, val date: LocalDate, val name: String, val signedAmountCents: Long,
                             val accountId: String, val categoryId: String?, val externalId: String?)
  data class SureCategory(val id: String, val name: String)
  class SureUnavailableException(message: String, cause: Throwable? = null) : RuntimeException
  class SureRequestException(val status: Int, val body: String) : RuntimeException
  class SureClient(baseUrl: String, apiKey: String, maxCallsPerSec: Int = 5, maxAttempts: Int = 3,
                   http: OkHttpClient = ..., sleeper: (Long) -> Unit = Thread::sleep,
                   meterRegistry: MeterRegistry = SimpleMeterRegistry()) {
      fun listTransactions(accountId: String, startDate: LocalDate, endDate: LocalDate,
                           minEntryAmount: BigDecimal, maxEntryAmount: BigDecimal): List<SureTransaction>
      fun listCategories(): List<SureCategory>
      fun updateCategory(transactionId: String, categoryId: String)
  }
  ```

- [ ] **Step 1: Add the test dependency**

In `build.gradle.kts` under `// Testing` add:

```kotlin
    testImplementation("com.squareup.okhttp3:mockwebserver:$okhttpVersion")
```

- [ ] **Step 2: Write the failing test**

Create `src/test/kotlin/org/jevy/bookkeeper/sure/SureClientTest.kt`:

```kotlin
package org.jevy.bookkeeper.sure

import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import okhttp3.mockwebserver.SocketPolicy
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.math.BigDecimal
import java.time.LocalDate
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

class SureClientTest {

    private lateinit var server: MockWebServer
    private val sleeps = mutableListOf<Long>()

    @BeforeEach
    fun setUp() {
        server = MockWebServer()
        server.start()
    }

    @AfterEach
    fun tearDown() = server.shutdown()

    private fun client(maxCallsPerSec: Int = 1000) = SureClient(
        baseUrl = server.url("/").toString().trimEnd('/'),
        apiKey = "secret",
        maxCallsPerSec = maxCallsPerSec,
        sleeper = { sleeps += it },
    )

    private fun txJson(id: String, cents: Long, category: String? = null, page: Int = 1, totalPages: Int = 1) = """
        {"transactions":[{"id":"$id","date":"2026-02-15","name":"COSTCO","signed_amount_cents":$cents,
          "external_id":"TRN-1","account":{"id":"acct-1","name":"Chequing"},
          "category":${if (category == null) "null" else """{"id":"cat-1","name":"$category"}"""}}],
         "pagination":{"page":$page,"per_page":100,"total_count":1,"total_pages":$totalPages}}
    """.trimIndent()

    @Test
    fun `listTransactions sends api key, filters, and parses the response`() {
        server.enqueue(MockResponse().setBody(txJson("t-1", -38491, "Groceries")))

        val result = client().listTransactions(
            accountId = "acct-1",
            startDate = LocalDate.of(2026, 2, 12),
            endDate = LocalDate.of(2026, 2, 18),
            minEntryAmount = BigDecimal("384.91"),
            maxEntryAmount = BigDecimal("384.91"),
        )

        val req = server.takeRequest()
        assertEquals("secret", req.getHeader("X-Api-Key"))
        assertEquals("/api/v1/transactions", req.requestUrl!!.encodedPath)
        assertEquals("acct-1", req.requestUrl!!.queryParameter("account_id"))
        assertEquals("2026-02-12", req.requestUrl!!.queryParameter("start_date"))
        assertEquals("2026-02-18", req.requestUrl!!.queryParameter("end_date"))
        assertEquals("384.91", req.requestUrl!!.queryParameter("min_amount"))
        assertEquals("384.91", req.requestUrl!!.queryParameter("max_amount"))
        assertEquals("100", req.requestUrl!!.queryParameter("per_page"))
        assertEquals("1", req.requestUrl!!.queryParameter("page"))

        assertEquals(1, result.size)
        val tx = result[0]
        assertEquals("t-1", tx.id)
        assertEquals(LocalDate.of(2026, 2, 15), tx.date)
        assertEquals("COSTCO", tx.name)
        assertEquals(-38491L, tx.signedAmountCents)
        assertEquals("acct-1", tx.accountId)
        assertEquals("cat-1", tx.categoryId)
        assertEquals("TRN-1", tx.externalId)
    }

    @Test
    fun `listTransactions follows pagination`() {
        server.enqueue(MockResponse().setBody(txJson("t-1", -100, page = 1, totalPages = 2)))
        server.enqueue(MockResponse().setBody(txJson("t-2", -100, page = 2, totalPages = 2)))

        val result = client().listTransactions("acct-1", LocalDate.of(2026, 1, 1), LocalDate.of(2026, 1, 1), BigDecimal("1.00"), BigDecimal("1.00"))

        assertEquals(listOf("t-1", "t-2"), result.map { it.id })
        server.takeRequest()
        assertEquals("2", server.takeRequest().requestUrl!!.queryParameter("page"))
    }

    @Test
    fun `null category parses as null categoryId`() {
        server.enqueue(MockResponse().setBody(txJson("t-1", -100, category = null)))
        val tx = client().listTransactions("acct-1", LocalDate.of(2026, 1, 1), LocalDate.of(2026, 1, 1), BigDecimal.ONE, BigDecimal.ONE).single()
        assertNull(tx.categoryId)
    }

    @Test
    fun `listCategories parses id and name`() {
        server.enqueue(MockResponse().setBody("""
            {"categories":[{"id":"c-1","name":"Groceries"},{"id":"c-2","name":"groceries"}],
             "pagination":{"page":1,"per_page":100,"total_count":2,"total_pages":1}}
        """.trimIndent()))
        val cats = client().listCategories()
        assertEquals(listOf(SureCategory("c-1", "Groceries"), SureCategory("c-2", "groceries")), cats)
        assertEquals("/api/v1/categories", server.takeRequest().requestUrl!!.encodedPath)
    }

    @Test
    fun `updateCategory PATCHes only category_id`() {
        server.enqueue(MockResponse().setBody("""{"id":"t-1"}"""))
        client().updateCategory("t-1", "c-1")
        val req = server.takeRequest()
        assertEquals("PATCH", req.method)
        assertEquals("/api/v1/transactions/t-1", req.requestUrl!!.encodedPath)
        assertEquals("secret", req.getHeader("X-Api-Key"))
        val body = req.body.readUtf8()
        assertEquals("""{"transaction":{"category_id":"c-1"}}""", body)
    }

    @Test
    fun `4xx raises SureRequestException without retry`() {
        server.enqueue(MockResponse().setResponseCode(422).setBody("""{"error":"validation_failed"}"""))
        val e = assertThrows<SureRequestException> { client().updateCategory("t-1", "c-1") }
        assertEquals(422, e.status)
        assertEquals(1, server.requestCount)
        assertTrue(sleeps.isEmpty())
    }

    @Test
    fun `5xx retries with backoff then raises SureUnavailableException`() {
        repeat(3) { server.enqueue(MockResponse().setResponseCode(503)) }
        assertThrows<SureUnavailableException> { client().listCategories() }
        assertEquals(3, server.requestCount)
        assertEquals(listOf(1000L, 2000L), sleeps)
    }

    @Test
    fun `5xx then success returns normally`() {
        server.enqueue(MockResponse().setResponseCode(500))
        server.enqueue(MockResponse().setBody("""{"categories":[],"pagination":{"page":1,"per_page":100,"total_count":0,"total_pages":1}}"""))
        assertEquals(emptyList(), client().listCategories())
        assertEquals(2, server.requestCount)
    }

    @Test
    fun `connection failure raises SureUnavailableException`() {
        repeat(3) { server.enqueue(MockResponse().setSocketPolicy(SocketPolicy.DISCONNECT_AT_START)) }
        assertThrows<SureUnavailableException> { client().listCategories() }
    }

    @Test
    fun `rate limiter spaces calls to at most maxCallsPerSec`() {
        repeat(3) { server.enqueue(MockResponse().setBody("""{"categories":[],"pagination":{"page":1,"per_page":100,"total_count":0,"total_pages":1}}""")) }
        val c = client(maxCallsPerSec = 2)  // 500 ms minimum spacing
        val t0 = System.nanoTime()
        repeat(3) { c.listCategories() }
        val elapsedMs = (System.nanoTime() - t0) / 1_000_000
        assertTrue(elapsedMs >= 950, "expected two 500 ms gaps, got ${elapsedMs}ms")
    }
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `./gradlew test --tests 'org.jevy.bookkeeper.sure.SureClientTest' --console=plain`
Expected: compilation FAILS, `SureClient` unresolved.

- [ ] **Step 4: Implement**

Create `src/main/kotlin/org/jevy/bookkeeper/sure/SureClient.kt`:

```kotlin
package org.jevy.bookkeeper.sure

import com.google.gson.JsonObject
import com.google.gson.JsonParser
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import okhttp3.HttpUrl.Companion.toHttpUrl
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import org.slf4j.LoggerFactory
import java.io.IOException
import java.math.BigDecimal
import java.time.LocalDate
import java.util.concurrent.TimeUnit

data class SureTransaction(
    val id: String,
    val date: LocalDate,
    val name: String,
    /** Income positive, expense negative. Same convention as the Sheet. */
    val signedAmountCents: Long,
    val accountId: String,
    val categoryId: String?,
    val externalId: String?,
)

data class SureCategory(val id: String, val name: String)

/** Sure could not be reached or answered 5xx after all retries. Never DLQ on this. */
class SureUnavailableException(message: String, cause: Throwable? = null) : RuntimeException(message, cause)

/** Sure answered 4xx. The request itself is wrong; safe to DLQ. */
class SureRequestException(val status: Int, val body: String) : RuntimeException("Sure responded $status: $body")

class SureClient(
    private val baseUrl: String,
    private val apiKey: String,
    private val maxCallsPerSec: Int = 5,
    private val maxAttempts: Int = 3,
    private val http: OkHttpClient = OkHttpClient.Builder()
        .connectTimeout(5, TimeUnit.SECONDS)
        .readTimeout(15, TimeUnit.SECONDS)
        .build(),
    private val sleeper: (Long) -> Unit = Thread::sleep,
    private val meterRegistry: MeterRegistry = SimpleMeterRegistry(),
) {
    private val logger = LoggerFactory.getLogger(SureClient::class.java)
    private val json = "application/json".toMediaType()
    private val minIntervalNanos = 1_000_000_000L / maxCallsPerSec.coerceAtLeast(1)
    private var lastCallNanos = 0L

    fun listTransactions(
        accountId: String,
        startDate: LocalDate,
        endDate: LocalDate,
        minEntryAmount: BigDecimal,
        maxEntryAmount: BigDecimal,
    ): List<SureTransaction> = paginate("transactions") { page ->
        "$baseUrl/api/v1/transactions".toHttpUrl().newBuilder()
            .addQueryParameter("account_id", accountId)
            .addQueryParameter("start_date", startDate.toString())
            .addQueryParameter("end_date", endDate.toString())
            .addQueryParameter("min_amount", minEntryAmount.toPlainString())
            .addQueryParameter("max_amount", maxEntryAmount.toPlainString())
            .addQueryParameter("per_page", "100")
            .addQueryParameter("page", page.toString())
            .build().toString()
    }.map { it.toSureTransaction() }

    fun listCategories(): List<SureCategory> = paginate("categories") { page ->
        "$baseUrl/api/v1/categories?per_page=100&page=$page"
    }.map { SureCategory(it["id"].asString, it["name"].asString) }

    fun updateCategory(transactionId: String, categoryId: String) {
        val body = JsonObject().apply {
            add("transaction", JsonObject().apply { addProperty("category_id", categoryId) })
        }
        val request = Request.Builder()
            .url("$baseUrl/api/v1/transactions/$transactionId")
            .patch(body.toString().toRequestBody(json))
            .build()
        execute(request, "PATCH /transactions/:id")
    }

    /** Fetches every page of a list endpoint, reading `pagination.total_pages`. */
    private fun paginate(arrayField: String, urlForPage: (Int) -> String): List<JsonObject> {
        val out = mutableListOf<JsonObject>()
        var page = 1
        while (true) {
            val response = execute(Request.Builder().url(urlForPage(page)).get().build(), "GET /$arrayField")
            response.getAsJsonArray(arrayField).forEach { out += it.asJsonObject }
            val totalPages = response.getAsJsonObject("pagination")?.get("total_pages")?.asInt ?: 1
            if (page >= totalPages) return out
            page++
        }
    }

    private fun execute(request: Request, endpoint: String): JsonObject {
        var attempt = 0
        while (true) {
            attempt++
            throttle()
            val sample = io.micrometer.core.instrument.Timer.start(meterRegistry)
            try {
                http.newCall(request.newBuilder().header("X-Api-Key", apiKey).header("Accept", "application/json").build())
                    .execute().use { resp ->
                        sample.stop(meterRegistry.timer("bookkeeper.sure.api.duration", "endpoint", endpoint, "status", resp.code.toString()))
                        val text = resp.body?.string() ?: ""
                        when {
                            resp.isSuccessful -> return if (text.isBlank()) JsonObject() else JsonParser.parseString(text).asJsonObject
                            resp.code in 500..599 -> throw IOException("Sure responded ${resp.code}")
                            else -> throw SureRequestException(resp.code, text)
                        }
                    }
            } catch (e: IOException) {
                if (attempt >= maxAttempts) {
                    throw SureUnavailableException("Sure unavailable after $attempt attempts on $endpoint: ${e.message}", e)
                }
                val backoffMs = 1000L shl (attempt - 1)   // 1000, 2000
                logger.warn("Sure call {} failed (attempt {}/{}): {}. Retrying in {} ms", endpoint, attempt, maxAttempts, e.message, backoffMs)
                sleeper(backoffMs)
            }
        }
    }

    @Synchronized
    private fun throttle() {
        val now = System.nanoTime()
        val wait = lastCallNanos + minIntervalNanos - now
        if (lastCallNanos != 0L && wait > 0) Thread.sleep(wait / 1_000_000, (wait % 1_000_000).toInt())
        lastCallNanos = System.nanoTime()
    }

    private fun JsonObject.toSureTransaction() = SureTransaction(
        id = get("id").asString,
        date = LocalDate.parse(get("date").asString),
        name = get("name")?.takeUnless { it.isJsonNull }?.asString ?: "",
        signedAmountCents = get("signed_amount_cents").asLong,
        accountId = getAsJsonObject("account").get("id").asString,
        categoryId = get("category")?.takeUnless { it.isJsonNull }?.asJsonObject?.get("id")?.asString,
        externalId = get("external_id")?.takeUnless { it.isJsonNull }?.asString,
    )
}
```

Note: the rate limiter uses real `Thread.sleep` on purpose (the test measures it), while retry backoff goes through `sleeper` so tests stay fast.

- [ ] **Step 5: Run test to verify it passes**

Run: `./gradlew test --tests 'org.jevy.bookkeeper.sure.SureClientTest' --console=plain`
Expected: PASS, 10 tests.

- [ ] **Step 6: Commit**

```bash
git add build.gradle.kts src/main/kotlin/org/jevy/bookkeeper/sure/SureClient.kt src/test/kotlin/org/jevy/bookkeeper/sure/SureClientTest.kt
git commit -m "Add SureClient with auth, rate limit, retry and pagination"
```

---

### Task 7: `CategoryResolver`

**Files:**
- Create: `src/main/kotlin/org/jevy/bookkeeper/sure/CategoryResolver.kt`
- Test: `src/test/kotlin/org/jevy/bookkeeper/sure/CategoryResolverTest.kt`

**Interfaces:**
- Consumes: `SureClient.listCategories()`, `SureCategory` (Task 6).
- Produces:
  ```kotlin
  sealed interface CategoryResolution {
      data class Resolved(val id: String) : CategoryResolution
      data class Ambiguous(val ids: List<String>) : CategoryResolution
      object Unknown : CategoryResolution
  }
  class CategoryResolver(client: SureClient) { fun resolve(name: String): CategoryResolution }
  ```

- [ ] **Step 1: Write the failing test**

Create `src/test/kotlin/org/jevy/bookkeeper/sure/CategoryResolverTest.kt`:

```kotlin
package org.jevy.bookkeeper.sure

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class CategoryResolverTest {

    private val client = mockk<SureClient>()

    @Test
    fun `resolves case-insensitively and trims`() {
        every { client.listCategories() } returns listOf(SureCategory("c-1", "Auto & Gas"))
        val r = CategoryResolver(client)
        assertEquals(CategoryResolution.Resolved("c-1"), r.resolve("auto & gas"))
        assertEquals(CategoryResolution.Resolved("c-1"), r.resolve("  Auto & Gas "))
    }

    @Test
    fun `caches after first fetch`() {
        every { client.listCategories() } returns listOf(SureCategory("c-1", "Groceries"))
        val r = CategoryResolver(client)
        r.resolve("Groceries")
        r.resolve("Groceries")
        verify(exactly = 1) { client.listCategories() }
    }

    @Test
    fun `duplicate folded names are ambiguous, never picked arbitrarily`() {
        every { client.listCategories() } returns listOf(SureCategory("c-1", "Groceries"), SureCategory("c-2", "groceries"))
        assertEquals(CategoryResolution.Ambiguous(listOf("c-1", "c-2")), CategoryResolver(client).resolve("Groceries"))
    }

    @Test
    fun `unknown name refreshes once then reports Unknown`() {
        every { client.listCategories() } returns listOf(SureCategory("c-1", "Groceries"))
        val r = CategoryResolver(client)
        assertEquals(CategoryResolution.Unknown, r.resolve("Not A Category"))
        verify(exactly = 2) { client.listCategories() }
    }

    @Test
    fun `a category added in Sure after startup is found on refresh`() {
        every { client.listCategories() } returnsMany listOf(
            listOf(SureCategory("c-1", "Groceries")),
            listOf(SureCategory("c-1", "Groceries"), SureCategory("c-9", "New Thing")),
        )
        assertEquals(CategoryResolution.Resolved("c-9"), CategoryResolver(client).resolve("New Thing"))
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew test --tests 'org.jevy.bookkeeper.sure.CategoryResolverTest' --console=plain`
Expected: compilation FAILS.

- [ ] **Step 3: Implement**

Create `src/main/kotlin/org/jevy/bookkeeper/sure/CategoryResolver.kt`:

```kotlin
package org.jevy.bookkeeper.sure

sealed interface CategoryResolution {
    data class Resolved(val id: String) : CategoryResolution
    /** More than one Sure category folds to the same name (e.g. "Groceries" and "groceries"). */
    data class Ambiguous(val ids: List<String>) : CategoryResolution
    object Unknown : CategoryResolution
}

/**
 * Sure category name to id. Case-insensitive. Fetched once, refreshed on a miss so a
 * category created in Sure after startup is picked up. Never creates categories.
 */
class CategoryResolver(private val client: SureClient) {

    private var byFoldedName: Map<String, List<String>>? = null

    private fun fold(name: String) = name.trim().lowercase()

    private fun load(): Map<String, List<String>> =
        client.listCategories().groupBy({ fold(it.name) }, { it.id }).also { byFoldedName = it }

    @Synchronized
    fun resolve(name: String): CategoryResolution {
        val key = fold(name)
        val ids = (byFoldedName ?: load())[key] ?: load()[key] ?: return CategoryResolution.Unknown
        return if (ids.size == 1) CategoryResolution.Resolved(ids[0]) else CategoryResolution.Ambiguous(ids)
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `./gradlew test --tests 'org.jevy.bookkeeper.sure.CategoryResolverTest' --console=plain`
Expected: PASS, 5 tests.

- [ ] **Step 5: Commit**

```bash
git add src/main/kotlin/org/jevy/bookkeeper/sure/CategoryResolver.kt src/test/kotlin/org/jevy/bookkeeper/sure/CategoryResolverTest.kt
git commit -m "Add CategoryResolver with case-insensitive cache and ambiguity detection"
```

---

### Task 8: `TransactionMatcher`

**Files:**
- Create: `src/main/kotlin/org/jevy/bookkeeper/sure/TransactionMatcher.kt`
- Test: `src/test/kotlin/org/jevy/bookkeeper/sure/TransactionMatcherTest.kt`

**Interfaces:**
- Consumes: `SureClient.listTransactions`, `SureTransaction` (Task 6), `AmountConvention` (Task 5).
- Produces:
  ```kotlin
  sealed interface MatchResult {
      data class Matched(val transaction: SureTransaction, val rung: Int) : MatchResult
      data class Unmatched(val reason: String) : MatchResult   // "account_unmapped" | "bad_amount" | "bad_date" | "no_match" | "ambiguous"
  }
  class TransactionMatcher(client: SureClient, accountMap: Map<String, String>) {
      fun match(tx: Transaction): MatchResult
  }
  ```

- [ ] **Step 1: Write the failing test**

Create `src/test/kotlin/org/jevy/bookkeeper/sure/TransactionMatcherTest.kt`:

```kotlin
package org.jevy.bookkeeper.sure

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.jevy.bookkeeper_agent.Transaction
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import java.time.LocalDate
import kotlin.test.assertEquals
import kotlin.test.assertIs

class TransactionMatcherTest {

    private val client = mockk<SureClient>()
    private val accountMap = mapOf("Chequing" to "acct-1")
    private val matcher = TransactionMatcher(client, accountMap)

    private fun sheetTx(
        id: String = "txn-1", date: String = "2/15/2026", description: String = "COSTCO WHOLESAL",
        amount: String = "-\$384.91", account: String = "Chequing",
    ): Transaction = Transaction.newBuilder()
        .setTransactionId(id).setDate(date).setDescription(description).setCategory("Groceries")
        .setAmount(amount).setAccount(account).build()

    private fun sureTx(id: String, date: String = "2026-02-15", name: String = "COSTCO WHOLESALE #123", cents: Long = -38491) =
        SureTransaction(id, LocalDate.parse(date), name, cents, "acct-1", null, "TRN-$id")

    private fun stubQuery(start: String, end: String, vararg results: SureTransaction) {
        every {
            client.listTransactions("acct-1", LocalDate.parse(start), LocalDate.parse(end), BigDecimal("384.91"), BigDecimal("384.91"))
        } returns results.toList()
    }

    @Test
    fun `rung 2 exact date and amount with one result`() {
        stubQuery("2026-02-15", "2026-02-15", sureTx("s-1"))
        val r = matcher.match(sheetTx())
        assertEquals(MatchResult.Matched(sureTx("s-1"), rung = 2), r)
    }

    @Test
    fun `rung 3 widens to plus or minus three days when exact misses`() {
        stubQuery("2026-02-15", "2026-02-15")
        stubQuery("2026-02-12", "2026-02-18", sureTx("s-1", date = "2026-02-18"))
        assertEquals(MatchResult.Matched(sureTx("s-1", date = "2026-02-18"), rung = 3), matcher.match(sheetTx()))
    }

    @Test
    fun `rung 3 window is inclusive at three days`() {
        stubQuery("2026-02-15", "2026-02-15")
        stubQuery("2026-02-12", "2026-02-18", sureTx("s-1", date = "2026-02-12"))
        assertIs<MatchResult.Matched>(matcher.match(sheetTx()))
    }

    @Test
    fun `rung 4 disambiguates several candidates by description`() {
        stubQuery("2026-02-15", "2026-02-15", sureTx("s-1", name = "COSTCO WHOLESALE"), sureTx("s-2", name = "SHELL GAS"))
        val r = matcher.match(sheetTx(description = "COSTCO WHOLESAL"))
        assertEquals(MatchResult.Matched(sureTx("s-1", name = "COSTCO WHOLESALE"), rung = 4), r)
    }

    @Test
    fun `rung 4 with no clear winner is ambiguous`() {
        stubQuery("2026-02-15", "2026-02-15", sureTx("s-1", name = "COSTCO WHOLESALE"), sureTx("s-2", name = "COSTCO WHOLESALE"))
        assertEquals(MatchResult.Unmatched("ambiguous"), matcher.match(sheetTx()))
    }

    @Test
    fun `no candidates in either window is no_match`() {
        stubQuery("2026-02-15", "2026-02-15")
        stubQuery("2026-02-12", "2026-02-18")
        assertEquals(MatchResult.Unmatched("no_match"), matcher.match(sheetTx()))
    }

    @Test
    fun `unmapped account is rejected before any API call`() {
        assertEquals(MatchResult.Unmatched("account_unmapped"), matcher.match(sheetTx(account = "Mystery")))
        verify(exactly = 0) { client.listTransactions(any(), any(), any(), any(), any()) }
    }

    @Test
    fun `unparseable amount is rejected before any API call`() {
        assertEquals(MatchResult.Unmatched("bad_amount"), matcher.match(sheetTx(amount = "n/a")))
        verify(exactly = 0) { client.listTransactions(any(), any(), any(), any(), any()) }
    }

    @Test
    fun `unparseable date is rejected before any API call`() {
        assertEquals(MatchResult.Unmatched("bad_date"), matcher.match(sheetTx(date = "2026-02-15")))
        verify(exactly = 0) { client.listTransactions(any(), any(), any(), any(), any()) }
    }

    @Test
    fun `rung 1 cache returns the previous match without querying`() {
        stubQuery("2026-02-15", "2026-02-15", sureTx("s-1"))
        matcher.match(sheetTx())
        val second = matcher.match(sheetTx())
        assertEquals(MatchResult.Matched(sureTx("s-1"), rung = 1), second)
        verify(exactly = 1) { client.listTransactions(any(), any(), any(), any(), any()) }
    }

    @Test
    fun `income amount flips sign for the query parameters`() {
        every {
            client.listTransactions("acct-1", LocalDate.parse("2026-02-15"), LocalDate.parse("2026-02-15"), BigDecimal("-1371.24"), BigDecimal("-1371.24"))
        } returns listOf(sureTx("s-1", cents = 137124))
        assertIs<MatchResult.Matched>(matcher.match(sheetTx(amount = "\$1,371.24")))
    }

    @Test
    fun `a candidate whose signed cents disagree is dropped`() {
        // Server-side filter is on the inverted column; belt and braces on the response.
        stubQuery("2026-02-15", "2026-02-15", sureTx("s-1", cents = 38491))
        stubQuery("2026-02-12", "2026-02-18", sureTx("s-1", cents = 38491))
        assertEquals(MatchResult.Unmatched("no_match"), matcher.match(sheetTx()))
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew test --tests 'org.jevy.bookkeeper.sure.TransactionMatcherTest' --console=plain`
Expected: compilation FAILS.

- [ ] **Step 3: Implement**

Create `src/main/kotlin/org/jevy/bookkeeper/sure/TransactionMatcher.kt`:

```kotlin
package org.jevy.bookkeeper.sure

import org.jevy.bookkeeper_agent.Transaction
import org.slf4j.LoggerFactory
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeParseException
import java.util.concurrent.ConcurrentHashMap

sealed interface MatchResult {
    data class Matched(val transaction: SureTransaction, val rung: Int) : MatchResult
    /** reason is one of: account_unmapped, bad_amount, bad_date, no_match, ambiguous */
    data class Unmatched(val reason: String) : MatchResult
}

/**
 * Finds the Sure transaction for a Sheet transaction. The two share no key, so this
 * is a ladder: cache, exact (account, date, amount), then a date window, then
 * description similarity to break ties. Ambiguity is never guessed through.
 */
class TransactionMatcher(
    private val client: SureClient,
    private val accountMap: Map<String, String>,
    private val dateWindowDays: Long = 3,
) {
    private val logger = LoggerFactory.getLogger(TransactionMatcher::class.java)
    private val sheetDate = DateTimeFormatter.ofPattern("M/d/yyyy")

    /** Rung 1. Process lifetime only; the PATCH is idempotent so durability buys nothing. */
    private val cache = ConcurrentHashMap<String, SureTransaction>()

    fun match(tx: Transaction): MatchResult {
        val transactionId = tx.getTransactionId().toString()
        cache[transactionId]?.let { return MatchResult.Matched(it, rung = 1) }

        val accountId = accountMap[tx.getAccount().toString().trim()]
            ?: return MatchResult.Unmatched("account_unmapped")

        val sheetAmount = try {
            AmountConvention.parseSheetAmount(tx.getAmount().toString())
        } catch (e: IllegalArgumentException) {
            return MatchResult.Unmatched("bad_amount")
        }
        val date = try {
            LocalDate.parse(tx.getDate().toString().trim(), sheetDate)
        } catch (e: DateTimeParseException) {
            return MatchResult.Unmatched("bad_date")
        }

        val expectedCents = AmountConvention.toCents(sheetAmount)
        val entryAmount = AmountConvention.toSureEntryAmount(sheetAmount)

        fun query(start: LocalDate, end: LocalDate): List<SureTransaction> =
            client.listTransactions(accountId, start, end, entryAmount, entryAmount)
                .filter { it.signedAmountCents == expectedCents }

        // Rung 2: exact date
        val exact = query(date, date)
        decide(tx, exact, rung = 2)?.let { return remember(transactionId, it) }

        // Rung 3: date window (only if rung 2 had nothing at all; several exact hits go to rung 4)
        if (exact.isEmpty()) {
            val windowed = query(date.minusDays(dateWindowDays), date.plusDays(dateWindowDays))
            decide(tx, windowed, rung = 3)?.let { return remember(transactionId, it) }
            if (windowed.isEmpty()) return MatchResult.Unmatched("no_match")
            return disambiguate(tx, windowed)?.let { remember(transactionId, MatchResult.Matched(it, rung = 4)) }
                ?: MatchResult.Unmatched("ambiguous")
        }

        // Rung 4: several exact-date candidates
        return disambiguate(tx, exact)?.let { remember(transactionId, MatchResult.Matched(it, rung = 4)) }
            ?: MatchResult.Unmatched("ambiguous")
    }

    private fun decide(tx: Transaction, candidates: List<SureTransaction>, rung: Int): MatchResult? =
        if (candidates.size == 1) MatchResult.Matched(candidates[0], rung) else null

    private fun remember(transactionId: String, result: MatchResult): MatchResult {
        if (result is MatchResult.Matched) cache[transactionId] = result.transaction
        return result
    }

    /** Returns the single best candidate by description similarity, or null if there is no clear winner. */
    private fun disambiguate(tx: Transaction, candidates: List<SureTransaction>): SureTransaction? {
        val target = normalize(tx.getDescription().toString())
        val scored = candidates.map { it to similarity(target, normalize(it.name)) }.sortedByDescending { it.second }
        val (best, bestScore) = scored[0]
        val runnerUp = scored.getOrNull(1)?.second ?: 0.0
        val clearWinner = bestScore >= 0.5 && bestScore - runnerUp >= 0.2
        if (!clearWinner) {
            logger.info("Ambiguous match for {}: {}", tx.getTransactionId(), scored.map { "${it.first.name}=${"%.2f".format(it.second)}" })
            return null
        }
        return best
    }

    private fun normalize(s: String) = s.lowercase().replace(Regex("[^a-z0-9 ]"), " ").replace(Regex("\\s+"), " ").trim()

    /** Token overlap (Jaccard) plus a prefix bonus, both in [0,1]; enough to separate COSTCO from SHELL. */
    private fun similarity(a: String, b: String): Double {
        if (a.isEmpty() || b.isEmpty()) return 0.0
        val ta = a.split(' ').toSet()
        val tb = b.split(' ').toSet()
        val jaccard = ta.intersect(tb).size.toDouble() / ta.union(tb).size
        val prefix = if (b.startsWith(a) || a.startsWith(b)) 1.0 else 0.0
        return maxOf(jaccard, prefix)
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `./gradlew test --tests 'org.jevy.bookkeeper.sure.TransactionMatcherTest' --console=plain`
Expected: PASS, 12 tests. If the rung 4 description test fails on the similarity threshold, adjust the constants in `disambiguate`, not the test.

- [ ] **Step 5: Commit**

```bash
git add src/main/kotlin/org/jevy/bookkeeper/sure/TransactionMatcher.kt src/test/kotlin/org/jevy/bookkeeper/sure/TransactionMatcherTest.kt
git commit -m "Add TransactionMatcher rung ladder"
```

---

### Task 9: `SureSink`

**Files:**
- Create: `src/main/kotlin/org/jevy/bookkeeper/sure/SureSink.kt`
- Test: `src/test/kotlin/org/jevy/bookkeeper/sure/SureSinkTest.kt`

**Interfaces:**
- Consumes: `CategorySink`, `SinkResult` (Task 1); `SureClient`, `SureUnavailableException`, `SureRequestException` (Task 6); `CategoryResolver`, `CategoryResolution` (Task 7); `TransactionMatcher`, `MatchResult` (Task 8); `AppConfig.sure*` (Task 4).
- Produces:
  ```kotlin
  class SureSink(config: AppConfig, client: SureClient = SureClient(config.sureApiUrl, config.sureApiKey, config.sureMaxApiCallsPerSec),
                 matcher: TransactionMatcher = TransactionMatcher(client, config.sureAccountMap),
                 resolver: CategoryResolver = CategoryResolver(client),
                 meterRegistry: MeterRegistry = SimpleMeterRegistry()) : CategorySink
      // name = "sure", consumerGroup = "sure-writer", dlqTopic = TopicNames.SURE_WRITE_FAILED
  ```

- [ ] **Step 1: Write the failing test**

Create `src/test/kotlin/org/jevy/bookkeeper/sure/SureSinkTest.kt`:

```kotlin
package org.jevy.bookkeeper.sure

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.jevy.bookkeeper.config.AppConfig
import org.jevy.bookkeeper.kafka.TopicNames
import org.jevy.bookkeeper.writer.SinkResult
import org.jevy.bookkeeper_agent.Transaction
import org.junit.jupiter.api.Test
import java.time.LocalDate
import kotlin.test.assertEquals
import kotlin.test.assertIs

class SureSinkTest {

    private fun config(enabled: Boolean = true, dryRun: Boolean = false) = AppConfig(
        kafkaBootstrapServers = "localhost:9092", schemaRegistryUrl = "http://localhost:8081",
        googleSheetId = "test", googleCredentialsJson = "{}", openrouterApiKey = "",
        maxTransactionAgeDays = 365, maxTransactions = 0, additionalContextPrompt = null, model = "",
        sureApiUrl = "http://sure", sureApiKey = "k", sureEnabled = enabled, sureDryRun = dryRun,
        sureAccountMap = mapOf("Chequing" to "acct-1"),
    )

    private val client = mockk<SureClient>(relaxed = true)
    private val matcher = mockk<TransactionMatcher>()
    private val resolver = mockk<CategoryResolver>()
    private val registry = SimpleMeterRegistry()

    private fun sink(enabled: Boolean = true, dryRun: Boolean = false) =
        SureSink(config(enabled, dryRun), client, matcher, resolver, registry)

    private fun tx(category: String? = "Groceries"): Transaction = Transaction.newBuilder()
        .setTransactionId("txn-1").setDate("2/15/2026").setDescription("COSTCO")
        .setCategory(category).setAmount("-\$384.91").setAccount("Chequing").build()

    private fun sureTx(categoryId: String? = null) =
        SureTransaction("s-1", LocalDate.of(2026, 2, 15), "COSTCO", -38491, "acct-1", categoryId, "TRN-1")

    @Test
    fun `identity`() {
        val s = sink()
        assertEquals("sure", s.name)
        assertEquals("sure-writer", s.consumerGroup)
        assertEquals(TopicNames.SURE_WRITE_FAILED, s.dlqTopic)
    }

    @Test
    fun `happy path matches, resolves, and PATCHes`() {
        every { matcher.match(any()) } returns MatchResult.Matched(sureTx(), rung = 2)
        every { resolver.resolve("Groceries") } returns CategoryResolution.Resolved("c-1")

        assertEquals(SinkResult.Written, sink().write(tx()))

        verify(exactly = 1) { client.updateCategory("s-1", "c-1") }
        assertEquals(1.0, registry.counter("bookkeeper.sure.transactions.written", "match_rung", "2").count())
    }

    @Test
    fun `already carrying the same category is Skipped with no PATCH`() {
        every { matcher.match(any()) } returns MatchResult.Matched(sureTx(categoryId = "c-1"), rung = 2)
        every { resolver.resolve("Groceries") } returns CategoryResolution.Resolved("c-1")

        assertEquals(SinkResult.Skipped("already_categorized"), sink().write(tx()))
        verify(exactly = 0) { client.updateCategory(any(), any()) }
    }

    @Test
    fun `different existing category is overwritten`() {
        every { matcher.match(any()) } returns MatchResult.Matched(sureTx(categoryId = "c-old"), rung = 2)
        every { resolver.resolve("Groceries") } returns CategoryResolution.Resolved("c-1")
        assertEquals(SinkResult.Written, sink().write(tx()))
        verify { client.updateCategory("s-1", "c-1") }
    }

    @Test
    fun `no category on the event is Skipped`() {
        assertEquals(SinkResult.Skipped("no_category"), sink().write(tx(category = null)))
        verify(exactly = 0) { matcher.match(any()) }
    }

    @Test
    fun `unmatched is Rejected with the matcher reason`() {
        every { matcher.match(any()) } returns MatchResult.Unmatched("no_match")
        val r = sink().write(tx())
        assertIs<SinkResult.Rejected>(r)
        assertEquals("no_match", r.reason)
        verify(exactly = 0) { resolver.resolve(any()) }
    }

    @Test
    fun `unknown category is Rejected category_unresolved`() {
        every { matcher.match(any()) } returns MatchResult.Matched(sureTx(), rung = 2)
        every { resolver.resolve("Groceries") } returns CategoryResolution.Unknown
        assertEquals("category_unresolved", (sink().write(tx()) as SinkResult.Rejected).reason)
        verify(exactly = 0) { client.updateCategory(any(), any()) }
    }

    @Test
    fun `ambiguous category is Rejected category_ambiguous`() {
        every { matcher.match(any()) } returns MatchResult.Matched(sureTx(), rung = 2)
        every { resolver.resolve("Groceries") } returns CategoryResolution.Ambiguous(listOf("c-1", "c-2"))
        assertEquals("category_ambiguous", (sink().write(tx()) as SinkResult.Rejected).reason)
    }

    @Test
    fun `Sure 4xx on PATCH is Rejected`() {
        every { matcher.match(any()) } returns MatchResult.Matched(sureTx(), rung = 2)
        every { resolver.resolve("Groceries") } returns CategoryResolution.Resolved("c-1")
        every { client.updateCategory(any(), any()) } throws SureRequestException(422, "nope")
        assertEquals("sure_4xx", (sink().write(tx()) as SinkResult.Rejected).reason)
    }

    @Test
    fun `Sure unavailable anywhere is Unavailable, never Rejected`() {
        every { matcher.match(any()) } throws SureUnavailableException("connection refused")
        assertIs<SinkResult.Unavailable>(sink().write(tx()))

        every { matcher.match(any()) } returns MatchResult.Matched(sureTx(), rung = 2)
        every { resolver.resolve(any()) } throws SureUnavailableException("503")
        assertIs<SinkResult.Unavailable>(sink().write(tx()))

        every { resolver.resolve(any()) } returns CategoryResolution.Resolved("c-1")
        every { client.updateCategory(any(), any()) } throws SureUnavailableException("timeout")
        assertIs<SinkResult.Unavailable>(sink().write(tx()))
    }

    @Test
    fun `disabled skips without touching Sure`() {
        assertEquals(SinkResult.Skipped("disabled"), sink(enabled = false).write(tx()))
        verify(exactly = 0) { matcher.match(any()) }
    }

    @Test
    fun `dry run matches and resolves but never PATCHes`() {
        every { matcher.match(any()) } returns MatchResult.Matched(sureTx(), rung = 2)
        every { resolver.resolve("Groceries") } returns CategoryResolution.Resolved("c-1")
        assertEquals(SinkResult.Skipped("dry_run"), sink(dryRun = true).write(tx()))
        verify(exactly = 0) { client.updateCategory(any(), any()) }
        assertEquals(1.0, registry.counter("bookkeeper.sure.match.rung", "match_rung", "2").count())
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew test --tests 'org.jevy.bookkeeper.sure.SureSinkTest' --console=plain`
Expected: compilation FAILS.

- [ ] **Step 3: Implement**

Create `src/main/kotlin/org/jevy/bookkeeper/sure/SureSink.kt`:

```kotlin
package org.jevy.bookkeeper.sure

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.jevy.bookkeeper.config.AppConfig
import org.jevy.bookkeeper.kafka.TopicNames
import org.jevy.bookkeeper.writer.CategorySink
import org.jevy.bookkeeper.writer.SinkResult
import org.jevy.bookkeeper_agent.Transaction
import org.slf4j.LoggerFactory

/**
 * Mirrors a categorization into Sure: match the Sheet transaction to a Sure
 * transaction, resolve the category name to a Sure id, skip if already set,
 * otherwise PATCH. Sure is a sink only; this never tombstones anything.
 */
class SureSink(
    private val config: AppConfig,
    private val client: SureClient = SureClient(config.sureApiUrl, config.sureApiKey, config.sureMaxApiCallsPerSec),
    private val matcher: TransactionMatcher = TransactionMatcher(client, config.sureAccountMap),
    private val resolver: CategoryResolver = CategoryResolver(client),
    private val meterRegistry: MeterRegistry = SimpleMeterRegistry(),
) : CategorySink {

    private val logger = LoggerFactory.getLogger(SureSink::class.java)

    override val name = "sure"
    override val consumerGroup = "sure-writer"
    override val dlqTopic = TopicNames.SURE_WRITE_FAILED

    override fun write(tx: Transaction): SinkResult {
        val transactionId = tx.getTransactionId().toString()
        if (!config.sureEnabled) return SinkResult.Skipped("disabled")

        val category = tx.getCategory()?.toString()
        if (category.isNullOrBlank()) return SinkResult.Skipped("no_category")

        return try {
            val matched = when (val m = matcher.match(tx)) {
                is MatchResult.Unmatched -> return SinkResult.Rejected(m.reason)
                is MatchResult.Matched -> m
            }
            meterRegistry.counter("bookkeeper.sure.match.rung", "match_rung", matched.rung.toString()).increment()

            val categoryId = when (val r = resolver.resolve(category)) {
                is CategoryResolution.Resolved -> r.id
                is CategoryResolution.Ambiguous -> return SinkResult.Rejected("category_ambiguous")
                CategoryResolution.Unknown -> return SinkResult.Rejected("category_unresolved")
            }

            if (matched.transaction.categoryId == categoryId) {
                return SinkResult.Skipped("already_categorized")
            }

            if (config.sureDryRun) {
                logger.info("DRY RUN: would PATCH Sure transaction {} ({}) with category '{}' ({}) for {} via rung {}",
                    matched.transaction.id, matched.transaction.name, category, categoryId, transactionId, matched.rung)
                return SinkResult.Skipped("dry_run")
            }

            client.updateCategory(matched.transaction.id, categoryId)
            meterRegistry.counter("bookkeeper.sure.transactions.written", "match_rung", matched.rung.toString()).increment()
            logger.info("Wrote category '{}' to Sure transaction {} for {} via rung {}", category, matched.transaction.id, transactionId, matched.rung)
            SinkResult.Written
        } catch (e: SureUnavailableException) {
            SinkResult.Unavailable(e)
        } catch (e: SureRequestException) {
            SinkResult.Rejected("sure_4xx", e)
        }
    }
}
```

Note: `SinkWriter` also counts `bookkeeper.sure.transactions.written` without a tag. Micrometer treats a differently-tagged meter with the same name as a separate meter; keep both, Grafana sums them. The `match_rung`-tagged one is the one the spec's dashboards watch.

- [ ] **Step 4: Run test to verify it passes**

Run: `./gradlew test --tests 'org.jevy.bookkeeper.sure.SureSinkTest' --console=plain`
Expected: PASS, 12 tests.

- [ ] **Step 5: Commit**

```bash
git add src/main/kotlin/org/jevy/bookkeeper/sure/SureSink.kt src/test/kotlin/org/jevy/bookkeeper/sure/SureSinkTest.kt
git commit -m "Add SureSink"
```

---

### Task 10: `sure-writer` command in `Main`

**Files:**
- Modify: `src/main/kotlin/org/jevy/bookkeeper/Main.kt`

**Interfaces:**
- Consumes: `SinkWriter` (Task 1), `SureSink` (Task 9).

- [ ] **Step 1: Add the command**

In `Main.kt` add imports:

```kotlin
import org.jevy.bookkeeper.sure.SureSink
import org.jevy.bookkeeper.writer.SinkWriter
```

Add a branch after the `"writer"` branch:

```kotlin
        "sure-writer" -> {
            val config = AppConfig.fromEnv()
            require(config.sureApiUrl.isNotBlank()) { "SURE_API_URL is required for sure-writer" }
            require(config.sureApiKey.isNotBlank()) { "SURE_API_KEY is required for sure-writer" }
            Metrics.startHttpServer(config.metricsPort)
            logger.info("Starting Sure Writer (enabled={}, dryRun={}, accounts mapped={})",
                config.sureEnabled, config.sureDryRun, config.sureAccountMap.size)
            SinkWriter(config, SureSink(config, meterRegistry = Metrics.registry), Metrics.registry, tombstoneUncategorized = false).run(
                onActivity = Metrics::updateActivity,
                onAlive = Metrics::setConsumerAlive,
            )
        }
```

Update both usage strings to include `sure-writer` after `writer`.

- [ ] **Step 2: Build**

Run: `./gradlew installDist --console=plain -q`
Expected: BUILD SUCCESSFUL.

- [ ] **Step 3: Smoke the usage line**

Run: `./build/install/bookkeeper-agent/bin/bookkeeper-agent 2>&1 | head -1`
Expected: usage line containing `sure-writer`.

- [ ] **Step 4: Commit**

```bash
git add src/main/kotlin/org/jevy/bookkeeper/Main.kt
git commit -m "Add sure-writer entrypoint"
```

---

### Task 11: `dlq-replay sure` mode

**Files:**
- Modify: `src/main/kotlin/org/jevy/bookkeeper/replay/DlqReplayer.kt`
- Modify: `src/main/kotlin/org/jevy/bookkeeper/Main.kt` (the `dlq-replay` branch)
- Test: `src/test/kotlin/org/jevy/bookkeeper/replay/DlqReplayerTest.kt`

**Interfaces:**
- Produces: `DlqReplayer(config, mode: ReplayMode = ReplayMode.RECATEGORIZE)`, `enum class ReplayMode { RECATEGORIZE, SURE }`.

- [ ] **Step 1: Read the existing test's helper style**

Open `src/test/kotlin/org/jevy/bookkeeper/replay/DlqReplayerTest.kt` and read how it mocks `KafkaFactory`, `consumer.partitionsFor`, `poll`, `position`, and `endOffsets`. The new test below follows the same pattern; copy any helper it needs from the existing file rather than re-inventing it.

- [ ] **Step 2: Write the failing test**

Append to `DlqReplayerTest`:

```kotlin
    @Test
    fun `sure mode republishes sure-write-failed records to categorized unchanged and tombstones the DLQ`() {
        val consumer = mockk<KafkaConsumer<String, Transaction>>(relaxed = true)
        val tombstoneProducer = mockk<KafkaProducer<String, ByteArray?>>(relaxed = true)
        val avroProducer = mockk<KafkaProducer<String, Transaction>>(relaxed = true)
        mockkObject(KafkaFactory)
        every { KafkaFactory.createConsumer(any(), any()) } returns consumer
        every { KafkaFactory.createTombstoneProducer(any()) } returns tombstoneProducer
        every { KafkaFactory.createProducer(any()) } returns avroProducer

        val tp = TopicPartition(TopicNames.SURE_WRITE_FAILED, 0)
        every { consumer.partitionsFor(TopicNames.SURE_WRITE_FAILED) } returns listOf(partitionInfo(TopicNames.SURE_WRITE_FAILED))
        every { consumer.endOffsets(any()) } returns mapOf(tp to 1L)
        every { consumer.position(tp) } returns 1L

        val tx = makeTransaction("txn-1", "COSTCO", category = "Groceries", categoryJustification = "because")
        every { consumer.poll(any<Duration>()) } returns
            ConsumerRecords(mapOf(tp to listOf(ConsumerRecord(TopicNames.SURE_WRITE_FAILED, 0, 0L, "txn-1", tx))))

        DlqReplayer(config, ReplayMode.SURE).run()

        verify { consumer.partitionsFor(TopicNames.SURE_WRITE_FAILED) }
        verify(exactly = 0) { consumer.partitionsFor(TopicNames.WRITE_FAILED) }
        verify { tombstoneProducer.send(match { it.topic() == TopicNames.SURE_WRITE_FAILED && it.key() == "txn-1" && it.value() == null }) }
        val sent = slot<ProducerRecord<String, Transaction>>()
        verify { avroProducer.send(capture(sent)) }
        assertEquals(TopicNames.CATEGORIZED, sent.captured.topic())
        assertEquals("Groceries", sent.captured.value().getCategory().toString())
        assertEquals("because", sent.captured.value().getCategoryJustification().toString())
    }
```

- [ ] **Step 3: Run test to verify it fails**

Run: `./gradlew test --tests 'org.jevy.bookkeeper.replay.DlqReplayerTest' --console=plain`
Expected: compilation FAILS on `ReplayMode`.

- [ ] **Step 4: Implement**

In `DlqReplayer.kt`:

```kotlin
enum class ReplayMode {
    /** categorization-failed and write-failed: clear the category, back to uncategorized, categorizer runs again. */
    RECATEGORIZE,
    /** sure-write-failed: the category is already right, back to categorized unchanged so the sinks retry. */
    SURE,
}

class DlqReplayer(private val config: AppConfig, private val mode: ReplayMode = ReplayMode.RECATEGORIZE) {
```

Replace the `dlqTopics` line:

```kotlin
        val dlqTopics = when (mode) {
            ReplayMode.RECATEGORIZE -> listOf(TopicNames.CATEGORIZATION_FAILED, TopicNames.WRITE_FAILED)
            ReplayMode.SURE -> listOf(TopicNames.SURE_WRITE_FAILED)
        }
```

Replace the body of `replayRecord` after the tombstone with:

```kotlin
        when (mode) {
            ReplayMode.RECATEGORIZE -> {
                // Null out category and category_justification so the categorizer will process it
                val cleaned = Transaction.newBuilder(record.value())
                    .setCategory(null)
                    .setCategoryJustification(null)
                    .build()
                avroProducer.send(ProducerRecord(TopicNames.UNCATEGORIZED, key, cleaned))
            }
            ReplayMode.SURE -> avroProducer.send(ProducerRecord(TopicNames.CATEGORIZED, key, record.value()))
        }

        logger.info("Replayed transaction {} from {} ({})", key, record.topic(), mode)
```

In `Main.kt`, in the `"dlq-replay"` branch, replace `DlqReplayer(config).run()` with:

```kotlin
            val mode = when (args.getOrNull(1)) {
                null -> ReplayMode.RECATEGORIZE
                "sure" -> ReplayMode.SURE
                else -> throw IllegalArgumentException("Unknown dlq-replay mode '${args[1]}'. Use no argument or 'sure'.")
            }
            logger.info("Starting DLQ Replayer ({})", mode)
            DlqReplayer(config, mode).run()
```

and add `import org.jevy.bookkeeper.replay.ReplayMode`. Update the usage strings to read `dlq-replay [sure]`.

- [ ] **Step 5: Run test to verify it passes**

Run: `./gradlew test --tests 'org.jevy.bookkeeper.replay.DlqReplayerTest' --console=plain`
Expected: PASS, all existing tests plus the new one.

- [ ] **Step 6: Commit**

```bash
git add src/main/kotlin/org/jevy/bookkeeper/replay/DlqReplayer.kt src/main/kotlin/org/jevy/bookkeeper/Main.kt src/test/kotlin/org/jevy/bookkeeper/replay/DlqReplayerTest.kt
git commit -m "Add dlq-replay sure mode that republishes to categorized"
```

---

### Task 12: Broker-gated integration test

**Files:**
- Test: `src/test/kotlin/org/jevy/bookkeeper/sure/SureWriterIntegrationTest.kt`

**Interfaces:**
- Consumes: everything above. Uses `MockWebServer` as Sure and the local Redpanda from `docker compose up -d redpanda`.

- [ ] **Step 1: Write the test**

```kotlin
package org.jevy.bookkeeper.sure

import okhttp3.mockwebserver.Dispatcher
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import okhttp3.mockwebserver.RecordedRequest
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.jevy.bookkeeper.config.AppConfig
import org.jevy.bookkeeper.kafka.KafkaFactory
import org.jevy.bookkeeper.kafka.TopicInitializer
import org.jevy.bookkeeper.kafka.TopicNames
import org.jevy.bookkeeper.writer.SinkUnavailableException
import org.jevy.bookkeeper.writer.SinkWriter
import org.jevy.bookkeeper_agent.Transaction
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.assertThrows
import java.time.Duration
import java.util.UUID
import kotlin.concurrent.thread
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Real Redpanda, fake Sure. Self-skips without a broker.
 * To run locally: `docker compose up -d redpanda` then `./gradlew test`.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SureWriterIntegrationTest {

    private val bootstrap = System.getenv("KAFKA_BOOTSTRAP_SERVERS") ?: "localhost:19092"
    private val schemaRegistry = System.getenv("SCHEMA_REGISTRY_URL") ?: "http://localhost:18081"
    private lateinit var sure: MockWebServer
    private val patches = mutableListOf<RecordedRequest>()

    @BeforeAll
    fun setup() {
        assumeTrue(brokerReachable(), "No Kafka broker at $bootstrap, skipping Sure writer integration test")
        TopicInitializer.run(bootstrap)
        sure = MockWebServer()
        sure.dispatcher = object : Dispatcher() {
            override fun dispatch(request: RecordedRequest): MockResponse {
                val path = request.requestUrl!!.encodedPath
                return when {
                    path == "/api/v1/categories" -> MockResponse().setBody(
                        """{"categories":[{"id":"c-groc","name":"Groceries"}],"pagination":{"page":1,"per_page":100,"total_count":1,"total_pages":1}}""")
                    path == "/api/v1/transactions" && request.requestUrl!!.queryParameter("start_date") == "2026-03-01" -> MockResponse().setBody(
                        """{"transactions":[{"id":"s-1","date":"2026-03-01","name":"MERCHANT","signed_amount_cents":-1000,"external_id":null,
                             "account":{"id":"acct-1","name":"Visa"},"category":null}],
                            "pagination":{"page":1,"per_page":100,"total_count":1,"total_pages":1}}""")
                    path == "/api/v1/transactions" -> MockResponse().setBody(
                        """{"transactions":[],"pagination":{"page":1,"per_page":100,"total_count":0,"total_pages":1}}""")
                    request.method == "PATCH" -> { patches += request; MockResponse().setBody("""{"id":"s-1"}""") }
                    else -> MockResponse().setResponseCode(404)
                }
            }
        }
        sure.start()
    }

    @AfterAll
    fun teardown() { if (this::sure.isInitialized) sure.shutdown() }

    private fun brokerReachable(): Boolean = try {
        AdminClient.create(mapOf(
            AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrap,
            AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG to "3000",
            AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG to "3000",
        )).use { it.listTopics().names().get() }
        true
    } catch (e: Exception) { false }

    private fun config(sureUrl: String, dryRun: Boolean = false) = AppConfig(
        kafkaBootstrapServers = bootstrap, schemaRegistryUrl = schemaRegistry,
        googleSheetId = "", googleCredentialsJson = "", openrouterApiKey = "",
        maxTransactionAgeDays = 365, maxTransactions = 0, additionalContextPrompt = null, model = "",
        sureApiUrl = sureUrl, sureApiKey = "k", sureMaxApiCallsPerSec = 1000, sureDryRun = dryRun,
        sureAccountMap = mapOf("Visa" to "acct-1"),
    )

    private fun tx(id: String, date: String): Transaction = Transaction.newBuilder()
        .setTransactionId(id).setDate(date).setDescription("MERCHANT").setCategory("Groceries")
        .setAmount("-\$10.00").setAccount("Visa").build()

    private fun publishCategorized(vararg txs: Transaction) {
        KafkaFactory.createProducer(config(sure.url("/").toString())).use { p ->
            txs.forEach { p.send(ProducerRecord(TopicNames.CATEGORIZED, it.getTransactionId().toString(), it)).get() }
            p.flush()
        }
    }

    /** Runs the writer on a fresh consumer group until [stopWhen] is true or 20 s pass. */
    private fun runWriter(cfg: AppConfig, group: String, stopWhen: () -> Boolean): Throwable? {
        val sink = SureSink(cfg)
        val writer = SinkWriter(cfg, object : org.jevy.bookkeeper.writer.CategorySink by sink {
            override val consumerGroup = group
        })
        var failure: Throwable? = null
        val t = thread { try { writer.run() } catch (e: Throwable) { failure = e } }
        val deadline = System.currentTimeMillis() + 20_000
        while (System.currentTimeMillis() < deadline && t.isAlive && !stopWhen()) Thread.sleep(200)
        t.interrupt()
        t.join(5_000)
        return failure
    }

    private fun dlqCount(): Int {
        val cfg = config(sure.url("/").toString())
        KafkaFactory.createConsumer(cfg, "it-dlq-reader-${UUID.randomUUID()}").use { c ->
            val tps = c.partitionsFor(TopicNames.SURE_WRITE_FAILED).map { TopicPartition(it.topic(), it.partition()) }
            c.assign(tps); c.seekToBeginning(tps)
            var n = 0
            repeat(5) { c.poll(Duration.ofSeconds(1)).forEach { if (it.value() != null) n++ } }
            return n
        }
    }

    @Test
    fun `a categorized event produces exactly one PATCH`() {
        val id = "it-match-${UUID.randomUUID()}"
        publishCategorized(tx(id, "3/1/2026"))
        patches.clear()
        runWriter(config(sure.url("/").toString().trimEnd('/')), "it-sure-${UUID.randomUUID()}") { patches.isNotEmpty() }
        assertEquals(1, patches.count { it.requestUrl!!.encodedPath == "/api/v1/transactions/s-1" })
    }

    @Test
    fun `a match failure produces a DLQ record and no PATCH`() {
        val id = "it-nomatch-${UUID.randomUUID()}"
        val before = dlqCount()
        publishCategorized(tx(id, "4/1/2026"))
        patches.clear()
        runWriter(config(sure.url("/").toString().trimEnd('/')), "it-sure-${UUID.randomUUID()}") { dlqCount() > before }
        assertTrue(dlqCount() > before)
        assertEquals(0, patches.size)
    }

    @Test
    fun `dry run performs zero PATCHes`() {
        publishCategorized(tx("it-dry-${UUID.randomUUID()}", "3/1/2026"))
        patches.clear()
        runWriter(config(sure.url("/").toString().trimEnd('/'), dryRun = true), "it-sure-${UUID.randomUUID()}") { false }
        assertEquals(0, patches.size)
    }

    @Test
    fun `Sure unreachable exits the loop without DLQ records`() {
        val before = dlqCount()
        publishCategorized(tx("it-down-${UUID.randomUUID()}", "3/1/2026"))
        val failure = runWriter(config("http://127.0.0.1:1"), "it-sure-${UUID.randomUUID()}") { false }
        assertTrue(failure is SinkUnavailableException, "expected SinkUnavailableException, got $failure")
        assertEquals(before, dlqCount())
    }
}
```

- [ ] **Step 2: Run without a broker to confirm it self-skips**

Run: `./gradlew test --tests 'org.jevy.bookkeeper.sure.SureWriterIntegrationTest' --console=plain`
Expected: tests reported as skipped, build green.

- [ ] **Step 3: Run with the broker**

Run: `docker compose up -d redpanda` then `./gradlew test --tests 'org.jevy.bookkeeper.sure.SureWriterIntegrationTest' --console=plain`
Expected: PASS, 4 tests. If the "unreachable" test takes long, that is the client's three retries with real 1 s and 2 s backoff plus connect timeouts; acceptable.

- [ ] **Step 4: Run the full suite with the broker up**

Run: `./gradlew test --console=plain`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/test/kotlin/org/jevy/bookkeeper/sure/SureWriterIntegrationTest.kt
git commit -m "Add broker-gated Sure writer integration test"
```

---

### Task 13: Kubernetes, alerts, TypeStream view, README

**Files:**
- Create: `k8s/app/deployment-sure-writer.yaml`
- Modify: `k8s/app/kustomization.yaml`
- Modify: `k8s/app/prometheusrule.yaml`
- Create: `pipelines/sure-write-failed-view.typestream.json`
- Modify: `README.md` (architecture section)

- [ ] **Step 1: Deployment**

Create `k8s/app/deployment-sure-writer.yaml`. Image tag matches `deployment-writer.yaml` at the time of the change; check it and use the same.

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: bookkeeper-sure-writer
  labels:
    app: bookkeeper
    component: sure-writer
spec:
  replicas: 1
  selector:
    matchLabels:
      app: bookkeeper
      component: sure-writer
  template:
    metadata:
      labels:
        app: bookkeeper
        component: sure-writer
    spec:
      containers:
        - name: sure-writer
          image: ghcr.io/jevy/bookkeeper-agent:0.8.3
          args: ["sure-writer"]
          env:
            - name: KAFKA_BOOTSTRAP_SERVERS
              value: "redpanda.apps.svc.cluster.local:9093"
            - name: SCHEMA_REGISTRY_URL
              value: "http://redpanda.apps.svc.cluster.local:8081"
            - name: GOOGLE_SHEET_ID
              valueFrom:
                secretKeyRef:
                  name: bookkeeper
                  key: GOOGLE_SHEET_ID
            - name: GOOGLE_CREDENTIALS_JSON
              valueFrom:
                secretKeyRef:
                  name: bookkeeper
                  key: GOOGLE_CREDENTIALS_JSON
            - name: SURE_API_URL
              value: "http://sure-web.apps.svc.cluster.local:3000"
            - name: SURE_API_KEY
              valueFrom:
                secretKeyRef:
                  name: sure-writer
                  key: SURE_API_KEY
            - name: SURE_ENABLED
              value: "false"
            - name: SURE_DRY_RUN
              value: "true"
            - name: SURE_MAX_API_CALLS_PER_SEC
              value: "5"
            - name: SURE_ACCOUNT_MAP
              valueFrom:
                secretKeyRef:
                  name: sure-writer
                  key: SURE_ACCOUNT_MAP
          ports:
            - containerPort: 9091
              name: metrics
          livenessProbe:
            httpGet:
              path: /healthz
              port: 9091
            initialDelaySeconds: 120
            periodSeconds: 60
            failureThreshold: 3
          resources:
            requests:
              memory: "256Mi"
              cpu: "100m"
            limits:
              memory: "512Mi"
              cpu: "500m"
```

`SURE_ENABLED=false` and `SURE_DRY_RUN=true` are the merge-time defaults per the rollout. They are flipped by hand during rollout steps 4 and 6 of the spec.

- [ ] **Step 2: Kustomization and alerts**

In `k8s/app/kustomization.yaml` add `- deployment-sure-writer.yaml` after `- deployment-writer.yaml`.

In `k8s/app/prometheusrule.yaml` append under `rules:`:

```yaml
        - alert: SureWriterConsumerGroupMembersLow
          expr: redpanda_kafka_consumer_group_consumers{redpanda_group="sure-writer"} < 1
          for: 5m
          labels:
            severity: warning
          annotations:
            summary: "Sure writer consumer group has no members"
            description: "Consumer group sure-writer has {{ $value }} members (expected 1)"
        - alert: SureWriterRejectRateHigh
          expr: |
            sum(increase(bookkeeper_sure_transactions_rejected_total[1h]))
              /
            clamp_min(sum(increase(bookkeeper_sure_transactions_written_total[1h]))
              + sum(increase(bookkeeper_sure_transactions_skipped_total[1h]))
              + sum(increase(bookkeeper_sure_transactions_rejected_total[1h])), 1)
              > 0.2
          for: 1h
          labels:
            severity: warning
          annotations:
            summary: "Sure writer rejecting more than 20% of transactions"
            description: "Expected during an initial replay if Sure is missing date ranges the Sheet has. Silence during replay; investigate afterwards."
        - alert: SureWriterStalled
          expr: |
            sum(increase(bookkeeper_sure_transactions_written_total[24h])) == 0
              and sum(increase(bookkeeper_sure_transactions_rejected_total[24h])) == 0
              and sum(increase(bookkeeper_writer_transactions_written_total[24h])) > 0
          for: 1h
          labels:
            severity: warning
          annotations:
            summary: "Sure writer has done nothing for 24h while the Sheets writer is active"
```

- [ ] **Step 3: TypeStream view**

Create `pipelines/sure-write-failed-view.typestream.json`:

```json
{
  "name": "bookkeeper-sure-write-failed-view",
  "version": "1",
  "description": "Materializes transactions.sure-write-failed as a KTable for accurate DLQ count",
  "graph": {
    "nodes": [
      {
        "id": "source",
        "kafkaSource": {
          "topicPath": "/dev/kafka/local/topics/transactions.sure-write-failed",
          "encoding": "AVRO"
        }
      },
      {
        "id": "view",
        "materializedView": {
          "groupByField": "",
          "aggregationType": "latest"
        }
      }
    ],
    "edges": [
      { "fromId": "source", "toId": "view" }
    ]
  }
}
```

- [ ] **Step 4: README**

In `README.md`:
- Change "Six services" to "Seven services".
- In the Mermaid graph add a node `SureWriter["Sure Writer\n(Deployment)"]` inside the self-hosted subgraph, a topic node `sureFailed[transactions.sure-write-failed]` inside Redpanda, an external node `Sure[(Sure)]`, and edges `categorized -- consume --> SureWriter`, `SureWriter -- PATCH category --> Sure`, `SureWriter -- on failure --> sureFailed`.
- Under "Categorization Pipeline" add after the Writer paragraph:

  > **Sure Writer** — Deployment (1 replica). Optional. Consumes categorized transactions on its own consumer group and mirrors each category into a self-hosted [Sure](https://github.com/we-promise/sure) instance by matching on account, date and amount. The Sheet stays the source of truth; Sure is a sink. Failures go to `transactions.sure-write-failed`; replay them with `dlq-replay sure`.

- [ ] **Step 5: Validate the kustomization renders**

Run: `kubectl kustomize k8s/app | grep -c "kind: Deployment"`
Expected: `5` (producer, categorizer, writer, email-processor, sure-writer).

- [ ] **Step 6: Commit**

```bash
git add k8s/app/ pipelines/sure-write-failed-view.typestream.json README.md
git commit -m "Add sure-writer deployment, alerts, DLQ view, and README"
```

---

## Self-review

**Spec coverage.** §2 shared skeleton and sinks: Tasks 1, 2, 9. §3 config: Task 4, deployment env in Task 13. §4 retention guard: Task 3; throttle: Task 6; poll interval respected by the bounded retry in Task 6. §5 ladder, amount and account normalization: Tasks 5, 8. §6 read-before-write, PATCH without `user_modified`, category resolution hazards: Tasks 6, 7, 9. §7 failure table including mid-run `Unavailable`, new topic, `dlq-replay sure`: Tasks 1, 3, 9, 11. §8 metrics and alerts: Tasks 1, 9, 13. §10 tests: every unit item has a task; integration items are Task 12. §11 rollout is operational and lives in the spec.

**Placeholder scan.** None. Every code step has full code.

**Type consistency.** `SinkResult.Rejected(reason, cause)` used identically in Tasks 1, 2, 9. `SureClient.listTransactions(accountId, startDate, endDate, minEntryAmount, maxEntryAmount)` matches the stub in Task 8's test. `MatchResult.Matched(transaction, rung)` shared by Tasks 8 and 9. `ReplayMode` used in Tasks 11 and Main. The spec's "success hook" is implemented as the `tombstoneUncategorized` flag in Task 1; the spec text calls it a hook, the behaviour is the same.

**Review Focus.** 1 is pinned in Task 5 (zero, plain decimal, malformed throws). 2 in Task 9 (already_categorized). 3 in Task 6 (pagination). 4 in Task 8 (account_unmapped, no API call). 5 in Task 9 (Unavailable on first call) and Task 1 (no commit), plus Task 12 end to end.
