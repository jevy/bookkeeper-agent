package org.jevy.bookkeeper.writer

import com.google.api.client.http.HttpHeaders
import com.google.api.client.http.HttpResponseException
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.jevy.bookkeeper.config.AppConfig
import org.jevy.bookkeeper.kafka.KafkaFactory
import org.jevy.bookkeeper.kafka.TopicInitializer
import org.jevy.bookkeeper.sheets.SheetsApi
import org.jevy.bookkeeper.sheets.SheetsClient
import org.jevy.bookkeeper_agent.Transaction
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.time.Duration
import java.util.UUID
import kotlin.concurrent.thread
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * Real Redpanda, fake Sheets. Self-skips without a broker.
 * To run locally: `docker compose up -d redpanda` then `./gradlew test`.
 *
 * This is the regression harness for the 2026-09 backfill, where 678 records reached
 * transactions.write-failed and had their offsets committed because a spent read quota
 * was classified as a permanent rejection. The offset assertions are the point: an
 * uncommitted offset is what lets a restart resume instead of discarding work.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SheetsWriterIntegrationTest {

    private val bootstrap = System.getenv("KAFKA_BOOTSTRAP_SERVERS") ?: "localhost:19092"
    private val schemaRegistry = System.getenv("SCHEMA_REGISTRY_URL") ?: "http://localhost:18081"

    @BeforeAll
    fun setup() {
        assumeTrue(brokerReachable(), "No Kafka broker at $bootstrap, skipping Sheets writer integration test")
        TopicInitializer.run(bootstrap)
    }

    private fun adminProps() = mapOf(
        AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrap,
        AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG to "3000",
        AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG to "3000",
    )

    private fun brokerReachable(): Boolean = try {
        AdminClient.create(adminProps()).use { it.listTopics().names().get() }
        true
    } catch (e: Exception) { false }

    private class Topics(val source: String, val dlq: String)

    private fun freshTopics(): Topics {
        val id = UUID.randomUUID().toString().take(8)
        val t = Topics("it.categorized.$id", "it.write-failed.$id")
        AdminClient.create(adminProps()).use { admin ->
            admin.createTopics(listOf(NewTopic(t.source, 1, 1.toShort()), NewTopic(t.dlq, 1, 1.toShort()))).all().get()
        }
        return t
    }

    private val header = listOf<Any>(
        "Date", "Description", "Category", "Amount", "Account",
        "Account #", "Institution", "Month", "Week", "Transaction ID",
        "Check Number", "Full Description", "Note", "Receipt", "Source",
        "Categorized Date", "Date Added", "Metadata", "Categorized", "Tags", "Migration Notes",
    )

    private fun sheetRow(id: String): List<Any> = listOf(
        "1/1/2026", "MERCHANT", "", "-\$10.00", "Visa",
        "xxxx1234", "TD", "1/1/2026", "12/29/2025", id,
        "", "MERCHANT", "", "", "Yodlee",
        "", "1/1/2026", "", "", "", "",
    )

    /** Refuses the first [quotaFailures] reads the way Google refuses a spent read quota. */
    private class FakeSheets(private val quotaFailures: Int, rowIds: List<String>, header: List<Any>, row: (String) -> List<Any>) : SheetsApi {
        private val rows = listOf(header) + rowIds.map(row)
        var reads = 0
        val writes = mutableListOf<Pair<String, String>>()

        override fun read(range: String): List<List<Any>> {
            reads++
            if (reads <= quotaFailures) {
                throw HttpResponseException.Builder(429, "Too Many Requests", HttpHeaders())
                    .setMessage(
                        "429 Too Many Requests: Quota exceeded for quota metric 'Read requests' and limit " +
                            "'Read requests per minute per user' of service 'sheets.googleapis.com'"
                    )
                    .build()
            }
            if (range == "Transactions!A:U") return rows
            // A single cell read of the Category column: empty, so the sink writes.
            return listOf(listOf(""))
        }

        override fun write(range: String, value: String) { synchronized(writes) { writes += range to value } }
    }

    private fun config() = AppConfig(
        kafkaBootstrapServers = bootstrap, schemaRegistryUrl = schemaRegistry,
        googleSheetId = "sheet-123", googleCredentialsJson = "{}", openrouterApiKey = "",
        maxTransactionAgeDays = 365, maxTransactions = 0, additionalContextPrompt = null, model = "",
    )

    private fun tx(id: String): Transaction = Transaction.newBuilder()
        .setTransactionId(id).setDate("1/1/2026").setDescription("MERCHANT").setCategory("Groceries")
        .setAmount("-\$10.00").setAccount("Visa").build()

    private fun publish(topic: String, vararg txs: Transaction) {
        KafkaFactory.createProducer(config()).use { p ->
            txs.forEach { p.send(ProducerRecord(topic, it.getTransactionId().toString(), it)).get() }
            p.flush()
        }
    }

    /**
     * Runs the real [SheetsSink] over the real [SinkWriter] against [sheets], on its own
     * topics and consumer group, until [stopWhen], the loop dies, or 30 s pass.
     */
    private fun runWriter(
        topics: Topics,
        group: String,
        sheets: SheetsApi,
        stopWhen: (SimpleMeterRegistry) -> Boolean,
    ): Throwable? {
        val cfg = config()
        val registry = SimpleMeterRegistry()
        // 2 attempts with a no-op sleeper: the retry path runs without the test waiting on backoff.
        val client = SheetsClient(cfg, sheets, maxReadsPerSec = 1000.0, maxAttempts = 2, sleeper = {})
        val sink = SheetsSink(cfg, client)
        val isolated = object : CategorySink by sink {
            override val consumerGroup = group
            override val sourceTopic = topics.source
            override val dlqTopic = topics.dlq
        }
        val writer = SinkWriter(cfg, isolated, registry)
        var failure: Throwable? = null
        val t = thread { try { writer.run() } catch (e: Throwable) { failure = e } }
        val deadline = System.currentTimeMillis() + 30_000
        while (System.currentTimeMillis() < deadline && t.isAlive && !stopWhen(registry)) Thread.sleep(200)
        t.interrupt()
        t.join(5_000)
        return failure
    }

    private fun SimpleMeterRegistry.count(name: String, vararg tags: String) = counter(name, *tags).count()

    private fun dlqRecords(topic: String): List<String> {
        KafkaFactory.createConsumer(config(), "it-dlq-reader-${UUID.randomUUID()}").use { c ->
            val tps = c.partitionsFor(topic).map { TopicPartition(it.topic(), it.partition()) }
            c.assign(tps); c.seekToBeginning(tps)
            val keys = mutableListOf<String>()
            repeat(3) { c.poll(Duration.ofSeconds(1)).forEach { if (it.value() != null) keys += it.key() } }
            return keys
        }
    }

    /** Committed offset for [group] on the source topic, or null when nothing was committed. */
    private fun committedOffset(topic: String, group: String): Long? {
        KafkaFactory.createConsumer(config(), group).use { c ->
            val tp = TopicPartition(topic, 0)
            return c.committed(setOf(tp))[tp]?.offset()
        }
    }

    @Test
    fun `a quota refusal that never clears leaves the record uncommitted and out of the DLQ`() {
        val topics = freshTopics()
        val group = "it-sheets-${UUID.randomUUID()}"
        publish(topics.source, tx("it-quota"))
        val sheets = FakeSheets(quotaFailures = Int.MAX_VALUE, rowIds = listOf("it-quota"), header = header, row = ::sheetRow)

        val failure = runWriter(topics, group, sheets) { false }

        assertTrue(failure is SinkUnavailableException, "expected SinkUnavailableException, got $failure")
        assertEquals(emptyList(), dlqRecords(topics.dlq), "a 429 must never dead-letter")
        assertNull(committedOffset(topics.source, group), "the offset must stand so a restart resumes this record")
    }

    @Test
    fun `a quota refusal that clears on retry writes the category and commits`() {
        val topics = freshTopics()
        val group = "it-sheets-${UUID.randomUUID()}"
        publish(topics.source, tx("it-retry"))
        val sheets = FakeSheets(quotaFailures = 1, rowIds = listOf("it-retry"), header = header, row = ::sheetRow)

        val failure = runWriter(topics, group, sheets) { it.count("bookkeeper.writer.transactions.written") >= 1.0 }

        // runWriter stops the loop by interrupting it, so an InterruptException is the normal exit.
        assertTrue(failure !is SinkUnavailableException, "the retry should have succeeded, got $failure")
        assertEquals(listOf("Transactions!C2" to "Groceries"), synchronized(sheets.writes) { sheets.writes.take(1) })
        assertEquals(emptyList(), dlqRecords(topics.dlq))
        assertEquals(1L, committedOffset(topics.source, group))
    }

    @Test
    fun `a record with no row in the sheet still reaches the DLQ and commits`() {
        val topics = freshTopics()
        val group = "it-sheets-${UUID.randomUUID()}"
        publish(topics.source, tx("it-missing"))
        val sheets = FakeSheets(quotaFailures = 0, rowIds = listOf("someone-else"), header = header, row = ::sheetRow)

        runWriter(topics, group, sheets) { it.count("bookkeeper.writer.transactions.rejected.reasons", "reason", "row_not_found") >= 1.0 }

        assertEquals(listOf("it-missing"), dlqRecords(topics.dlq), "a genuinely unwritable record still dead-letters")
        assertEquals(1L, committedOffset(topics.source, group))
    }

    @Test
    fun `the full sheet is read once across two records`() {
        val topics = freshTopics()
        val group = "it-sheets-${UUID.randomUUID()}"
        publish(topics.source, tx("it-a"), tx("it-b"))
        val sheets = FakeSheets(quotaFailures = 0, rowIds = listOf("it-a", "it-b"), header = header, row = ::sheetRow)

        runWriter(topics, group, sheets) { it.count("bookkeeper.writer.transactions.written") >= 2.0 }

        // One full-sheet read plus one cell read per record. Uncached it was two per record,
        // which is what spent 60 reads/min in about three minutes.
        assertEquals(3, sheets.reads)
    }
}
