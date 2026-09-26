package org.jevy.bookkeeper.sure

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import okhttp3.mockwebserver.Dispatcher
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import okhttp3.mockwebserver.RecordedRequest
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.jevy.bookkeeper.config.AppConfig
import org.jevy.bookkeeper.kafka.KafkaFactory
import org.jevy.bookkeeper.kafka.TopicInitializer
import org.jevy.bookkeeper.writer.CategorySink
import org.jevy.bookkeeper.writer.SinkUnavailableException
import org.jevy.bookkeeper.writer.SinkWriter
import org.jevy.bookkeeper_agent.Transaction
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.time.Duration
import java.util.UUID
import kotlin.concurrent.thread
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Real Redpanda, fake Sure. Self-skips without a broker.
 * To run locally: `docker compose up -d redpanda` then `./gradlew test`.
 *
 * Each test gets its own source and DLQ topic so a fresh consumer group never
 * replays another test's records. The sink under test is the real [SureSink]
 * with only its topic names overridden.
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
                    request.method == "PATCH" -> { synchronized(patches) { patches += request }; MockResponse().setBody("""{"id":"s-1"}""") }
                    else -> MockResponse().setResponseCode(404)
                }
            }
        }
        sure.start()
    }

    @AfterAll
    fun teardown() { if (this::sure.isInitialized) sure.shutdown() }

    private fun adminProps() = mapOf(
        AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrap,
        AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG to "3000",
        AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG to "3000",
    )

    private fun brokerReachable(): Boolean = try {
        AdminClient.create(adminProps()).use { it.listTopics().names().get() }
        true
    } catch (e: Exception) { false }

    /** Per-test topic pair, created up front so consumers do not depend on auto-creation. */
    private class Topics(val source: String, val dlq: String)

    private fun freshTopics(): Topics {
        val id = UUID.randomUUID().toString().take(8)
        val t = Topics("it.written.$id", "it.sure-dlq.$id")
        AdminClient.create(adminProps()).use { admin ->
            admin.createTopics(listOf(NewTopic(t.source, 1, 1.toShort()), NewTopic(t.dlq, 1, 1.toShort()))).all().get()
        }
        return t
    }

    private fun sureUrl() = sure.url("/").toString().trimEnd('/')

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

    private fun publish(topic: String, vararg txs: Transaction) {
        KafkaFactory.createProducer(config(sureUrl())).use { p ->
            txs.forEach { p.send(ProducerRecord(topic, it.getTransactionId().toString(), it)).get() }
            p.flush()
        }
    }

    /**
     * Runs the real SureSink under SinkWriter on the given topics with a fresh consumer group,
     * until [stopWhen] (given the writer's registry) is true, the loop dies, or 30 s pass.
     */
    private fun runWriter(cfg: AppConfig, topics: Topics, stopWhen: (SimpleMeterRegistry) -> Boolean): Throwable? {
        val registry = SimpleMeterRegistry()
        val sink = SureSink(cfg)
        val isolated = object : CategorySink by sink {
            override val consumerGroup = "it-sure-${UUID.randomUUID()}"
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
        KafkaFactory.createConsumer(config(sureUrl()), "it-dlq-reader-${UUID.randomUUID()}").use { c ->
            val tps = c.partitionsFor(topic).map { TopicPartition(it.topic(), it.partition()) }
            c.assign(tps); c.seekToBeginning(tps)
            val keys = mutableListOf<String>()
            repeat(3) { c.poll(Duration.ofSeconds(1)).forEach { if (it.value() != null) keys += it.key() } }
            return keys
        }
    }

    @Test
    fun `an event on written produces exactly one PATCH`() {
        val topics = freshTopics()
        publish(topics.source, tx("it-match", "3/1/2026"))
        synchronized(patches) { patches.clear() }

        runWriter(config(sureUrl()), topics) { it.count("bookkeeper.sure.transactions.written") >= 1.0 }

        assertEquals(1, synchronized(patches) { patches.count { it.requestUrl!!.encodedPath == "/api/v1/transactions/s-1" } })
    }

    @Test
    fun `a match failure produces a DLQ record and no PATCH`() {
        val topics = freshTopics()
        publish(topics.source, tx("it-nomatch", "4/1/2026"))
        synchronized(patches) { patches.clear() }

        runWriter(config(sureUrl()), topics) { it.count("bookkeeper.sure.transactions.rejected", "reason", "no_match") >= 1.0 }

        assertEquals(listOf("it-nomatch"), dlqRecords(topics.dlq))
        assertEquals(0, synchronized(patches) { patches.size })
    }

    @Test
    fun `dry run performs zero PATCHes`() {
        val topics = freshTopics()
        publish(topics.source, tx("it-dry", "3/1/2026"))
        synchronized(patches) { patches.clear() }

        runWriter(config(sureUrl(), dryRun = true), topics) { it.count("bookkeeper.sure.transactions.skipped", "reason", "dry_run") >= 1.0 }

        assertEquals(0, synchronized(patches) { patches.size })
    }

    @Test
    fun `Sure unreachable exits the loop without DLQ records`() {
        val topics = freshTopics()
        publish(topics.source, tx("it-down", "3/1/2026"))

        val failure = runWriter(config("http://127.0.0.1:1"), topics) { false }

        assertTrue(failure is SinkUnavailableException, "expected SinkUnavailableException, got $failure")
        assertEquals(emptyList(), dlqRecords(topics.dlq))
    }
}
