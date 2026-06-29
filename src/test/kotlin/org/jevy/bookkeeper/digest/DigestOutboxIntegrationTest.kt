package org.jevy.bookkeeper.digest

import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.jevy.bookkeeper.config.AppConfig
import org.jevy.bookkeeper.kafka.KafkaFactory
import org.jevy.bookkeeper.kafka.TopicInitializer
import org.jevy.bookkeeper.kafka.TopicNames
import org.jevy.bookkeeper_agent.Transaction
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * End-to-end verification of the digest outbox against a real Redpanda broker.
 *
 * Injects Avro events into [TopicNames.PENDING_DIGEST] and exercises the real
 * [DigestSender.readPendingDigest] + tombstone round-trip — proving compaction-key
 * semantics that the MockK unit tests in [DigestSenderTest] cannot.
 *
 * Self-skips when no broker is reachable (so `./gradlew build` stays green in CI).
 * To run locally: `docker compose up -d redpanda` then `./gradlew test`.
 * Override the broker with KAFKA_BOOTSTRAP_SERVERS / SCHEMA_REGISTRY_URL.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DigestOutboxIntegrationTest {

    private val bootstrap = System.getenv("KAFKA_BOOTSTRAP_SERVERS") ?: "localhost:19092"
    private val schemaRegistry = System.getenv("SCHEMA_REGISTRY_URL") ?: "http://localhost:18081"

    private val config = AppConfig(
        kafkaBootstrapServers = bootstrap,
        schemaRegistryUrl = schemaRegistry,
        googleSheetId = "",
        googleCredentialsJson = "",
        openrouterApiKey = "",
        maxTransactionAgeDays = 365,
        maxTransactions = 0,
        additionalContextPrompt = null,
        model = "",
    )

    private val sender = DigestSender(config)

    @BeforeAll
    fun setup() {
        assumeTrue(brokerReachable(), "No Kafka broker at $bootstrap — skipping outbox integration test")
        TopicInitializer.run(bootstrap)
    }

    private fun brokerReachable(): Boolean = try {
        AdminClient.create(
            mapOf(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrap,
                AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG to "3000",
                AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG to "3000",
            )
        ).use { it.listTopics().names().get() }
        true
    } catch (e: Exception) {
        false
    }

    private fun makeTx(id: String, category: String, categorizationDate: String? = null): Transaction =
        Transaction.newBuilder()
            .setTransactionId(id)
            .setDate("3/1/2026")
            .setDescription("MERCHANT $id")
            .setCategory(category)
            .setAmount("-\$10.00")
            .setAccount("Visa")
            .setCategorizationDate(categorizationDate)
            .build()

    private fun produce(block: (KafkaProducer<String, Transaction>) -> Unit) {
        KafkaFactory.createProducer(config).use { p -> block(p); p.flush() }
    }

    private fun tombstone(vararg ids: String) {
        KafkaFactory.createTombstoneProducer(config).use { p ->
            ids.forEach { p.send(ProducerRecord(TopicNames.PENDING_DIGEST, it, null)) }
            p.flush()
        }
    }

    private fun pendingIds(): Set<String> =
        sender.readPendingDigest().map { it.getTransactionId().toString() }.toSet()

    @Test
    fun `categorized txn appears in pending digest regardless of date, then tombstone removes it`() {
        val deadZone = "int-deadzone-1" // a 2020 date the old filter would have dropped
        val normal = "int-normal-1"
        produce { p ->
            // same key -> same partition, so per-key offset order is preserved
            p.send(ProducerRecord(TopicNames.PENDING_DIGEST, deadZone, makeTx(deadZone, "Groceries", "1/1/2020")))
            p.send(ProducerRecord(TopicNames.PENDING_DIGEST, normal, makeTx(normal, "Gas", "3/1/2026")))
        }

        val afterProduce = pendingIds()
        assertTrue(deadZone in afterProduce, "dead-zone txn must be pending (the regression)")
        assertTrue(normal in afterProduce)

        // Simulate a confirmed send: tombstone both ids (what DigestSender does after sendEmail)
        tombstone(deadZone, normal)

        val afterTombstone = pendingIds()
        assertFalse(deadZone in afterTombstone, "tombstoned txn must drop out — idempotent next run")
        assertFalse(normal in afterTombstone)
    }

    @Test
    fun `re-categorization after a tombstone re-queues the corrected txn`() {
        val id = "int-recat-1"
        produce { p -> p.send(ProducerRecord(TopicNames.PENDING_DIGEST, id, makeTx(id, "Groceries"))) }
        tombstone(id) // digested
        assertFalse(id in pendingIds())

        // Correction re-publishes -> categorizer re-writes the key with the new category
        produce { p -> p.send(ProducerRecord(TopicNames.PENDING_DIGEST, id, makeTx(id, "Household"))) }

        val pending = sender.readPendingDigest().filter { it.getTransactionId().toString() == id }
        assertEquals(1, pending.size, "corrected txn must re-appear")
        assertEquals("Household", pending[0].getCategory().toString())
    }

    @Test
    fun `latest write wins for a re-categorized key before any send`() {
        val id = "int-latest-1"
        produce { p ->
            p.send(ProducerRecord(TopicNames.PENDING_DIGEST, id, makeTx(id, "Groceries")))
            p.send(ProducerRecord(TopicNames.PENDING_DIGEST, id, makeTx(id, "Dining")))
        }
        val pending = sender.readPendingDigest().filter { it.getTransactionId().toString() == id }
        assertEquals(1, pending.size, "a key collapses to one entry")
        assertEquals("Dining", pending[0].getCategory().toString())
    }
}
