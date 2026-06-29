package org.jevy.bookkeeper.digest

import io.mockk.every
import io.mockk.mockk
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.Node
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.TopicPartition
import org.jevy.bookkeeper.config.AppConfig
import org.jevy.bookkeeper.kafka.TopicNames
import org.jevy.bookkeeper_agent.Transaction
import org.junit.jupiter.api.Test
import java.time.Duration
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class DigestSenderTest {

    private val config = AppConfig(
        kafkaBootstrapServers = "localhost:9092",
        schemaRegistryUrl = "http://localhost:8081",
        googleSheetId = "test",
        googleCredentialsJson = "{}",
        openrouterApiKey = "",
        maxTransactionAgeDays = 365,
        maxTransactions = 0,
        additionalContextPrompt = null,
        model = "anthropic/claude-sonnet-4-6",
        s3Bucket = "test-bucket",
        sesFromAddress = "digest@test.com",
        digestToAddress = "user@test.com",
    )

    private val sender = DigestSender(config)

    private fun makeTx(
        description: String = "COSTCO WHOLESAL",
        amount: String = "-\$384.91",
        category: String = "Groceries",
        transactionId: String = "txn-100",
        categorizationDate: String? = null,
    ): Transaction = Transaction.newBuilder()
        .setTransactionId(transactionId)
        .setDate("3/1/2026")
        .setDescription(description)
        .setCategory(category)
        .setAmount(amount)
        .setAccount("Visa")
        .setCategorizationDate(categorizationDate)
        .build()

    @Test
    fun `formatDigestBody includes all transactions numbered`() {
        val transactions = listOf(
            makeTx("COSTCO WHOLESAL", "-\$384.91", "Groceries", "txn-1"),
            makeTx("NETFLIX.COM", "-\$22.99", "Entertainment", "txn-2"),
            makeTx("SHELL OIL 57442", "-\$65.30", "Auto & Transport", "txn-3"),
        )

        val body = sender.formatDigestBody(transactions)

        assertTrue(body.contains("1."))
        assertTrue(body.contains("2."))
        assertTrue(body.contains("3."))
        assertTrue(body.contains("COSTCO WHOLESAL"))
        assertTrue(body.contains("NETFLIX.COM"))
        assertTrue(body.contains("SHELL OIL 57442"))
        assertTrue(body.contains("Groceries"))
        assertTrue(body.contains("Entertainment"))
        assertTrue(body.contains("Auto & Transport"))
        assertTrue(body.contains("To correct, reply with the number and your context:"))
    }

    @Test
    fun `formatDigestBody truncates long descriptions`() {
        val transactions = listOf(
            makeTx("A VERY LONG MERCHANT DESCRIPTION THAT EXCEEDS LIMIT", "-\$10.00", "Shopping", "txn-1"),
        )

        val body = sender.formatDigestBody(transactions)
        // Description should be truncated to 25 chars
        val lines = body.lines().filter { it.contains("1.") }
        assertTrue(lines.isNotEmpty())
    }

    @Test
    fun `formatDigestBody handles empty transactions`() {
        val body = sender.formatDigestBody(emptyList())
        assertTrue(body.contains("Here are your pending categorizations:"))
        assertTrue(body.contains("To correct, reply with the number and your context:"))
    }

    @Test
    fun `collapseToLatest keeps latest non-null value per key`() {
        val v1 = makeTx(transactionId = "txn-1", category = "Groceries")
        val v2 = makeTx(transactionId = "txn-1", category = "Household")
        val other = makeTx(transactionId = "txn-2", category = "Gas")

        val result = sender.collapseToLatest(listOf("txn-1" to v1, "txn-2" to other, "txn-1" to v2))

        assertEquals(2, result.size)
        val byId = result.associateBy { it.getTransactionId().toString() }
        // re-categorization wins: latest value for txn-1 is Household
        assertEquals("Household", byId["txn-1"]?.getCategory()?.toString())
        assertEquals("Gas", byId["txn-2"]?.getCategory()?.toString())
    }

    @Test
    fun `collapseToLatest excludes tombstoned keys (idempotency after send)`() {
        val v1 = makeTx(transactionId = "txn-1")
        val v2 = makeTx(transactionId = "txn-2")

        // txn-1 was digested then tombstoned; txn-2 still pending
        val result = sender.collapseToLatest(listOf("txn-1" to v1, "txn-2" to v2, "txn-1" to null))

        assertEquals(1, result.size)
        assertEquals("txn-2", result[0].getTransactionId().toString())
    }

    @Test
    fun `collapseToLatest re-includes a re-categorized key after its tombstone`() {
        val original = makeTx(transactionId = "txn-1", category = "Groceries")
        val corrected = makeTx(transactionId = "txn-1", category = "Household")

        // sent (categorized) -> tombstoned -> re-categorized with correction
        val result = sender.collapseToLatest(
            listOf("txn-1" to original, "txn-1" to null, "txn-1" to corrected),
        )

        assertEquals(1, result.size)
        assertEquals("Household", result[0].getCategory()?.toString())
    }

    @Test
    fun `collapseToLatest preserves first-seen order`() {
        val a = makeTx(transactionId = "txn-a")
        val b = makeTx(transactionId = "txn-b")
        val c = makeTx(transactionId = "txn-c")

        val result = sender.collapseToLatest(listOf("txn-a" to a, "txn-b" to b, "txn-c" to c, "txn-a" to a))

        assertEquals(listOf("txn-a", "txn-b", "txn-c"), result.map { it.getTransactionId().toString() })
    }

    @Test
    fun `readPendingDigest reads the whole topic regardless of categorization date`() {
        // Regression: a txn categorized in the 00:00-07:00 dead zone (any date) must still appear.
        val tp = TopicPartition(TopicNames.PENDING_DIGEST, 0)
        val consumer = mockk<KafkaConsumer<String, Transaction>>(relaxed = true)
        every { consumer.partitionsFor(TopicNames.PENDING_DIGEST) } returns
            listOf(PartitionInfo(TopicNames.PENDING_DIGEST, 0, Node.noNode(), emptyArray(), emptyArray()))
        every { consumer.endOffsets(any()) } returns mapOf(tp to 2L)

        // A categorization with an "old" date that the previous date-filter would have dropped,
        // plus a tombstoned key that must be excluded.
        val deadZoneTx = makeTx(transactionId = "txn-deadzone", categorizationDate = "1/1/2020")
        val sentTx = makeTx(transactionId = "txn-sent")
        val records = ConsumerRecords(
            mapOf(
                tp to listOf(
                    ConsumerRecord(TopicNames.PENDING_DIGEST, 0, 0L, "txn-deadzone", deadZoneTx),
                    ConsumerRecord(TopicNames.PENDING_DIGEST, 0, 1L, "txn-sent", sentTx),
                    ConsumerRecord<String, Transaction>(TopicNames.PENDING_DIGEST, 0, 2L, "txn-sent", null),
                ),
            ),
        )
        var pollCount = 0
        every { consumer.poll(any<Duration>()) } answers {
            pollCount++
            if (pollCount == 1) records else ConsumerRecords(emptyMap())
        }
        every { consumer.position(any()) } answers { if (pollCount > 0) 2L else 0L }

        val pending = sender.readPendingDigest(consumer)

        assertEquals(1, pending.size)
        assertEquals("txn-deadzone", pending[0].getTransactionId().toString())
    }

    @Test
    fun `readPendingDigest returns empty when topic has no partitions`() {
        val consumer = mockk<KafkaConsumer<String, Transaction>>(relaxed = true)
        every { consumer.partitionsFor(TopicNames.PENDING_DIGEST) } returns null

        assertEquals(emptyList(), sender.readPendingDigest(consumer))
    }
}
