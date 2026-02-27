package org.jevy.tiller.categorizer.categorizer

import io.mockk.*
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.TopicPartition
import org.jevy.tiller.categorizer.config.AppConfig
import org.jevy.tiller.categorizer.kafka.KafkaFactory
import org.jevy.tiller.categorizer.kafka.TopicNames
import org.jevy.tiller_categorizer_agent.Transaction
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import java.time.Duration

class CategorizerAgentTest {

    private val config = AppConfig(
        kafkaBootstrapServers = "localhost:9092",
        schemaRegistryUrl = "http://localhost:8081",
        googleSheetId = "test",
        googleCredentialsJson = "{}",
        openrouterApiKey = "test-key",
        maxTransactionAgeDays = 365,
        maxTransactions = 0,
        additionalContextPrompt = null,
        model = "test-model",
    )

    @AfterEach
    fun tearDown() {
        unmockkAll()
    }

    private fun makeTransaction(id: String, description: String, category: String? = null): Transaction =
        Transaction.newBuilder()
            .setTransactionId(id)
            .setDate("1/1/2026")
            .setDescription(description)
            .setAmount("\$10")
            .setAccount("Visa")
            .apply { if (category != null) setCategory(category) }
            .build()

    private fun makeRecords(vararg transactions: Transaction): ConsumerRecords<String, Transaction> {
        val tp = TopicPartition(TopicNames.UNCATEGORIZED, 0)
        val records = transactions.mapIndexed { i, tx ->
            ConsumerRecord(TopicNames.UNCATEGORIZED, 0, i.toLong(), tx.getTransactionId().toString(), tx)
        }
        return ConsumerRecords(mapOf(tp to records))
    }

    private fun setupMocks(): Triple<KafkaConsumer<String, Transaction>, KafkaProducer<String, Transaction>, KafkaProducer<String, ByteArray?>> {
        val consumer = mockk<KafkaConsumer<String, Transaction>>(relaxed = true)
        val producer = mockk<KafkaProducer<String, Transaction>>(relaxed = true)
        val tombstoneProducer = mockk<KafkaProducer<String, ByteArray?>>(relaxed = true)

        mockkObject(KafkaFactory)
        every { KafkaFactory.createConsumer(any(), any()) } returns consumer
        every { KafkaFactory.createProducer(any()) } returns producer
        every { KafkaFactory.createTombstoneProducer(any()) } returns tombstoneProducer

        return Triple(consumer, producer, tombstoneProducer)
    }

    @Test
    fun `commitSync is called after each poll batch`() {
        val (consumer, _, _) = setupMocks()

        val tx1 = makeTransaction("txn-1", "Store A")
        val tx2 = makeTransaction("txn-2", "Store B")

        var pollCount = 0
        every { consumer.poll(any<Duration>()) } answers {
            pollCount++
            when (pollCount) {
                1 -> makeRecords(tx1)
                2 -> makeRecords(tx2)
                else -> throw InterruptedException("stop")
            }
        }

        val agent = spyk(CategorizerAgent(config))
        every { agent.categorize(any()) } returns CategorizerAgent.CategorizationResult("Groceries", "test")

        try {
            agent.run()
        } catch (_: InterruptedException) {}

        // commitSync once per poll batch
        verify(exactly = 2) { consumer.commitSync() }

        // Each batch: poll -> process -> commit -> next poll
        verifyOrder {
            consumer.poll(any<Duration>())
            consumer.commitSync()
            consumer.poll(any<Duration>())
            consumer.commitSync()
        }
    }

    @Test
    fun `commitSync is called even when poll returns empty batch`() {
        val (consumer, _, _) = setupMocks()

        val tx = makeTransaction("txn-1", "Store A")

        var pollCount = 0
        every { consumer.poll(any<Duration>()) } answers {
            pollCount++
            when (pollCount) {
                1 -> makeRecords(tx)
                2 -> ConsumerRecords(emptyMap())
                else -> throw InterruptedException("stop")
            }
        }

        val agent = spyk(CategorizerAgent(config))
        every { agent.categorize(any()) } returns CategorizerAgent.CategorizationResult("Groceries", "test")

        try {
            agent.run()
        } catch (_: InterruptedException) {}

        // commit should happen after every poll, even empty ones
        verify(exactly = 2) { consumer.commitSync() }
    }

    @Test
    fun `categorization retries 3 times then sends to DLQ`() {
        val (consumer, producer, tombstoneProducer) = setupMocks()

        val tx = makeTransaction("txn-1", "Bad Store")

        var pollCount = 0
        every { consumer.poll(any<Duration>()) } answers {
            pollCount++
            if (pollCount == 1) makeRecords(tx) else throw InterruptedException("stop")
        }

        val agent = spyk(CategorizerAgent(config))
        every { agent.categorize(any()) } throws RuntimeException("API error")

        try {
            agent.run()
        } catch (_: InterruptedException) {}

        // categorize called exactly 3 times (MAX_RETRIES)
        verify(exactly = CategorizerAgent.MAX_RETRIES) { agent.categorize(any()) }
        // Sent to DLQ after exhausting retries
        verify { producer.send(match { it.topic() == TopicNames.CATEGORIZATION_FAILED }) }
        verify { tombstoneProducer.send(match { it.topic() == TopicNames.UNCATEGORIZED && it.value() == null }) }
        // Batch still committed
        verify(atLeast = 1) { consumer.commitSync() }
    }

    @Test
    fun `already categorized transactions are skipped and batch is committed`() {
        val (consumer, _, _) = setupMocks()

        val tx = makeTransaction("txn-1", "Test Store", category = "Groceries")

        var pollCount = 0
        every { consumer.poll(any<Duration>()) } answers {
            pollCount++
            if (pollCount == 1) makeRecords(tx) else throw InterruptedException("stop")
        }

        val agent = spyk(CategorizerAgent(config))

        try {
            agent.run()
        } catch (_: InterruptedException) {}

        verify(exactly = 0) { agent.categorize(any()) }
        verify(atLeast = 1) { consumer.commitSync() }
    }

    @Test
    fun `failed categorization sends to DLQ and tombstones uncategorized topic`() {
        val (consumer, producer, tombstoneProducer) = setupMocks()

        val tx = makeTransaction("txn-1", "Mystery Store")

        var pollCount = 0
        every { consumer.poll(any<Duration>()) } answers {
            pollCount++
            if (pollCount == 1) makeRecords(tx) else throw InterruptedException("stop")
        }

        val agent = spyk(CategorizerAgent(config))
        every { agent.categorize(any()) } returns null

        try {
            agent.run()
        } catch (_: InterruptedException) {}

        verify { producer.send(match { it.topic() == TopicNames.CATEGORIZATION_FAILED }) }
        verify { tombstoneProducer.send(match { it.topic() == TopicNames.UNCATEGORIZED && it.value() == null }) }
        verify(atLeast = 1) { consumer.commitSync() }
    }

    @Test
    fun `categorization exception on one record does not prevent next record from succeeding`() {
        val (consumer, producer, tombstoneProducer) = setupMocks()

        val tx1 = makeTransaction("txn-1", "Bad Store")
        val tx2 = makeTransaction("txn-2", "Good Store")

        var pollCount = 0
        every { consumer.poll(any<Duration>()) } answers {
            pollCount++
            when (pollCount) {
                1 -> makeRecords(tx1, tx2)
                else -> throw InterruptedException("stop")
            }
        }

        val agent = spyk(CategorizerAgent(config))
        // tx1 fails all 3 retries
        every { agent.categorize(match { it.getTransactionId() == "txn-1" }) } throws RuntimeException("API error")
        // tx2 succeeds
        every { agent.categorize(match { it.getTransactionId() == "txn-2" }) } returns
            CategorizerAgent.CategorizationResult("Groceries", "test")

        try {
            agent.run()
        } catch (_: InterruptedException) {}

        // tx1 sent to DLQ after exhausting retries
        verify { producer.send(match { it.topic() == TopicNames.CATEGORIZATION_FAILED }) }
        verify { tombstoneProducer.send(match { it.topic() == TopicNames.UNCATEGORIZED && it.value() == null }) }
        // tx2 categorized successfully
        verify { producer.send(match { it.topic() == TopicNames.CATEGORIZED }) }
        verify(atLeast = 1) { consumer.commitSync() }
    }

    @Test
    fun `categorization succeeds on retry after transient failure`() {
        val (consumer, producer, tombstoneProducer) = setupMocks()

        val tx = makeTransaction("txn-1", "Flaky Store")

        var pollCount = 0
        every { consumer.poll(any<Duration>()) } answers {
            pollCount++
            if (pollCount == 1) makeRecords(tx) else throw InterruptedException("stop")
        }

        val agent = spyk(CategorizerAgent(config))
        // Fail once, then succeed on second attempt
        var categorizeCount = 0
        every { agent.categorize(any()) } answers {
            categorizeCount++
            if (categorizeCount == 1) throw RuntimeException("Transient error")
            else CategorizerAgent.CategorizationResult("Groceries", "retry worked")
        }

        try {
            agent.run()
        } catch (_: InterruptedException) {}

        // categorize called exactly 2 times (1 failure + 1 success)
        verify(exactly = 2) { agent.categorize(any()) }
        // Sent to CATEGORIZED, not DLQ
        verify { producer.send(match { it.topic() == TopicNames.CATEGORIZED }) }
        verify(exactly = 0) { producer.send(match { it.topic() == TopicNames.CATEGORIZATION_FAILED }) }
        verify(exactly = 0) { tombstoneProducer.send(any()) }
        verify(atLeast = 1) { consumer.commitSync() }
    }

    @Test
    fun `tombstone records are skipped`() {
        val (consumer, producer, _) = setupMocks()

        val tp = TopicPartition(TopicNames.UNCATEGORIZED, 0)
        val tombstone = ConsumerRecord<String, Transaction>(TopicNames.UNCATEGORIZED, 0, 0L, "txn-1", null)
        val records = ConsumerRecords(mapOf(tp to listOf(tombstone)))

        var pollCount = 0
        every { consumer.poll(any<Duration>()) } answers {
            pollCount++
            if (pollCount == 1) records else throw InterruptedException("stop")
        }

        val agent = spyk(CategorizerAgent(config))

        try {
            agent.run()
        } catch (_: InterruptedException) {}

        verify(exactly = 0) { agent.categorize(any()) }
        verify(exactly = 0) { producer.send(any()) }
    }
}
