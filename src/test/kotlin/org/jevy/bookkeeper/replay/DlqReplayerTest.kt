package org.jevy.bookkeeper.replay

import io.mockk.*
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.Node
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.TopicPartition
import org.jevy.bookkeeper.config.AppConfig
import org.jevy.bookkeeper.kafka.KafkaFactory
import org.jevy.bookkeeper.kafka.TopicNames
import org.jevy.bookkeeper_agent.Transaction
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import java.time.Duration
import kotlin.test.assertEquals
import kotlin.test.assertNull

class DlqReplayerTest {

    private val config = AppConfig(
        kafkaBootstrapServers = "localhost:9092",
        schemaRegistryUrl = "http://localhost:8081",
        googleSheetId = "",
        googleCredentialsJson = "",
        openrouterApiKey = "",
        maxTransactionAgeDays = 0,
        maxTransactions = 0,
        additionalContextPrompt = null,
        model = "",
    )

    @AfterEach
    fun tearDown() {
        unmockkAll()
    }

    private fun makeTransaction(
        id: String,
        description: String,
        category: String? = null,
        categoryJustification: String? = null,
    ): Transaction =
        Transaction.newBuilder()
            .setTransactionId(id)
            .setDate("1/1/2026")
            .setDescription(description)
            .setAmount("\$10")
            .setAccount("Visa")
            .apply {
                if (category != null) setCategory(category)
                if (categoryJustification != null) setCategoryJustification(categoryJustification)
            }
            .build()

    private fun partitionInfo(topic: String, partition: Int = 0): PartitionInfo =
        PartitionInfo(topic, partition, Node.noNode(), emptyArray(), emptyArray())

    private fun setupMocks(): Triple<KafkaConsumer<String, Transaction>, KafkaProducer<String, Transaction>, KafkaProducer<String, ByteArray?>> {
        val consumer = mockk<KafkaConsumer<String, Transaction>>(relaxed = true)
        val avroProducer = mockk<KafkaProducer<String, Transaction>>(relaxed = true)
        val tombstoneProducer = mockk<KafkaProducer<String, ByteArray?>>(relaxed = true)

        mockkObject(KafkaFactory)
        every { KafkaFactory.createConsumer(any(), "dlq-replayer") } returns consumer
        every { KafkaFactory.createProducer(any()) } returns avroProducer
        every { KafkaFactory.createTombstoneProducer(any()) } returns tombstoneProducer

        return Triple(consumer, avroProducer, tombstoneProducer)
    }

    private fun setupPartitions(
        consumer: KafkaConsumer<String, Transaction>,
        topics: List<String>,
    ): List<TopicPartition> {
        val partitions = topics.map { topic ->
            every { consumer.partitionsFor(topic) } returns listOf(partitionInfo(topic))
            TopicPartition(topic, 0)
        }
        // Return empty for any DLQ topic not in the list
        val allDlqTopics = listOf(TopicNames.CATEGORIZATION_FAILED, TopicNames.WRITE_FAILED)
        for (topic in allDlqTopics - topics.toSet()) {
            every { consumer.partitionsFor(topic) } returns emptyList()
        }
        return partitions
    }

    private fun setupFiniteConsumption(
        consumer: KafkaConsumer<String, Transaction>,
        partitions: List<TopicPartition>,
        recordsByPoll: List<ConsumerRecords<String, Transaction>>,
    ) {
        // End offsets: each partition has as many records as appear across all polls
        val endOffsets = partitions.associateWith { tp ->
            recordsByPoll.sumOf { poll ->
                poll.records(tp).count().toLong()
            }
        }
        every { consumer.endOffsets(any()) } returns endOffsets

        var pollCount = 0
        every { consumer.poll(any<Duration>()) } answers {
            pollCount++
            if (pollCount <= recordsByPoll.size) recordsByPoll[pollCount - 1]
            else ConsumerRecords(emptyMap())
        }

        // Position tracks consumed records
        val positionMap = partitions.associateWith { 0L }.toMutableMap()
        every { consumer.position(any()) } answers {
            val tp = firstArg<TopicPartition>()
            // After each poll, position advances to end
            if (pollCount > 0) endOffsets[tp] ?: 0L
            else positionMap[tp] ?: 0L
        }
    }

    private fun makeConsumerRecords(
        topic: String,
        vararg transactions: Transaction,
    ): ConsumerRecords<String, Transaction> {
        val tp = TopicPartition(topic, 0)
        val records = transactions.mapIndexed { i, tx ->
            ConsumerRecord(topic, 0, i.toLong(), tx.getTransactionId().toString(), tx)
        }
        return ConsumerRecords(mapOf(tp to records))
    }

    @Test
    fun `replays categorization-failed transactions to uncategorized`() {
        val (consumer, avroProducer, tombstoneProducer) = setupMocks()
        val partitions = setupPartitions(consumer, listOf(TopicNames.CATEGORIZATION_FAILED))

        val tx = makeTransaction("txn-1", "Failed Store")
        val records = makeConsumerRecords(TopicNames.CATEGORIZATION_FAILED, tx)
        setupFiniteConsumption(consumer, partitions, listOf(records))

        DlqReplayer(config).run()

        verify { tombstoneProducer.send(match { it.topic() == TopicNames.CATEGORIZATION_FAILED && it.key() == "txn-1" && it.value() == null }) }
        verify { avroProducer.send(match { it.topic() == TopicNames.UNCATEGORIZED && it.key() == "txn-1" }) }
        verify { avroProducer.flush() }
        verify { tombstoneProducer.flush() }
    }

    @Test
    fun `replays write-failed transactions and clears category`() {
        val (consumer, avroProducer, tombstoneProducer) = setupMocks()
        val partitions = setupPartitions(consumer, listOf(TopicNames.WRITE_FAILED))

        val tx = makeTransaction("txn-1", "Write Failed Store", category = "Groceries", categoryJustification = "was groceries")
        val records = makeConsumerRecords(TopicNames.WRITE_FAILED, tx)
        setupFiniteConsumption(consumer, partitions, listOf(records))

        val publishedSlot = slot<ProducerRecord<String, Transaction>>()
        every { avroProducer.send(capture(publishedSlot)) } returns mockk(relaxed = true)

        DlqReplayer(config).run()

        verify { tombstoneProducer.send(match { it.topic() == TopicNames.WRITE_FAILED && it.key() == "txn-1" }) }

        val published = publishedSlot.captured
        assertEquals(TopicNames.UNCATEGORIZED, published.topic())
        assertEquals("txn-1", published.key())
        assertNull(published.value().getCategory())
        assertNull(published.value().getCategoryJustification())
    }

    @Test
    fun `replays from both DLQ topics`() {
        val (consumer, avroProducer, tombstoneProducer) = setupMocks()

        val catFailedTp = TopicPartition(TopicNames.CATEGORIZATION_FAILED, 0)
        val writeFailedTp = TopicPartition(TopicNames.WRITE_FAILED, 0)
        val allPartitions = listOf(catFailedTp, writeFailedTp)

        every { consumer.partitionsFor(TopicNames.CATEGORIZATION_FAILED) } returns listOf(partitionInfo(TopicNames.CATEGORIZATION_FAILED))
        every { consumer.partitionsFor(TopicNames.WRITE_FAILED) } returns listOf(partitionInfo(TopicNames.WRITE_FAILED))

        val tx1 = makeTransaction("txn-1", "Cat Failed")
        val tx2 = makeTransaction("txn-2", "Write Failed", category = "Food")

        val catRecord = ConsumerRecord(TopicNames.CATEGORIZATION_FAILED, 0, 0L, "txn-1", tx1)
        val writeRecord = ConsumerRecord(TopicNames.WRITE_FAILED, 0, 0L, "txn-2", tx2)
        val records = ConsumerRecords(mapOf(
            catFailedTp to listOf(catRecord),
            writeFailedTp to listOf(writeRecord),
        ))

        every { consumer.endOffsets(any()) } returns mapOf(catFailedTp to 1L, writeFailedTp to 1L)
        var pollCount = 0
        every { consumer.poll(any<Duration>()) } answers {
            pollCount++
            if (pollCount == 1) records else ConsumerRecords(emptyMap())
        }
        every { consumer.position(any()) } answers {
            if (pollCount > 0) 1L else 0L
        }

        DlqReplayer(config).run()

        verify { tombstoneProducer.send(match { it.topic() == TopicNames.CATEGORIZATION_FAILED && it.key() == "txn-1" }) }
        verify { tombstoneProducer.send(match { it.topic() == TopicNames.WRITE_FAILED && it.key() == "txn-2" }) }
        verify(exactly = 2) { avroProducer.send(match { it.topic() == TopicNames.UNCATEGORIZED }) }
    }

    @Test
    fun `skips tombstone records`() {
        val (consumer, avroProducer, tombstoneProducer) = setupMocks()
        val partitions = setupPartitions(consumer, listOf(TopicNames.CATEGORIZATION_FAILED))

        val tp = TopicPartition(TopicNames.CATEGORIZATION_FAILED, 0)
        val tombstone = ConsumerRecord<String, Transaction>(TopicNames.CATEGORIZATION_FAILED, 0, 0L, "txn-1", null)
        val records = ConsumerRecords(mapOf(tp to listOf(tombstone)))

        every { consumer.endOffsets(any()) } returns mapOf(tp to 1L)
        var pollCount = 0
        every { consumer.poll(any<Duration>()) } answers {
            pollCount++
            if (pollCount == 1) records else ConsumerRecords(emptyMap())
        }
        every { consumer.position(any()) } answers { if (pollCount > 0) 1L else 0L }

        DlqReplayer(config).run()

        verify(exactly = 0) { tombstoneProducer.send(any()) }
        verify(exactly = 0) { avroProducer.send(any()) }
    }

    @Test
    fun `does nothing when DLQ topics have no records`() {
        val (consumer, avroProducer, tombstoneProducer) = setupMocks()
        val partitions = setupPartitions(consumer, listOf(TopicNames.CATEGORIZATION_FAILED, TopicNames.WRITE_FAILED))

        setupFiniteConsumption(consumer, partitions, emptyList())

        DlqReplayer(config).run()

        verify(exactly = 0) { tombstoneProducer.send(any()) }
        verify(exactly = 0) { avroProducer.send(any()) }
    }

    @Test
    fun `does nothing when no DLQ partitions exist`() {
        val (consumer, avroProducer, tombstoneProducer) = setupMocks()

        every { consumer.partitionsFor(TopicNames.CATEGORIZATION_FAILED) } returns emptyList()
        every { consumer.partitionsFor(TopicNames.WRITE_FAILED) } returns emptyList()

        DlqReplayer(config).run()

        verify(exactly = 0) { consumer.assign(any()) }
        verify(exactly = 0) { avroProducer.send(any()) }
        verify(exactly = 0) { tombstoneProducer.send(any()) }
    }

    @Test
    fun `preserves transaction fields other than category`() {
        val (consumer, avroProducer, _) = setupMocks()
        val partitions = setupPartitions(consumer, listOf(TopicNames.WRITE_FAILED))

        val tx = Transaction.newBuilder()
            .setTransactionId("txn-99")
            .setDate("3/15/2026")
            .setDescription("Costco Wholesale")
            .setAmount("-\$384.91")
            .setAccount("Visa")
            .setInstitution("TD")
            .setCategory("Groceries")
            .setCategoryJustification("bulk store")
            .build()

        val records = makeConsumerRecords(TopicNames.WRITE_FAILED, tx)
        setupFiniteConsumption(consumer, partitions, listOf(records))

        val publishedSlot = slot<ProducerRecord<String, Transaction>>()
        every { avroProducer.send(capture(publishedSlot)) } returns mockk(relaxed = true)

        DlqReplayer(config).run()

        val published = publishedSlot.captured.value()
        assertEquals("txn-99", published.getTransactionId().toString())
        assertEquals("3/15/2026", published.getDate().toString())
        assertEquals("Costco Wholesale", published.getDescription().toString())
        assertEquals("-\$384.91", published.getAmount().toString())
        assertEquals("Visa", published.getAccount().toString())
        assertEquals("TD", published.getInstitution().toString())
        assertNull(published.getCategory())
        assertNull(published.getCategoryJustification())
    }
}
