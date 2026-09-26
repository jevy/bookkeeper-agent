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
        override val sourceTopic = "fake.source"
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
        val tp = TopicPartition("fake.source", 0)
        val records = ConsumerRecords(mapOf(tp to listOf(ConsumerRecord("fake.source", 0, 0L, id, tx(id)))))
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
    fun `Skipped commits, counts skipped untagged and by reason, no DLQ, tombstones when enabled`() {
        pollOnce("txn-1")
        runUntilStopped(SinkWriter(config, FakeSink(SinkResult.Skipped("already_categorized")), registry, tombstoneUncategorized = true), mutableListOf())
        verify(exactly = 1) { consumer.commitSync() }
        verify(exactly = 0) { dlqProducer.send(any()) }
        // Same as the pre-split CategoryWriter: any non-throwing return tombstoned uncategorized.
        verify { tombstoneProducer.send(match { it.topic() == TopicNames.UNCATEGORIZED && it.key() == "txn-1" && it.value() == null }) }
        assertEquals(1.0, registry.counter("bookkeeper.fake.transactions.skipped").count())
        assertEquals(1.0, registry.counter("bookkeeper.fake.transactions.skipped.reasons", "reason", "already_categorized").count())
    }

    @Test
    fun `untagged counters exist at zero before any record, so scrapes never see an absent series`() {
        every { consumer.poll(any<Duration>()) } throws InterruptedException("stop")
        runUntilStopped(SinkWriter(config, FakeSink(SinkResult.Written), registry), mutableListOf())
        assertEquals(0.0, registry.counter("bookkeeper.fake.transactions.written").count())
        assertEquals(0.0, registry.counter("bookkeeper.fake.transactions.skipped").count())
        assertEquals(0.0, registry.counter("bookkeeper.fake.transactions.rejected").count())
        assertEquals(0.0, registry.counter("bookkeeper.fake.errors").count())
    }

    @Test
    fun `flushes producers before committing so a committed offset never outruns its publish`() {
        pollOnce("txn-1")
        runUntilStopped(SinkWriter(config, FakeSink(SinkResult.Written), registry, tombstoneUncategorized = true, publishOnDone = "next.topic"), mutableListOf())
        verifyOrder {
            dlqProducer.send(any())
            dlqProducer.flush()
            tombstoneProducer.flush()
            consumer.commitSync()
        }
    }

    @Test
    fun `Rejected sends to sink DLQ, commits, tombstones when enabled`() {
        pollOnce("txn-1")
        runUntilStopped(SinkWriter(config, FakeSink(SinkResult.Rejected("no_match")), registry, tombstoneUncategorized = true), mutableListOf())
        verify { dlqProducer.send(match { it.topic() == "fake.dlq" && it.key() == "txn-1" && it.value() != null }) }
        verify { tombstoneProducer.send(match { it.topic() == TopicNames.UNCATEGORIZED && it.key() == "txn-1" }) }
        verify(exactly = 1) { consumer.commitSync() }
        assertEquals(1.0, registry.counter("bookkeeper.fake.transactions.rejected").count())
        assertEquals(1.0, registry.counter("bookkeeper.fake.transactions.rejected.reasons", "reason", "no_match").count())
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
    fun `subscribes with the sink consumer group and source topic`() {
        pollOnce("txn-1")
        runUntilStopped(SinkWriter(config, FakeSink(SinkResult.Written), registry), mutableListOf())
        verify { KafkaFactory.createConsumer(config, "fake-group") }
        verify { consumer.subscribe(listOf("fake.source")) }
    }

    @Test
    fun `publishOnDone republishes the record unchanged on Written`() {
        pollOnce("txn-1")
        runUntilStopped(SinkWriter(config, FakeSink(SinkResult.Written), registry, publishOnDone = "next.topic"), mutableListOf())
        verify(exactly = 1) { dlqProducer.send(match { it.topic() == "next.topic" && it.key() == "txn-1" && it.value()?.getCategory()?.toString() == "Groceries" }) }
    }

    @Test
    fun `publishOnDone republishes on Skipped landed but not on Skipped not landed`() {
        pollOnce("txn-1")
        runUntilStopped(SinkWriter(config, FakeSink(SinkResult.Skipped("already_categorized", landed = true)), registry, publishOnDone = "next.topic"), mutableListOf())
        verify(exactly = 1) { dlqProducer.send(match { it.topic() == "next.topic" && it.key() == "txn-1" }) }

        clearMocks(dlqProducer)
        pollOnce("txn-2")
        runUntilStopped(SinkWriter(config, FakeSink(SinkResult.Skipped("no_category")), registry, publishOnDone = "next.topic"), mutableListOf())
        verify(exactly = 0) { dlqProducer.send(any()) }
    }

    @Test
    fun `publishOnDone never fires on Rejected and never fires when unset`() {
        pollOnce("txn-1")
        runUntilStopped(SinkWriter(config, FakeSink(SinkResult.Rejected("no_match")), registry, publishOnDone = "next.topic"), mutableListOf())
        verify(exactly = 0) { dlqProducer.send(match { it.topic() == "next.topic" }) }

        clearMocks(dlqProducer)
        pollOnce("txn-2")
        runUntilStopped(SinkWriter(config, FakeSink(SinkResult.Written), registry), mutableListOf())
        verify(exactly = 0) { dlqProducer.send(any()) }
    }
}
