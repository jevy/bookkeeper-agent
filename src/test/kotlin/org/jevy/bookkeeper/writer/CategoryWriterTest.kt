package org.jevy.bookkeeper.writer

import io.mockk.*
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.TopicPartition
import org.jevy.bookkeeper.DurableTransactionId
import org.jevy.bookkeeper.config.AppConfig
import org.jevy.bookkeeper.kafka.KafkaFactory
import org.jevy.bookkeeper.kafka.TopicNames
import org.jevy.bookkeeper.sheets.SheetsClient
import org.jevy.bookkeeper_agent.Transaction
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.Duration
import kotlin.test.assertEquals
import kotlin.test.assertNull

class CategoryWriterTest {

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
    )

    private val header = listOf<Any>(
        "Date", "Description", "Category", "Amount", "Account",
        "Account #", "Institution", "Month", "Week", "Transaction ID",
        "Check Number", "Full Description", "Note", "Receipt", "Source",
        "Categorized Date", "Date Added",
    )

    @AfterEach
    fun tearDown() {
        unmockkAll()
    }

    private val sheetsClient = mockk<SheetsClient>(relaxed = true).also {
        every { it.readAllRows("Transactions!1:1") } returns listOf(header)
    }
    private val writer = CategoryWriter(config, sheetsClient)

    private fun makeTx(
        transactionId: String,
        date: String = "1/1/2026",
        description: String = "Test",
        category: String? = null,
        amount: String = "\$10",
        account: String = "Visa",
        owner: String? = null,
    ): Transaction = Transaction.newBuilder()
        .setTransactionId(transactionId)
        .setDate(date)
        .setDescription(description)
        .setCategory(category)
        .setAmount(amount)
        .setAccount(account)
        .setOwner(owner)
        .build()

    // --- Tier 1: Transaction ID column scan (non-durable IDs) ---

    @Test
    fun `findRow returns correct row by scanning Transaction ID column`() {
        every { sheetsClient.readAllRows("Transactions!J:J") } returns listOf(
            listOf("Transaction ID" as Any), // header (row 1)
            listOf("txn-100" as Any),        // row 2
            listOf("txn-200" as Any),        // row 3
            listOf("txn-300" as Any),        // row 4
        )

        val tx = makeTx("txn-300")
        val row = writer.findRow(tx)

        assertEquals(4, row)
    }

    @Test
    fun `findRow returns null when Yodlee ID not found and content does not match`() {
        every { sheetsClient.readAllRows("Transactions!J:J") } returns listOf(
            listOf("Transaction ID" as Any),
            listOf("txn-100" as Any),
        )
        // Tier 2 fallback — content won't match
        every { sheetsClient.readAllRows("Transactions!A:E") } returns listOf(
            listOf("Date" as Any, "Description" as Any, "Category" as Any, "Amount" as Any, "Account" as Any),
            listOf("1/1/2026" as Any, "Other" as Any, "" as Any, "\$99" as Any, "Amex" as Any),
        )

        val tx = makeTx("txn-missing")
        val row = writer.findRow(tx)

        assertNull(row)
    }

    // --- Tier 2: Content-based matching (durable IDs) ---

    @Test
    fun `findRow matches durable ID by content columns`() {
        val tx = makeTx(
            transactionId = "durable-abc123",
            date = "2/15/2026",
            description = "COSTCO WHOLESAL",
            amount = "-\$384.91",
            account = "Chequing",
            owner = "test-owner",
        )

        // Tier 2 reads content columns A:E
        every { sheetsClient.readAllRows("Transactions!A:E") } returns listOf(
            listOf("Date" as Any, "Description" as Any, "Category" as Any, "Amount" as Any, "Account" as Any),
            listOf("1/1/2026" as Any, "Unrelated" as Any, "" as Any, "\$10" as Any, "Visa" as Any),         // row 2 — no match
            listOf("2/15/2026" as Any, "COSTCO WHOLESAL" as Any, "" as Any, "-\$384.91" as Any, "Chequing" as Any), // row 3 — match
        )

        val row = writer.findRow(tx)

        assertEquals(3, row)
    }

    @Test
    fun `findRow skips tier 1 for durable IDs`() {
        val tx = makeTx(
            transactionId = "durable-abc123",
            date = "1/1/2026",
            description = "Test",
            amount = "\$10",
            account = "Visa",
        )

        every { sheetsClient.readAllRows("Transactions!A:E") } returns listOf(
            listOf("Date" as Any, "Description" as Any, "Category" as Any, "Amount" as Any, "Account" as Any),
            listOf("1/1/2026" as Any, "Test" as Any, "" as Any, "\$10" as Any, "Visa" as Any),
        )

        writer.findRow(tx)

        // Should NOT read Transaction ID column for durable IDs
        verify(exactly = 0) { sheetsClient.readAllRows("Transactions!J:J") }
    }

    @Test
    fun `findRow returns null when durable ID content does not match any row`() {
        val tx = makeTx(
            transactionId = "durable-abc123",
            date = "3/1/2026",
            description = "Unique Store",
            amount = "-\$50.00",
            account = "Savings",
        )

        every { sheetsClient.readAllRows("Transactions!A:E") } returns listOf(
            listOf("Date" as Any, "Description" as Any, "Category" as Any, "Amount" as Any, "Account" as Any),
            listOf("1/1/2026" as Any, "Other" as Any, "" as Any, "\$10" as Any, "Visa" as Any),
        )

        val row = writer.findRow(tx)

        assertNull(row)
    }

    // --- writeCategory integration ---

    @Test
    fun `writeCategory throws RowNotFoundException when row not found`() {
        val tx = makeTx("txn-missing", category = "Groceries")

        every { sheetsClient.readAllRows("Transactions!J:J") } returns listOf(
            listOf("Transaction ID" as Any),
            listOf("txn-100" as Any),
        )
        every { sheetsClient.readAllRows("Transactions!A:E") } returns listOf(
            listOf("Date" as Any, "Description" as Any, "Category" as Any, "Amount" as Any, "Account" as Any),
            listOf("9/9/2025" as Any, "Nope" as Any, "" as Any, "\$999" as Any, "Other" as Any),
        )

        assertThrows<RowNotFoundException> { writer.writeCategory(tx) }
    }

    @Test
    fun `writeCategory skips when row already has category`() {
        val tx = makeTx("txn-100", category = "Groceries")

        every { sheetsClient.readAllRows("Transactions!J:J") } returns listOf(
            listOf("Transaction ID" as Any),
            listOf("txn-100" as Any), // row 2
        )
        every { sheetsClient.readAllRows("Transactions!C2:C2") } returns
            listOf(listOf("Existing Category" as Any))

        writer.writeCategory(tx)

        verify(exactly = 0) { sheetsClient.writeCell(any(), any()) }
    }

    @Test
    fun `writeCategory writes category and date when row is empty`() {
        val tx = makeTx("txn-100", category = "Groceries")

        every { sheetsClient.readAllRows("Transactions!J:J") } returns listOf(
            listOf("Transaction ID" as Any),
            listOf("txn-100" as Any), // row 2
        )
        every { sheetsClient.readAllRows("Transactions!C2:C2") } returns
            listOf(listOf("" as Any))

        writer.writeCategory(tx)

        verify { sheetsClient.writeCell("Transactions!C2", "Groceries") }
        verify { sheetsClient.writeCell(match { it.startsWith("Transactions!P2") }, any()) }
    }

    @Test
    fun `writeCategory works with durable ID via content matching`() {
        val tx = makeTx(
            transactionId = "durable-abc123",
            date = "2/15/2026",
            description = "COSTCO WHOLESAL",
            category = "Groceries",
            amount = "-\$384.91",
            account = "Chequing",
            owner = "test-owner",
        )

        every { sheetsClient.readAllRows("Transactions!A:E") } returns listOf(
            listOf("Date" as Any, "Description" as Any, "Category" as Any, "Amount" as Any, "Account" as Any),
            listOf("2/15/2026" as Any, "COSTCO WHOLESAL" as Any, "" as Any, "-\$384.91" as Any, "Chequing" as Any), // row 2
        )
        every { sheetsClient.readAllRows("Transactions!C2:C2") } returns
            listOf(listOf("" as Any))

        writer.writeCategory(tx)

        verify { sheetsClient.writeCell("Transactions!C2", "Groceries") }
        verify { sheetsClient.writeCell(match { it.startsWith("Transactions!P2") }, any()) }
    }

    // --- run() integration ---

    @Test
    fun `run sends to DLQ and tombstones uncategorized when row not found`() {
        val consumer = mockk<KafkaConsumer<String, Transaction>>(relaxed = true)
        val dlqProducer = mockk<KafkaProducer<String, Transaction>>(relaxed = true)
        val tombstoneProducer = mockk<KafkaProducer<String, ByteArray?>>(relaxed = true)

        mockkObject(KafkaFactory)
        every { KafkaFactory.createConsumer(any(), any()) } returns consumer
        every { KafkaFactory.createProducer(any()) } returns dlqProducer
        every { KafkaFactory.createTombstoneProducer(any()) } returns tombstoneProducer

        val tx = Transaction.newBuilder()
            .setTransactionId("txn-missing")
            .setDate("1/1/2026")
            .setDescription("Test")
            .setCategory("Groceries")
            .setAmount("\$10")
            .setAccount("Visa")
            .build()

        val tp = TopicPartition(TopicNames.CATEGORIZED, 0)
        val record = ConsumerRecord(TopicNames.CATEGORIZED, 0, 0L, "txn-missing", tx)
        val records = ConsumerRecords(mapOf(tp to listOf(record)))

        var pollCount = 0
        every { consumer.poll(any<Duration>()) } answers {
            pollCount++
            if (pollCount == 1) records else throw InterruptedException("stop")
        }

        // Row not found — tier 1 scan misses, tier 2 content mismatch
        every { sheetsClient.readAllRows("Transactions!J:J") } returns listOf(
            listOf("Transaction ID" as Any),
            listOf("txn-100" as Any),
        )
        every { sheetsClient.readAllRows("Transactions!A:E") } returns listOf(
            listOf("Date" as Any, "Description" as Any, "Category" as Any, "Amount" as Any, "Account" as Any),
            listOf("9/9/2025" as Any, "Nope" as Any, "" as Any, "\$999" as Any, "Other" as Any),
        )

        val writer = CategoryWriter(config, sheetsClient)

        try {
            writer.run()
        } catch (_: InterruptedException) {}

        // Sent to write-failed DLQ
        verify { dlqProducer.send(match { it.topic() == TopicNames.WRITE_FAILED && it.key() == "txn-missing" }) }
        // Tombstoned from uncategorized to break the loop
        verify { tombstoneProducer.send(match { it.topic() == TopicNames.UNCATEGORIZED && it.key() == "txn-missing" && it.value() == null }) }
        // Batch still committed
        verify(atLeast = 1) { consumer.commitSync() }
    }

    @Test
    fun `run sends to DLQ and tombstones uncategorized on generic exception`() {
        val consumer = mockk<KafkaConsumer<String, Transaction>>(relaxed = true)
        val dlqProducer = mockk<KafkaProducer<String, Transaction>>(relaxed = true)
        val tombstoneProducer = mockk<KafkaProducer<String, ByteArray?>>(relaxed = true)

        mockkObject(KafkaFactory)
        every { KafkaFactory.createConsumer(any(), any()) } returns consumer
        every { KafkaFactory.createProducer(any()) } returns dlqProducer
        every { KafkaFactory.createTombstoneProducer(any()) } returns tombstoneProducer

        val tx = Transaction.newBuilder()
            .setTransactionId("txn-100")
            .setDate("1/1/2026")
            .setDescription("Test")
            .setCategory("Groceries")
            .setAmount("\$10")
            .setAccount("Visa")
            .build()

        val tp = TopicPartition(TopicNames.CATEGORIZED, 0)
        val record = ConsumerRecord(TopicNames.CATEGORIZED, 0, 0L, "txn-100", tx)
        val records = ConsumerRecords(mapOf(tp to listOf(record)))

        var pollCount = 0
        every { consumer.poll(any<Duration>()) } answers {
            pollCount++
            if (pollCount == 1) records else throw InterruptedException("stop")
        }

        // Row found but writeCell throws a generic exception
        every { sheetsClient.readAllRows("Transactions!J:J") } returns listOf(
            listOf("Transaction ID" as Any),
            listOf("txn-100" as Any), // row 2
        )
        every { sheetsClient.readAllRows("Transactions!C2:C2") } returns
            listOf(listOf("" as Any))
        every { sheetsClient.writeCell(any(), any()) } throws RuntimeException("Sheets API 500")

        val writer = CategoryWriter(config, sheetsClient)

        try {
            writer.run()
        } catch (_: InterruptedException) {}

        // Sent to write-failed DLQ
        verify { dlqProducer.send(match { it.topic() == TopicNames.WRITE_FAILED && it.key() == "txn-100" }) }
        // Tombstoned from uncategorized (new behavior — generic exceptions also tombstone)
        verify { tombstoneProducer.send(match { it.topic() == TopicNames.UNCATEGORIZED && it.key() == "txn-100" && it.value() == null }) }
        verify(atLeast = 1) { consumer.commitSync() }
    }
}
