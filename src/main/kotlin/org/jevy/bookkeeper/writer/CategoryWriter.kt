package org.jevy.bookkeeper.writer

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.apache.kafka.clients.producer.ProducerRecord
import org.jevy.bookkeeper.DurableTransactionId
import org.jevy.bookkeeper.config.AppConfig
import org.jevy.bookkeeper.kafka.KafkaFactory
import org.jevy.bookkeeper.kafka.TopicNames
import org.jevy.bookkeeper.sheets.SheetsClient
import org.jevy.bookkeeper_agent.Transaction
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.LocalDate
import java.time.format.DateTimeFormatter

class RowNotFoundException(message: String) : RuntimeException(message)

class CategoryWriter(
    private val config: AppConfig,
    private val sheetsClient: SheetsClient = SheetsClient(config),
    private val meterRegistry: MeterRegistry = SimpleMeterRegistry(),
) {

    private val logger = LoggerFactory.getLogger(CategoryWriter::class.java)

    private val writtenCounter = meterRegistry.counter("bookkeeper.writer.transactions.written")
    private val skippedCounter = meterRegistry.counter("bookkeeper.writer.transactions.skipped")
    private val errorsCounter = meterRegistry.counter("bookkeeper.writer.errors")
    private val durationTimer = meterRegistry.timer("bookkeeper.writer.duration")

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

    fun run(onActivity: () -> Unit = {}, onAlive: (Boolean) -> Unit = {}) {
        val consumer = KafkaFactory.createConsumer(config, "category-writer")
        val tombstoneProducer = KafkaFactory.createTombstoneProducer(config)
        val dlqProducer = KafkaFactory.createProducer(config)
        consumer.subscribe(listOf(TopicNames.CATEGORIZED))
        logger.info("Subscribed to {}", TopicNames.CATEGORIZED)

        onAlive(true)

        try {
            while (true) {
                val records = consumer.poll(Duration.ofSeconds(5))
                onActivity()
                for (record in records) {
                    val transactionId = record.key()
                    try {
                        durationTimer.record(Runnable { writeCategory(record.value()) })
                        tombstoneProducer.send(ProducerRecord(TopicNames.UNCATEGORIZED, transactionId, null))
                        logger.debug("Tombstoned transaction {} from uncategorized", transactionId)
                    } catch (e: RowNotFoundException) {
                        errorsCounter.increment()
                        logger.error("Row not found for transaction {}, sending to write-failed DLQ and tombstoning from uncategorized", transactionId, e)
                        dlqProducer.send(ProducerRecord(TopicNames.WRITE_FAILED, transactionId, record.value()))
                        tombstoneProducer.send(ProducerRecord(TopicNames.UNCATEGORIZED, transactionId, null))
                    } catch (e: Exception) {
                        errorsCounter.increment()
                        logger.error("Error writing category for transaction {}, sending to write-failed DLQ and tombstoning", transactionId, e)
                        dlqProducer.send(ProducerRecord(TopicNames.WRITE_FAILED, transactionId, record.value()))
                        tombstoneProducer.send(ProducerRecord(TopicNames.UNCATEGORIZED, transactionId, null))
                    }
                }
                consumer.commitSync()
            }
        } finally {
            onAlive(false)
            logger.error("Consumer loop exited — marking unhealthy")
        }
    }

    internal fun writeCategory(transaction: Transaction) {
        val category = transaction.getCategory()?.toString()
        if (category.isNullOrBlank()) {
            logger.warn("Transaction {} has no category, skipping", transaction.getTransactionId())
            return
        }

        val transactionId = transaction.getTransactionId().toString()
        val rowNumber = findRow(transaction)

        if (rowNumber == null) {
            skippedCounter.increment()
            throw RowNotFoundException("Could not find row for transaction $transactionId")
        }

        val categoryCol = columnLetters["Category"] ?: "C"
        val categorizedDateCol = columnLetters["Categorized Date"] ?: "P"

        // Check if already categorized
        val rows = sheetsClient.readAllRows("Transactions!${categoryCol}$rowNumber:${categoryCol}$rowNumber")
        val existing = rows.firstOrNull()?.firstOrNull()?.toString() ?: ""
        if (existing.isNotBlank()) {
            logger.info("Transaction {} already has category '{}', skipping", transactionId, existing)
            skippedCounter.increment()
            return
        }

        // Write category and categorized date
        sheetsClient.writeCell("Transactions!${categoryCol}$rowNumber", category)
        sheetsClient.writeCell("Transactions!${categorizedDateCol}$rowNumber", LocalDate.now().format(DateTimeFormatter.ofPattern("M/d/yyyy")))
        logger.info("Wrote category '{}' to row {} for transaction {}", category, rowNumber, transactionId)
        writtenCounter.increment()
    }

    internal fun findRow(transaction: Transaction): Int? {
        val transactionId = transaction.getTransactionId().toString()
        val isDurable = transactionId.startsWith("durable-")

        // Tier 1: scan the Transaction ID column (efficient single-column read)
        if (!isDurable) {
            val txIdCol = columnLetters["Transaction ID"] ?: "J"
            val allRows = sheetsClient.readAllRows("Transactions!${txIdCol}:${txIdCol}")
            for ((index, row) in allRows.withIndex()) {
                if (row.firstOrNull()?.toString() == transactionId) {
                    return index + 1 // 1-indexed
                }
            }
        }

        // Tier 2: content-based matching using durable ID
        val dateCol = columnLetters["Date"] ?: "A"
        val descCol = columnLetters["Description"] ?: "B"
        val amountCol = columnLetters["Amount"] ?: "D"
        val accountCol = columnLetters["Account"] ?: "E"
        // Read a range spanning all needed columns in a single API call
        val rangeStart = minOf(dateCol, descCol, amountCol, accountCol)
        val rangeEnd = maxOf(dateCol, descCol, amountCol, accountCol)
        val allRows = sheetsClient.readAllRows("Transactions!${rangeStart}:${rangeEnd}")
        if (allRows.isEmpty()) return null

        // Build column index from the range we read
        val headerRow = allRows.first().map { it.toString() }
        val colIndex = headerRow.withIndex().associate { (i, name) -> name to i }

        val expectedDurableId = DurableTransactionId.generate(transaction)

        for ((index, row) in allRows.drop(1).withIndex()) {
            val rowDate = colIndex["Date"]?.let { row.getOrNull(it)?.toString() } ?: ""
            val rowDesc = colIndex["Description"]?.let { row.getOrNull(it)?.toString() } ?: ""
            val rowAmount = colIndex["Amount"]?.let { row.getOrNull(it)?.toString() } ?: ""
            val rowAccount = colIndex["Account"]?.let { row.getOrNull(it)?.toString() } ?: ""
            val owner = transaction.getOwner()?.toString() ?: ""

            val rowDurableId = DurableTransactionId.generate(owner, rowDate, rowDesc, rowAmount, rowAccount)
            if (rowDurableId == expectedDurableId) {
                return index + 2 // 1-indexed, skip header
            }
        }

        return null
    }
}
