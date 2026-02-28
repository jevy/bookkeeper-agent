package org.jevy.bookkeeper.writer

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.apache.kafka.clients.producer.ProducerRecord
import org.jevy.bookkeeper.config.AppConfig
import org.jevy.bookkeeper.kafka.KafkaFactory
import org.jevy.bookkeeper.kafka.TopicNames
import org.jevy.bookkeeper.producer.DurableTransactionId
import org.jevy.bookkeeper.sheets.SheetTransaction
import org.jevy.bookkeeper.sheets.SheetsClient
import org.jevy.bookkeeper.sheets.TransactionMapper
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
                        logger.error("Error writing category for transaction {}, sending to write-failed DLQ", transactionId, e)
                        dlqProducer.send(ProducerRecord(TopicNames.WRITE_FAILED, transactionId, record.value()))
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

        val allRows = sheetsClient.readAllRows()
        if (allRows.isEmpty()) throw RowNotFoundException("No rows found for transaction $transactionId")

        val header = allRows.first().map { it.toString() }
        val colIndex = header.withIndex().associate { (i, name) -> name to i }
        val indexed = allRows.drop(1).mapIndexed { i, row ->
            SheetTransaction(i + 2, TransactionMapper.fromSheetRow(row, colIndex, config.googleSheetId))
        }

        val rowNumber = findRow(transaction, indexed)

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
