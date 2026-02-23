package org.jevy.tiller.categorizer.writer

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.jevy.tiller.categorizer.config.AppConfig
import org.jevy.tiller.categorizer.kafka.KafkaFactory
import org.jevy.tiller.categorizer.kafka.TopicNames
import org.jevy.tiller.categorizer.sheets.SheetsClient
import org.jevy.tiller_categorizer_agent.Transaction
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.LocalDate
import java.time.format.DateTimeFormatter

class CategoryWriter(
    private val config: AppConfig,
    private val sheetsClient: SheetsClient = SheetsClient(config),
    private val meterRegistry: MeterRegistry = SimpleMeterRegistry(),
) {

    private val logger = LoggerFactory.getLogger(CategoryWriter::class.java)

    private val writtenCounter = meterRegistry.counter("tiller.writer.transactions.written")
    private val skippedCounter = meterRegistry.counter("tiller.writer.transactions.skipped")
    private val errorsCounter = meterRegistry.counter("tiller.writer.errors")
    private val durationTimer = meterRegistry.timer("tiller.writer.duration")

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

    fun run() {
        val consumer = KafkaFactory.createConsumer(config, "category-writer")
        consumer.subscribe(listOf(TopicNames.CATEGORIZED))
        logger.info("Subscribed to {}", TopicNames.CATEGORIZED)

        while (true) {
            val records = consumer.poll(Duration.ofSeconds(5))
            for (record in records) {
                try {
                    durationTimer.record(Runnable { writeCategory(record.value()) })
                } catch (e: Exception) {
                    errorsCounter.increment()
                    logger.error("Error writing category for transaction {}", record.key(), e)
                }
            }
            consumer.commitSync()
        }
    }

    internal fun writeCategory(transaction: Transaction) {
        val category = transaction.getCategory()?.toString()
        if (category.isNullOrBlank()) {
            logger.warn("Transaction {} has no category, skipping", transaction.getTransactionId())
            return
        }

        val transactionId = transaction.getTransactionId().toString()
        val rowNumber = findRow(transactionId)

        if (rowNumber == null) {
            logger.warn("Could not find row for transaction {}", transactionId)
            skippedCounter.increment()
            return
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

    internal fun findRow(transactionId: String): Int? {
        val txIdCol = columnLetters["Transaction ID"] ?: "J"
        val allRows = sheetsClient.readAllRows("Transactions!${txIdCol}:${txIdCol}")
        for ((index, row) in allRows.withIndex()) {
            if (row.firstOrNull()?.toString() == transactionId) {
                return index + 1 // 1-indexed
            }
        }
        return null
    }
}
