package org.jevy.tiller.categorizer.writer

import org.jevy.tiller.categorizer.config.AppConfig
import org.jevy.tiller.categorizer.kafka.KafkaFactory
import org.jevy.tiller.categorizer.kafka.TopicNames
import org.jevy.tiller.categorizer.sheets.SheetsClient
import org.jevy.tiller_categorizer_agent.Transaction
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.LocalDate
import java.time.format.DateTimeFormatter

class CategoryWriter(private val config: AppConfig, private val sheetsClient: SheetsClient = SheetsClient(config)) {

    private val logger = LoggerFactory.getLogger(CategoryWriter::class.java)

    fun run() {
        val consumer = KafkaFactory.createConsumer(config, "category-writer")
        consumer.subscribe(listOf(TopicNames.CATEGORIZED))
        logger.info("Subscribed to {}", TopicNames.CATEGORIZED)

        while (true) {
            val records = consumer.poll(Duration.ofSeconds(5))
            for (record in records) {
                try {
                    writeCategory(record.value())
                } catch (e: Exception) {
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
        val rowNumber = findRow(transactionId, transaction.getSheetRowNumber())

        if (rowNumber == null) {
            logger.warn("Could not find row for transaction {}", transactionId)
            return
        }

        // Check if already categorized
        val rows = sheetsClient.readAllRows("Transactions!C$rowNumber:C$rowNumber")
        val existing = rows.firstOrNull()?.firstOrNull()?.toString() ?: ""
        if (existing.isNotBlank()) {
            logger.info("Transaction {} already has category '{}', skipping", transactionId, existing)
            return
        }

        // Write category (column C) and categorized date (column P)
        sheetsClient.writeCell("Transactions!C$rowNumber", category)
        sheetsClient.writeCell("Transactions!P$rowNumber", LocalDate.now().format(DateTimeFormatter.ofPattern("M/d/yyyy")))
        logger.info("Wrote category '{}' to row {} for transaction {}", category, rowNumber, transactionId)
    }

    internal fun findRow(transactionId: String, hintRow: Int): Int? {
        // Try the hint row first
        val hintRows = sheetsClient.readAllRows("Transactions!J$hintRow:J$hintRow")
        val hintId = hintRows.firstOrNull()?.firstOrNull()?.toString() ?: ""
        if (hintId == transactionId) return hintRow

        // Fall back to scanning all rows
        logger.debug("Hint row {} didn't match, scanning for transaction {}", hintRow, transactionId)
        val allRows = sheetsClient.readAllRows("Transactions!J:J")
        for ((index, row) in allRows.withIndex()) {
            if (row.firstOrNull()?.toString() == transactionId) {
                return index + 1 // 1-indexed
            }
        }
        return null
    }
}
