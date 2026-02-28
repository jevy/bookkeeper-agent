package org.jevy.bookkeeper.producer

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.apache.kafka.clients.producer.ProducerRecord
import org.jevy.bookkeeper.config.AppConfig
import org.jevy.bookkeeper.kafka.KafkaFactory
import org.jevy.bookkeeper.kafka.TopicNames
import org.jevy.bookkeeper.sheets.SheetsClient
import org.jevy.bookkeeper.sheets.TransactionMapper
import org.jevy.bookkeeper_agent.Transaction
import org.slf4j.LoggerFactory
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeParseException
import java.util.concurrent.atomic.AtomicInteger

class TransactionProducer(
    private val config: AppConfig,
    private val meterRegistry: MeterRegistry = SimpleMeterRegistry(),
) {

    private val logger = LoggerFactory.getLogger(TransactionProducer::class.java)

    fun run() {
        val sheetsClient = SheetsClient(config)
        val producer = KafkaFactory.createProducer(config)

        pollAndPublish(sheetsClient, producer)
        producer.close()
    }

    private fun pollAndPublish(
        sheetsClient: SheetsClient,
        producer: org.apache.kafka.clients.producer.KafkaProducer<String, Transaction>,
    ) {
        val rows = sheetsClient.readAllRows()
        if (rows.isEmpty()) {
            logger.info("No rows found in sheet")
            return
        }

        val header = rows.first().map { it.toString() }
        val colIndex = header.withIndex().associate { (i, name) -> name to i }
        var published = 0
        var skipped = 0
        val publishErrors = AtomicInteger(0)

        for ((index, row) in rows.drop(1).withIndex()) {
            val rowNumber = index + 2 // 1-indexed, skip header
            try {
                val transaction = rowToTransaction(row, colIndex, config.maxTransactionAgeDays, config.googleSheetId)
                if (transaction == null) {
                    skipped++
                    continue
                }

                val txId = transaction.getTransactionId().toString()
                val record = ProducerRecord(TopicNames.UNCATEGORIZED, txId, transaction)
                producer.send(record) { metadata, exception ->
                    if (exception != null) {
                        publishErrors.incrementAndGet()
                        logger.error("Failed to publish transaction {}", txId, exception)
                    } else {
                        logger.debug("Published transaction {} to partition {} offset {}",
                            txId, metadata.partition(), metadata.offset())
                    }
                }

                // Write back durable ID to sheet so CategoryWriter.findRow() can locate it later
                if (txId.startsWith("durable-")) {
                    val txIdCol = colIndex["Transaction ID"]
                    if (txIdCol != null) {
                        val cellRef = "Transactions!${colLetter(txIdCol)}$rowNumber"
                        try {
                            sheetsClient.writeCell(cellRef, txId)
                            logger.debug("Wrote durable ID {} to {}", txId, cellRef)
                        } catch (e: Exception) {
                            logger.warn("Failed to write durable ID {} to sheet: {}", txId, e.message)
                        }
                    }
                }
                published++
                if (config.maxTransactions > 0 && published >= config.maxTransactions) {
                    logger.info("Reached max transactions limit ({}), stopping", config.maxTransactions)
                    break
                }
            } catch (e: Exception) {
                logger.error("Failed to process row {}", rowNumber, e)
            }
        }

        producer.flush()
        val scanned = rows.size - 1
        logger.info("Scanned {} rows: published={}, skipped={}, errors={}", scanned, published, skipped, publishErrors.get())
        meterRegistry.counter("bookkeeper.producer.rows.scanned").increment(scanned.toDouble())
        meterRegistry.counter("bookkeeper.producer.transactions.published").increment(published.toDouble())
        meterRegistry.counter("bookkeeper.producer.transactions.skipped").increment(skipped.toDouble())
        meterRegistry.counter("bookkeeper.producer.publish.errors").increment(publishErrors.get().toDouble())
    }

    companion object {
        private val skipLogger = LoggerFactory.getLogger("TransactionProducer.skip")

        private fun colLetter(index: Int): String {
            var n = index
            val sb = StringBuilder()
            while (n >= 0) {
                sb.insert(0, ('A' + n % 26))
                n = n / 26 - 1
            }
            return sb.toString()
        }

        internal fun rowToTransaction(row: List<Any>, colIndex: Map<String, Int>, maxAgeDays: Long = 365, owner: String? = null): Transaction? {
            val transaction = TransactionMapper.fromSheetRow(row, colIndex, owner)

            // Producer-specific filtering: skip categorized
            if (!transaction.getCategory().isNullOrBlank()) return null

            // Skip transactions older than maxAgeDays
            val dateStr = transaction.getDate()?.toString() ?: ""
            if (dateStr.isNotBlank()) {
                try {
                    val txDate = LocalDate.parse(dateStr, DateTimeFormatter.ofPattern("M/d/yyyy"))
                    if (txDate.isBefore(LocalDate.now().minusDays(maxAgeDays))) {
                        skipLogger.info("Skipped row: too old ({}) — id={}, desc={}", dateStr, transaction.getTransactionId(), transaction.getDescription())
                        return null
                    }
                } catch (_: DateTimeParseException) {}
            }

            // Clear category for uncategorized topic
            return Transaction.newBuilder(transaction).setCategory(null).build()
        }
    }
}
