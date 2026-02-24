package org.jevy.tiller.categorizer.producer

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.apache.kafka.clients.producer.ProducerRecord
import org.jevy.tiller.categorizer.config.AppConfig
import org.jevy.tiller.categorizer.kafka.KafkaFactory
import org.jevy.tiller.categorizer.kafka.TopicNames
import org.jevy.tiller.categorizer.sheets.SheetsClient
import org.jevy.tiller_categorizer_agent.Transaction
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

                val record = ProducerRecord(TopicNames.UNCATEGORIZED, transaction.getTransactionId().toString(), transaction)
                producer.send(record) { metadata, exception ->
                    if (exception != null) {
                        publishErrors.incrementAndGet()
                        logger.error("Failed to publish transaction {}", transaction.getTransactionId(), exception)
                    } else {
                        logger.debug("Published transaction {} to partition {} offset {}",
                            transaction.getTransactionId(), metadata.partition(), metadata.offset())
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
        logger.info("Published {} uncategorized transactions", published)

        val scanned = rows.size - 1
        meterRegistry.counter("tiller.producer.rows.scanned").increment(scanned.toDouble())
        meterRegistry.counter("tiller.producer.transactions.published").increment(published.toDouble())
        meterRegistry.counter("tiller.producer.transactions.skipped").increment(skipped.toDouble())
        meterRegistry.counter("tiller.producer.publish.errors").increment(publishErrors.get().toDouble())
    }

    companion object {
        internal fun rowToTransaction(row: List<Any>, colIndex: Map<String, Int>, maxAgeDays: Long = 365, owner: String? = null): Transaction? {
            fun col(name: String): String? = colIndex[name]?.let { row.getOrNull(it)?.toString() }

            val category = col("Category") ?: ""
            if (category.isNotBlank()) return null

            val transactionId = col("Transaction ID") ?: return null
            if (transactionId.isBlank()) return null

            // Skip transactions older than maxAgeDays
            val dateStr = col("Date") ?: ""
            if (dateStr.isNotBlank()) {
                try {
                    val txDate = LocalDate.parse(dateStr, DateTimeFormatter.ofPattern("M/d/yyyy"))
                    if (txDate.isBefore(LocalDate.now().minusDays(maxAgeDays))) return null
                } catch (_: DateTimeParseException) {
                    // If date can't be parsed, include the transaction
                }
            }

            return Transaction.newBuilder()
                .setTransactionId(transactionId)
                .setDate(col("Date") ?: "")
                .setDescription(col("Description") ?: "")
                .setCategory(null)
                .setAmount(col("Amount") ?: "")
                .setAccount(col("Account") ?: "")
                .setAccountNumber(col("Account #"))
                .setInstitution(col("Institution"))
                .setMonth(col("Month"))
                .setWeek(col("Week"))
                .setCheckNumber(col("Check Number"))
                .setFullDescription(col("Full Description"))
                .setNote(col("Note"))
                .setSource(col("Source"))
                .setDateAdded(col("Date Added"))
                .setOwner(owner)
                .build()
        }
    }
}
