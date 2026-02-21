package org.jevy.tiller.categorizer.producer

import org.apache.kafka.clients.producer.ProducerRecord
import org.jevy.tiller.categorizer.config.AppConfig
import org.jevy.tiller.categorizer.kafka.KafkaFactory
import org.jevy.tiller.categorizer.kafka.TopicNames
import org.jevy.tiller.categorizer.sheets.SheetsClient
import org.jevy.tiller_categorizer_agent.Transaction
import org.slf4j.LoggerFactory

class TransactionProducer(private val config: AppConfig) {

    private val logger = LoggerFactory.getLogger(TransactionProducer::class.java)

    fun run() {
        val sheetsClient = SheetsClient(config)
        val producer = KafkaFactory.createProducer(config)

        while (true) {
            try {
                pollAndPublish(sheetsClient, producer)
            } catch (e: Exception) {
                logger.error("Error during poll cycle", e)
            }

            logger.info("Sleeping {} seconds until next poll", config.pollIntervalSeconds)
            Thread.sleep(config.pollIntervalSeconds * 1000)
        }
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
        var published = 0

        rows.drop(1).forEachIndexed { index, row ->
            val rowNumber = index + 2 // 1-indexed, skip header
            try {
                val transaction = rowToTransaction(row, rowNumber) ?: return@forEachIndexed

                val record = ProducerRecord(TopicNames.UNCATEGORIZED, transaction.getTransactionId().toString(), transaction)
                producer.send(record) { metadata, exception ->
                    if (exception != null) {
                        logger.error("Failed to publish transaction {}", transaction.getTransactionId(), exception)
                    } else {
                        logger.debug("Published transaction {} to partition {} offset {}",
                            transaction.getTransactionId(), metadata.partition(), metadata.offset())
                    }
                }
                published++
            } catch (e: Exception) {
                logger.error("Failed to process row {}", rowNumber, e)
            }
        }

        producer.flush()
        logger.info("Published {} uncategorized transactions", published)
    }

    companion object {
        internal fun rowToTransaction(row: List<Any>, rowNumber: Int): Transaction? {
            val category = row.getOrNull(2)?.toString() ?: ""
            if (category.isNotBlank()) return null

            val transactionId = row.getOrNull(9)?.toString() ?: return null
            if (transactionId.isBlank()) return null

            return Transaction.newBuilder()
                .setTransactionId(transactionId)
                .setDate(row.getOrNull(0)?.toString() ?: "")
                .setDescription(row.getOrNull(1)?.toString() ?: "")
                .setCategory(null)
                .setAmount(row.getOrNull(3)?.toString() ?: "")
                .setAccount(row.getOrNull(4)?.toString() ?: "")
                .setAccountNumber(row.getOrNull(5)?.toString())
                .setInstitution(row.getOrNull(6)?.toString())
                .setMonth(row.getOrNull(7)?.toString())
                .setWeek(row.getOrNull(8)?.toString())
                .setCheckNumber(row.getOrNull(10)?.toString())
                .setFullDescription(row.getOrNull(11)?.toString())
                .setNote(row.getOrNull(12)?.toString())
                .setSource(row.getOrNull(14)?.toString())
                .setDateAdded(row.getOrNull(16)?.toString())
                .setSheetRowNumber(rowNumber)
                .build()
        }
    }
}
