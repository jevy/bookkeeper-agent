package org.jevy.bookkeeper.producer

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.StringDeserializer
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
        val dlqIds = loadDlqIds(config)

        pollAndPublish(sheetsClient, producer, dlqIds)
        producer.close()
    }

    private fun pollAndPublish(
        sheetsClient: SheetsClient,
        producer: org.apache.kafka.clients.producer.KafkaProducer<String, Transaction>,
        dlqIds: Set<String> = emptySet(),
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
        var dlqSkipped = 0
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

                if (txId in dlqIds) {
                    logger.info("Skipping transaction {} — already in DLQ", txId)
                    dlqSkipped++
                    continue
                }

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
        logger.info("Scanned {} rows: published={}, skipped={}, dlqSkipped={}, errors={}", scanned, published, skipped, dlqSkipped, publishErrors.get())
        meterRegistry.counter("bookkeeper.producer.rows.scanned").increment(scanned.toDouble())
        meterRegistry.counter("bookkeeper.producer.transactions.published").increment(published.toDouble())
        meterRegistry.counter("bookkeeper.producer.transactions.skipped").increment(skipped.toDouble())
        meterRegistry.counter("bookkeeper.producer.transactions.dlq_skipped").increment(dlqSkipped.toDouble())
        meterRegistry.counter("bookkeeper.producer.publish.errors").increment(publishErrors.get().toDouble())
    }

    companion object {
        private val skipLogger = LoggerFactory.getLogger("TransactionProducer.skip")
        private val dlqLogger = LoggerFactory.getLogger("TransactionProducer.dlq")

        internal fun loadDlqIds(config: AppConfig): Set<String> {
            val props = mapOf(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to config.kafkaBootstrapServers,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java.name,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to ByteArrayDeserializer::class.java.name,
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to "false",
            )
            val ids = mutableSetOf<String>()
            try {
                KafkaConsumer<String, ByteArray>(props).use { consumer ->
                    val topics = listOf(TopicNames.WRITE_FAILED, TopicNames.CATEGORIZATION_FAILED)
                    val partitions = topics.flatMap { topic ->
                        consumer.partitionsFor(topic)?.map { org.apache.kafka.common.TopicPartition(it.topic(), it.partition()) } ?: emptyList()
                    }
                    if (partitions.isEmpty()) return ids
                    consumer.assign(partitions)
                    consumer.seekToBeginning(partitions)

                    val endOffsets = consumer.endOffsets(partitions)
                    while (true) {
                        val records = consumer.poll(Duration.ofSeconds(2))
                        for (record in records) {
                            if (record.key() != null) ids.add(record.key())
                        }
                        // Check if we've reached the end of all partitions
                        val allDone = partitions.all { tp ->
                            consumer.position(tp) >= (endOffsets[tp] ?: 0)
                        }
                        if (allDone) break
                    }
                }
                dlqLogger.info("Loaded {} DLQ transaction IDs", ids.size)
            } catch (e: Exception) {
                dlqLogger.warn("Failed to load DLQ IDs, proceeding without DLQ filter", e)
            }
            return ids
        }

        internal fun rowToTransaction(row: List<Any>, colIndex: Map<String, Int>, maxAgeDays: Long = 365, owner: String? = null): Transaction? {
            fun col(name: String): String? = colIndex[name]?.let { row.getOrNull(it)?.toString() }

            val category = col("Category") ?: ""
            if (category.isNotBlank()) return null

            val description = col("Description") ?: "(no description)"
            val transactionId = col("Transaction ID")?.takeIf { it.isNotBlank() }
                ?: DurableTransactionId.generate(
                    owner = owner ?: "",
                    date = col("Date") ?: "",
                    description = description,
                    amount = col("Amount") ?: "",
                    account = col("Account") ?: "",
                )

            // Skip transactions older than maxAgeDays
            val dateStr = col("Date") ?: ""
            if (dateStr.isNotBlank()) {
                try {
                    val txDate = LocalDate.parse(dateStr, DateTimeFormatter.ofPattern("M/d/yyyy"))
                    if (txDate.isBefore(LocalDate.now().minusDays(maxAgeDays))) {
                        skipLogger.info("Skipped row: too old ({}) — id={}, desc={}", dateStr, transactionId, description)
                        return null
                    }
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
