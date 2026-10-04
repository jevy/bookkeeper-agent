package org.jevy.bookkeeper.sure

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.jevy.bookkeeper.config.AppConfig
import org.jevy.bookkeeper.kafka.KafkaFactory
import org.jevy.bookkeeper.kafka.TopicNames
import org.jevy.bookkeeper.sheets.SheetsClient
import org.jevy.bookkeeper.sheets.TransactionMapper
import org.jevy.bookkeeper_agent.Transaction
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeParseException

/**
 * Mirrors into Sure the categories that never pass through the categorizer: rows Tiller's
 * AutoCat filled in, and rows categorized or re-categorized by hand in the Sheet. Before this
 * existed only the LLM's categorizations reached transactions.written, so most of each month
 * stayed uncategorized in Sure.
 *
 * Reads the whole Sheet, compares each categorized row against the latest category already
 * published to transactions.written for that transaction, and publishes only rows that are
 * new or whose category changed. The sure-writer does the matching and the PATCH as for any
 * other record, so this costs Sure nothing for rows already in sync.
 */
class SureSync(
    private val config: AppConfig,
    private val meterRegistry: MeterRegistry = SimpleMeterRegistry(),
) {
    private val logger = LoggerFactory.getLogger(SureSync::class.java)

    fun run() {
        val rows = SheetsClient(config).readAllRows()
        if (rows.size <= 1) {
            logger.info("No rows found in sheet")
            return
        }
        val lastPublished = loadLatestCategories(config)
        val cutoff = LocalDate.now().minusDays(config.sureSyncMaxAgeDays)
        val selection = select(rows, config.googleSheetId, lastPublished, config.sureAccountMap.keys, cutoff)

        KafkaFactory.createProducer(config).use { producer ->
            for (tx in selection.toSync) {
                producer.send(ProducerRecord(TopicNames.WRITTEN, tx.getTransactionId().toString(), tx))
            }
            producer.flush()
        }

        logger.info("Sure sync: published={}, inSync={}, uncategorized={}, tooOld={}, unmappedAccount={} (cutoff {}, {} keys already on {})",
            selection.toSync.size, selection.inSync, selection.uncategorized, selection.tooOld, selection.unmappedAccount,
            cutoff, lastPublished.size, TopicNames.WRITTEN)
        meterRegistry.counter("bookkeeper.sure_sync.published").increment(selection.toSync.size.toDouble())
        meterRegistry.counter("bookkeeper.sure_sync.in_sync").increment(selection.inSync.toDouble())
        meterRegistry.counter("bookkeeper.sure_sync.unmapped_account").increment(selection.unmappedAccount.toDouble())
    }

    data class Selection(
        val toSync: List<Transaction>,
        val inSync: Int,
        val uncategorized: Int,
        val tooOld: Int,
        val unmappedAccount: Int,
    )

    companion object {
        private val sheetDate = DateTimeFormatter.ofPattern("M/d/yyyy")

        /**
         * Picks the Sheet rows Sure has not been told about: categorized, dated on or after
         * [cutoff], on an account Sure is mapped for, and either never published or published
         * with a different category. Unmapped accounts are skipped rather than sent, since the
         * sure-writer would only dead-letter them as account_unmapped.
         */
        fun select(
            rows: List<List<Any>>,
            owner: String,
            lastPublished: Map<String, String>,
            mappedAccounts: Set<String>,
            cutoff: LocalDate,
        ): Selection {
            val colIndex = rows.first().map { it.toString() }.withIndex().associate { (i, name) -> name to i }
            val toSync = mutableListOf<Transaction>()
            var inSync = 0
            var uncategorized = 0
            var tooOld = 0
            var unmapped = 0

            for (row in rows.drop(1)) {
                val tx = TransactionMapper.fromSheetRow(row, colIndex, owner)
                val category = tx.getCategory()?.toString()?.trim()
                if (category.isNullOrEmpty()) { uncategorized++; continue }

                val date = try {
                    LocalDate.parse(tx.getDate().toString().trim(), sheetDate)
                } catch (_: DateTimeParseException) { null }
                if (date == null || date.isBefore(cutoff)) { tooOld++; continue }

                if (tx.getAccount().toString().trim() !in mappedAccounts) { unmapped++; continue }

                if (lastPublished[tx.getTransactionId().toString()]?.trim() == category) { inSync++; continue }
                toSync += tx
            }
            return Selection(toSync, inSync, uncategorized, tooOld, unmapped)
        }

        /** Reduces a topic's records, in offset order, to the latest category per key. A tombstone forgets the key. */
        fun latestCategories(records: Iterable<Pair<String, Transaction?>>): Map<String, String> {
            val latest = HashMap<String, String>()
            for ((key, value) in records) latest.record(key, value)
            return latest
        }

        private fun MutableMap<String, String>.record(key: String, value: Transaction?) {
            val category = value?.getCategory()?.toString()
            if (category == null) remove(key) else this[key] = category
        }

        internal fun loadLatestCategories(config: AppConfig): Map<String, String> {
            val consumer = KafkaFactory.createAvroConsumer<Transaction>(config, "sure-sync", maxPollRecords = 500)
            return consumer.use {
                val partitions = consumer.partitionsFor(TopicNames.WRITTEN)?.map { TopicPartition(it.topic(), it.partition()) } ?: emptyList()
                if (partitions.isEmpty()) return emptyMap()
                consumer.assign(partitions)
                consumer.seekToBeginning(partitions)
                val endOffsets = consumer.endOffsets(partitions)
                // Reduce as we read. The topic holds every copy ever published (148k records for
                // ~5k keys in production), and buffering them all exhausted a 512Mi pod's heap.
                val latest = HashMap<String, String>()
                while (partitions.any { consumer.position(it) < (endOffsets[it] ?: 0) }) {
                    for (record in consumer.poll(Duration.ofSeconds(2))) {
                        record.key()?.let { latest.record(it, record.value()) }
                    }
                }
                latest
            }
        }
    }
}
