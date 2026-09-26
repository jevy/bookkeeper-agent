package org.jevy.bookkeeper.replay

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.jevy.bookkeeper.config.AppConfig
import org.jevy.bookkeeper.kafka.KafkaFactory
import org.jevy.bookkeeper.kafka.TopicNames
import org.jevy.bookkeeper_agent.Transaction
import org.slf4j.LoggerFactory
import java.time.Duration

enum class ReplayMode {
    /** categorization-failed and write-failed: clear the category, back to uncategorized, categorizer runs again. */
    RECATEGORIZE,
    /** sure-write-failed: the category is already in the Sheet, back to written unchanged so only the Sure sink retries. */
    SURE,
}

class DlqReplayer(private val config: AppConfig, private val mode: ReplayMode = ReplayMode.RECATEGORIZE) {

    private val logger = LoggerFactory.getLogger(DlqReplayer::class.java)

    fun run() {
        val consumer = KafkaFactory.createConsumer(config, "dlq-replayer")
        val tombstoneProducer = KafkaFactory.createTombstoneProducer(config)
        val avroProducer = KafkaFactory.createProducer(config)

        val dlqTopics = when (mode) {
            ReplayMode.RECATEGORIZE -> listOf(TopicNames.CATEGORIZATION_FAILED, TopicNames.WRITE_FAILED)
            ReplayMode.SURE -> listOf(TopicNames.SURE_WRITE_FAILED)
        }
        val partitions = dlqTopics.flatMap { topic ->
            consumer.partitionsFor(topic)?.map { TopicPartition(it.topic(), it.partition()) } ?: emptyList()
        }

        if (partitions.isEmpty()) {
            logger.info("No DLQ partitions found, nothing to replay")
            consumer.close()
            tombstoneProducer.close()
            avroProducer.close()
            return
        }

        consumer.assign(partitions)
        consumer.seekToBeginning(partitions)
        val endOffsets = consumer.endOffsets(partitions)

        val replayedPerTopic = mutableMapOf<String, Int>()

        while (true) {
            val records = consumer.poll(Duration.ofSeconds(2))
            for (record in records) {
                if (record.key() == null || record.value() == null) continue
                replayRecord(record, tombstoneProducer, avroProducer)
                replayedPerTopic.merge(record.topic(), 1) { a, b -> a + b }
            }
            val allDone = partitions.all { tp ->
                consumer.position(tp) >= (endOffsets[tp] ?: 0)
            }
            if (allDone) break
        }

        tombstoneProducer.flush()
        avroProducer.flush()

        val total = replayedPerTopic.values.sum()
        logger.info("DLQ replay complete: {} total replayed, per-topic: {}", total, replayedPerTopic)

        consumer.close()
        tombstoneProducer.close()
        avroProducer.close()
    }

    private fun replayRecord(
        record: ConsumerRecord<String, Transaction>,
        tombstoneProducer: org.apache.kafka.clients.producer.KafkaProducer<String, ByteArray?>,
        avroProducer: org.apache.kafka.clients.producer.KafkaProducer<String, Transaction>,
    ) {
        val key = record.key()

        // Tombstone the DLQ entry first
        tombstoneProducer.send(ProducerRecord(record.topic(), key, null))

        when (mode) {
            ReplayMode.RECATEGORIZE -> {
                // Null out category and category_justification so the categorizer will process it
                val cleaned = Transaction.newBuilder(record.value())
                    .setCategory(null)
                    .setCategoryJustification(null)
                    .build()
                avroProducer.send(ProducerRecord(TopicNames.UNCATEGORIZED, key, cleaned))
            }
            ReplayMode.SURE -> avroProducer.send(ProducerRecord(TopicNames.WRITTEN, key, record.value()))
        }

        logger.info("Replayed transaction {} from {} ({})", key, record.topic(), mode)
    }
}
