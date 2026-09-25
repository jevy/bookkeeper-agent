package org.jevy.bookkeeper.writer

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.jevy.bookkeeper.config.AppConfig
import org.jevy.bookkeeper.kafka.KafkaFactory
import org.jevy.bookkeeper.kafka.TopicNames
import org.jevy.bookkeeper_agent.Transaction
import org.slf4j.LoggerFactory
import java.time.Duration

/**
 * Shared consumer loop for every category sink. Consumes [CategorySink.sourceTopic]
 * with the sink's own consumer group and dispatches on [SinkResult].
 *
 * [tombstoneUncategorized] is true only for the Sheets sink: once the Sheet has
 * the category (or the record is in the DLQ), the transaction is done from the
 * pipeline's point of view and is removed from transactions.uncategorized.
 *
 * [publishOnDone], when set, republishes the record unchanged to that topic on
 * Written and on Skipped(landed = true). The Sheets writer uses it to feed
 * transactions.written, which is what the Sure writer consumes, so Sure only
 * ever mirrors what actually landed in the Sheet.
 */
class SinkWriter(
    private val config: AppConfig,
    private val sink: CategorySink,
    private val meterRegistry: MeterRegistry = SimpleMeterRegistry(),
    private val tombstoneUncategorized: Boolean = false,
    private val publishOnDone: String? = null,
) {
    private val logger = LoggerFactory.getLogger("${SinkWriter::class.java.name}.${sink.name}")

    private val writtenCounter = meterRegistry.counter("bookkeeper.${sink.name}.transactions.written")
    private val errorsCounter = meterRegistry.counter("bookkeeper.${sink.name}.errors")
    private val durationTimer = meterRegistry.timer("bookkeeper.${sink.name}.duration")

    private fun skipped(reason: String) =
        meterRegistry.counter("bookkeeper.${sink.name}.transactions.skipped", "reason", reason).increment()

    private fun rejected(reason: String) =
        meterRegistry.counter("bookkeeper.${sink.name}.transactions.rejected", "reason", reason).increment()

    fun run(onActivity: () -> Unit = {}, onAlive: (Boolean) -> Unit = {}) {
        val consumer = KafkaFactory.createConsumer(config, sink.consumerGroup)
        val dlqProducer = KafkaFactory.createProducer(config)
        val tombstoneProducer: KafkaProducer<String, ByteArray?>? =
            if (tombstoneUncategorized) KafkaFactory.createTombstoneProducer(config) else null

        consumer.subscribe(listOf(sink.sourceTopic))
        logger.info("Sink '{}' subscribed to {} as group {}", sink.name, sink.sourceTopic, sink.consumerGroup)
        onAlive(true)

        fun publishDone(transactionId: String, value: Transaction) {
            publishOnDone?.let { topic ->
                dlqProducer.send(ProducerRecord(topic, transactionId, value))
                logger.debug("Published transaction {} to {}", transactionId, topic)
            }
        }

        try {
            while (true) {
                val records = consumer.poll(Duration.ofSeconds(5))
                onActivity()
                for (record in records) {
                    val transactionId = record.key()
                    val result = durationTimer.recordCallable { sink.write(record.value()) }!!
                    when (result) {
                        is SinkResult.Written -> {
                            writtenCounter.increment()
                            tombstoneProducer?.tombstone(transactionId)
                            publishDone(transactionId, record.value())
                        }
                        is SinkResult.Skipped -> {
                            skipped(result.reason)
                            logger.info("Skipped transaction {} ({})", transactionId, result.reason)
                            if (result.landed) publishDone(transactionId, record.value())
                        }
                        is SinkResult.Rejected -> {
                            errorsCounter.increment()
                            rejected(result.reason)
                            logger.error("Rejected transaction {} ({}), sending to {}", transactionId, result.reason, sink.dlqTopic, result.cause)
                            dlqProducer.send(ProducerRecord(sink.dlqTopic, transactionId, record.value()))
                            tombstoneProducer?.tombstone(transactionId)
                        }
                        is SinkResult.Unavailable -> {
                            logger.error("Sink '{}' unavailable while handling {}; exiting without commit", sink.name, transactionId, result.cause)
                            throw SinkUnavailableException(result.cause)
                        }
                    }
                }
                consumer.commitSync()
            }
        } finally {
            onAlive(false)
            logger.error("Consumer loop exited, marking unhealthy")
        }
    }

    private fun KafkaProducer<String, ByteArray?>.tombstone(transactionId: String) {
        send(ProducerRecord(TopicNames.UNCATEGORIZED, transactionId, null))
        logger.debug("Tombstoned transaction {} from uncategorized", transactionId)
    }
}
