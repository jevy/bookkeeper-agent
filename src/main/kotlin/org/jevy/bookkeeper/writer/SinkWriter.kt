package org.jevy.bookkeeper.writer

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
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
 * pipeline's point of view and is removed from transactions.uncategorized. As in
 * the pre-split CategoryWriter, every non-fatal outcome tombstones.
 *
 * [publishOnDone], when set, republishes the record unchanged to that topic on
 * Written and on Skipped(landed = true). The Sheets writer uses it to feed
 * transactions.written, which is what the Sure writer consumes, so Sure only
 * ever mirrors what actually landed in the Sheet.
 *
 * Metrics: the untagged counters are registered eagerly so every series exists
 * at zero from startup (an absent series breaks PromQL arithmetic). The
 * per-reason breakdowns live under separate names because the Prometheus
 * registry refuses one name with two different tag-key sets.
 */
class SinkWriter(
    private val config: AppConfig,
    private val sink: CategorySink,
    private val meterRegistry: MeterRegistry = SimpleMeterRegistry(),
    private val tombstoneUncategorized: Boolean = false,
    private val publishOnDone: String? = null,
    private val clock: () -> Long = System::currentTimeMillis,
) {
    private val logger = LoggerFactory.getLogger("${SinkWriter::class.java.name}.${sink.name}")

    private val writtenCounter = meterRegistry.counter("bookkeeper.${sink.name}.transactions.written")
    private val skippedCounter = meterRegistry.counter("bookkeeper.${sink.name}.transactions.skipped")
    private val rejectedCounter = meterRegistry.counter("bookkeeper.${sink.name}.transactions.rejected")
    private val errorsCounter = meterRegistry.counter("bookkeeper.${sink.name}.errors")
    private val durationTimer = meterRegistry.timer("bookkeeper.${sink.name}.duration")
    private val throttledCounter = meterRegistry.counter("bookkeeper.${sink.name}.throttled")

    private fun skipped(reason: String) {
        skippedCounter.increment()
        meterRegistry.counter("bookkeeper.${sink.name}.transactions.skipped.reasons", "reason", reason).increment()
    }

    private fun rejected(reason: String) {
        rejectedCounter.increment()
        meterRegistry.counter("bookkeeper.${sink.name}.transactions.rejected.reasons", "reason", reason).increment()
    }

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
                val records = consumer.poll(Duration.ofSeconds(5)).toList()
                onActivity()
                var throttledFor: Duration? = null
                for ((index, record) in records.withIndex()) {
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
                            tombstoneProducer?.tombstone(transactionId)
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
                            if (result.retryAfter == null) {
                                logger.error("Sink '{}' unavailable while handling {}; exiting without commit", sink.name, transactionId, result.cause)
                                throw SinkUnavailableException(result.cause)
                            }
                            throttledCounter.increment()
                            logger.warn("Sink '{}' throttled while handling {}; pausing {}s and retrying it", sink.name, transactionId, result.retryAfter.seconds)
                            rewind(consumer, records.subList(index, records.size))
                            throttledFor = result.retryAfter
                        }
                    }
                    if (throttledFor != null) break
                }
                // Make every publish, DLQ send and tombstone durable before the offset moves,
                // so a committed record can never be missing from transactions.written.
                dlqProducer.flush()
                tombstoneProducer?.flush()
                consumer.commitSync()
                throttledFor?.let { waitOut(consumer, it, onActivity) }
            }
        } finally {
            onAlive(false)
            logger.error("Consumer loop exited, marking unhealthy")
        }
    }

    /** Moves each partition back to its first record in [unprocessed], so the next poll redelivers them. */
    private fun rewind(consumer: KafkaConsumer<String, Transaction>, unprocessed: List<ConsumerRecord<String, Transaction>>) {
        unprocessed.groupBy { TopicPartition(it.topic(), it.partition()) }
            .forEach { (tp, recs) -> consumer.seek(tp, recs.minOf { it.offset() }) }
    }

    /**
     * Sits out a throttle with the assignment paused. Polling while paused keeps the consumer in
     * its group past max.poll.interval.ms, and [onActivity] keeps /healthz green, so the pod is
     * neither rebalanced away nor restarted by its liveness probe.
     */
    private fun waitOut(consumer: KafkaConsumer<String, Transaction>, retryAfter: Duration, onActivity: () -> Unit) {
        val deadline = clock() + retryAfter.toMillis()
        consumer.pause(consumer.assignment())
        while (clock() < deadline) {
            val stray = consumer.poll(Duration.ofMillis((deadline - clock()).coerceIn(1, 5_000)))
            // A rebalance during the wait hands partitions back unpaused; their records are not ours yet.
            if (!stray.isEmpty) {
                rewind(consumer, stray.toList())
                consumer.pause(consumer.assignment())
            }
            onActivity()
        }
        consumer.resume(consumer.paused())
    }

    private fun KafkaProducer<String, ByteArray?>.tombstone(transactionId: String) {
        send(ProducerRecord(TopicNames.UNCATEGORIZED, transactionId, null))
        logger.debug("Tombstoned transaction {} from uncategorized", transactionId)
    }
}
