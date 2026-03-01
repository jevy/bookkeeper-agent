package org.jevy.bookkeeper.digest

import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.jevy.bookkeeper.config.AppConfig
import org.jevy.bookkeeper.kafka.TopicNames
import org.jevy.bookkeeper_agent.Transaction
import org.slf4j.LoggerFactory
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.ses.SesClient
import software.amazon.awssdk.services.ses.model.*
import java.time.Duration
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.util.UUID

class DigestSender(private val config: AppConfig) {

    private val logger = LoggerFactory.getLogger(DigestSender::class.java)
    private val gson = Gson()
    private val dateFormatter = DateTimeFormatter.ofPattern("MMM d, yyyy")

    fun run() {
        val transactions = readLast24hTransactions()

        if (transactions.isEmpty()) {
            logger.info("No transactions in the last 24h, skipping digest email")
            return
        }

        val today = LocalDate.now()
        val dateStr = today.format(dateFormatter)

        // Build number → transaction_id mapping
        val mapping = transactions.mapIndexed { index, txn ->
            (index + 1).toString() to txn.getTransactionId().toString()
        }.toMap()

        // Store mapping in S3
        storeMappingInS3(today, mapping)

        // Format and send email
        val subject = "Bookkeeper Digest - $dateStr (${transactions.size} transactions)"
        val body = formatDigestBody(transactions)
        sendEmail(subject, body)

        logger.info("Sent digest email with {} transactions for {}", transactions.size, dateStr)
    }

    internal fun readLast24hTransactions(): List<Transaction> {
        val consumer = createManualConsumer()
        try {
            val topic = TopicNames.CATEGORIZED
            val partitionInfos = consumer.partitionsFor(topic) ?: run {
                logger.warn("No partitions found for topic {}", topic)
                return emptyList()
            }

            val partitions = partitionInfos.map { TopicPartition(it.topic(), it.partition()) }
            consumer.assign(partitions)

            // Seek to 24h ago using offsetsForTimes
            val twentyFourHoursAgo = Instant.now().minus(Duration.ofHours(24)).toEpochMilli()
            val timestampMap = partitions.associateWith { twentyFourHoursAgo }
            val offsets = consumer.offsetsForTimes(timestampMap)

            for (tp in partitions) {
                val offsetAndTimestamp = offsets[tp]
                if (offsetAndTimestamp != null) {
                    consumer.seek(tp, offsetAndTimestamp.offset())
                } else {
                    // No messages after the timestamp on this partition — seek to end
                    consumer.seekToEnd(listOf(tp))
                }
            }

            val endOffsets = consumer.endOffsets(partitions)
            val transactions = mutableListOf<Transaction>()

            while (true) {
                val records = consumer.poll(Duration.ofSeconds(2))
                for (record in records) {
                    val txn = record.value() ?: continue
                    transactions.add(txn)
                }
                val allDone = partitions.all { tp ->
                    consumer.position(tp) >= (endOffsets[tp] ?: 0)
                }
                if (allDone) break
            }

            logger.info("Read {} transactions from last 24h", transactions.size)
            return transactions
        } finally {
            consumer.close()
        }
    }

    private fun createManualConsumer(): KafkaConsumer<String, Transaction> {
        val props = mapOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to config.kafkaBootstrapServers,
            ConsumerConfig.GROUP_ID_CONFIG to "digest-sender-${UUID.randomUUID()}",
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java.name,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to KafkaAvroDeserializer::class.java.name,
            KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG to config.schemaRegistryUrl,
            KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG to "true",
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to "false",
            ConsumerConfig.MAX_POLL_RECORDS_CONFIG to "500",
        )
        return KafkaConsumer(props)
    }

    internal fun formatDigestBody(transactions: List<Transaction>): String {
        val lines = transactions.mapIndexed { index, txn ->
            val num = (index + 1).toString().padStart(2)
            val desc = (txn.getDescription()?.toString() ?: "Unknown").take(25).padEnd(25)
            val amount = (txn.getAmount()?.toString() ?: "").padStart(10)
            val category = txn.getCategory()?.toString() ?: "Unknown"
            "$num. $desc $amount  → $category"
        }

        return buildString {
            appendLine("Here are yesterday's categorizations:")
            appendLine()
            lines.forEach { appendLine(it) }
            appendLine()
            appendLine("To correct, reply with the number and your context:")
            appendLine("  3: That's the gas station near my office, should be Gas")
            appendLine("  5: Cleaning supplies from Amazon, probably Household")
        }
    }

    private fun storeMappingInS3(date: LocalDate, mapping: Map<String, String>) {
        val key = "mappings/${date}.json"
        val json = gson.toJson(mapping)

        val s3 = S3Client.builder().region(Region.of(config.awsRegion)).build()
        s3.use { client ->
            client.putObject(
                PutObjectRequest.builder()
                    .bucket(config.s3Bucket)
                    .key(key)
                    .contentType("application/json")
                    .build(),
                RequestBody.fromString(json),
            )
        }
        logger.info("Stored mapping for {} ({} entries) at s3://{}/{}", date, mapping.size, config.s3Bucket, key)
    }

    private fun sendEmail(subject: String, body: String) {
        val ses = SesClient.builder().region(Region.of(config.awsRegion)).build()
        ses.use { client ->
            val messageId = "<digest-${LocalDate.now()}@${config.sesFromAddress.substringAfter("@")}>"

            client.sendEmail(
                SendEmailRequest.builder()
                    .source(config.sesFromAddress)
                    .destination(Destination.builder().toAddresses(config.digestToAddress).build())
                    .message(
                        Message.builder()
                            .subject(Content.builder().data(subject).charset("UTF-8").build())
                            .body(
                                Body.builder()
                                    .text(Content.builder().data(body).charset("UTF-8").build())
                                    .html(Content.builder().data(bodyToHtml(body)).charset("UTF-8").build())
                                    .build()
                            )
                            .build()
                    )
                    .build()
            )
        }
        logger.info("Sent digest email to {}", config.digestToAddress)
    }

    private fun bodyToHtml(text: String): String {
        val escaped = text
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
        return "<html><body><pre style=\"font-family: monospace;\">$escaped</pre></body></html>"
    }

    companion object {
        fun loadMapping(s3: S3Client, bucket: String, date: String): Map<String, String> {
            val key = "mappings/$date.json"
            val response = s3.getObject(
                software.amazon.awssdk.services.s3.model.GetObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .build()
            )
            val json = response.readAllBytes().toString(Charsets.UTF_8)
            val type = object : TypeToken<Map<String, String>>() {}.type
            return Gson().fromJson(json, type)
        }
    }
}
