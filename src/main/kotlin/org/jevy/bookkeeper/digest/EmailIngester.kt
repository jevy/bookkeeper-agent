package org.jevy.bookkeeper.digest

import jakarta.mail.Session
import jakarta.mail.internet.MimeMessage
import jakarta.mail.internet.MimeMultipart
import jakarta.mail.Part
import org.apache.kafka.clients.producer.ProducerRecord
import org.jevy.bookkeeper.config.AppConfig
import org.jevy.bookkeeper.kafka.KafkaFactory
import org.jevy.bookkeeper.kafka.TopicNames
import org.jevy.bookkeeper_agent.EmailMessage
import org.slf4j.LoggerFactory
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.*
import java.io.ByteArrayInputStream
import java.time.Instant
import java.util.Properties

class EmailIngester(private val config: AppConfig) {

    private val logger = LoggerFactory.getLogger(EmailIngester::class.java)

    fun run() {
        val s3 = S3Client.builder().region(Region.of(config.awsRegion)).build()
        val producer = KafkaFactory.createAvroProducer<EmailMessage>(config)

        s3.use { s3Client ->
            producer.use { kafkaProducer ->
                val objects = listInboxObjects(s3Client)

                if (objects.isEmpty()) {
                    logger.info("No new emails in inbox/")
                    return
                }

                logger.info("Found {} emails in inbox/", objects.size)

                for (obj in objects) {
                    try {
                        processEmail(s3Client, kafkaProducer, obj)
                    } catch (e: Exception) {
                        logger.error("Failed to process email {}, skipping", obj.key(), e)
                    }
                }

                kafkaProducer.flush()
            }
        }
    }

    private fun listInboxObjects(s3: S3Client): List<S3Object> {
        val response = s3.listObjectsV2(
            ListObjectsV2Request.builder()
                .bucket(config.s3Bucket)
                .prefix("inbox/")
                .build()
        )
        return response.contents().filter { it.key() != "inbox/" }
    }

    private fun processEmail(
        s3: S3Client,
        producer: org.apache.kafka.clients.producer.KafkaProducer<String, EmailMessage>,
        obj: S3Object,
    ) {
        val key = obj.key()
        logger.info("Processing email: {}", key)

        // Download raw email
        val rawBytes = s3.getObject(
            GetObjectRequest.builder()
                .bucket(config.s3Bucket)
                .key(key)
                .build()
        ).readAllBytes()

        // Parse MIME
        val session = Session.getDefaultInstance(Properties())
        val mimeMessage = MimeMessage(session, ByteArrayInputStream(rawBytes))

        val messageId = mimeMessage.messageID ?: "unknown-${Instant.now().toEpochMilli()}"
        val subject = mimeMessage.subject ?: ""
        val from = mimeMessage.from?.firstOrNull()?.toString() ?: ""
        val bodyText = extractTextBody(mimeMessage)

        // Publish to Kafka
        val emailMessage = EmailMessage.newBuilder()
            .setMessageId(messageId)
            .setSubject(subject)
            .setFromAddress(from)
            .setBodyText(bodyText)
            .setS3Key(key)
            .setReceivedAt(Instant.now().toString())
            .build()

        producer.send(ProducerRecord(TopicNames.EMAIL_INBOX, messageId, emailMessage))
        logger.info("Published email {} to {}", messageId, TopicNames.EMAIL_INBOX)

        // Move from inbox/ to ingested/
        val ingestedKey = key.replaceFirst("inbox/", "ingested/")
        s3.copyObject(
            CopyObjectRequest.builder()
                .sourceBucket(config.s3Bucket)
                .sourceKey(key)
                .destinationBucket(config.s3Bucket)
                .destinationKey(ingestedKey)
                .build()
        )
        s3.deleteObject(
            DeleteObjectRequest.builder()
                .bucket(config.s3Bucket)
                .key(key)
                .build()
        )
        logger.info("Moved {} → {}", key, ingestedKey)
    }

    internal fun extractTextBody(message: MimeMessage): String {
        return extractText(message) ?: ""
    }

    private fun extractText(part: Part): String? {
        if (part.isMimeType("text/plain")) {
            return part.content as? String
        }
        if (part.isMimeType("multipart/*")) {
            val multipart = part.content as MimeMultipart
            // Prefer text/plain over text/html
            for (i in 0 until multipart.count) {
                val bodyPart = multipart.getBodyPart(i)
                if (bodyPart.isMimeType("text/plain")) {
                    return bodyPart.content as? String
                }
            }
            // Recurse into nested multiparts
            for (i in 0 until multipart.count) {
                val result = extractText(multipart.getBodyPart(i))
                if (result != null) return result
            }
        }
        return null
    }
}
