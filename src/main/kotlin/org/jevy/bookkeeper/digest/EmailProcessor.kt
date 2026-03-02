package org.jevy.bookkeeper.digest

import org.apache.kafka.clients.producer.ProducerRecord
import org.jevy.bookkeeper.config.AppConfig
import org.jevy.bookkeeper.kafka.KafkaFactory
import org.jevy.bookkeeper.kafka.TopicNames
import org.jevy.bookkeeper.sheets.SheetsClient
import org.jevy.bookkeeper.sheets.SheetTransaction
import org.jevy.bookkeeper.sheets.TransactionMapper
import org.jevy.bookkeeper_agent.EmailMessage
import org.jevy.bookkeeper_agent.Transaction
import org.slf4j.LoggerFactory
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.CopyObjectRequest
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest
import java.time.Duration
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.Locale

class EmailProcessor(
    private val config: AppConfig,
    private val sheetsClient: SheetsClient = SheetsClient(config),
) {

    private val logger = LoggerFactory.getLogger(EmailProcessor::class.java)
    private val correctionRegex = Regex("""^\s*(\d+)\s*[:.]\s*(.+)$""")
    private val subjectDateRegex = Regex("""Bookkeeper Digest - (.+?) \(""")

    // Date formats we accept in the subject line
    private val subjectDateFormatter = DateTimeFormatter.ofPattern("MMM d, yyyy", Locale.ENGLISH)

    fun run(onActivity: () -> Unit = {}, onAlive: (Boolean) -> Unit = {}) {
        val consumer = KafkaFactory.createAvroConsumer<EmailMessage>(config, "email-processor")
        val transactionProducer = KafkaFactory.createProducer(config)
        val processedProducer = KafkaFactory.createAvroProducer<EmailMessage>(config)
        val s3 = S3Client.builder().region(Region.of(config.awsRegion)).build()

        consumer.subscribe(listOf(TopicNames.EMAIL_INBOX))
        logger.info("Subscribed to {}", TopicNames.EMAIL_INBOX)

        onAlive(true)

        try {
            while (true) {
                val records = consumer.poll(Duration.ofSeconds(5))
                onActivity()
                for (record in records) {
                    val email = record.value() ?: continue
                    try {
                        processEmail(email, transactionProducer, processedProducer, s3)
                    } catch (e: Exception) {
                        logger.error("Failed to process email {}", email.getMessageId(), e)
                    }
                }
                consumer.commitSync()
            }
        } finally {
            onAlive(false)
            s3.close()
            logger.error("Consumer loop exited — marking unhealthy")
        }
    }

    internal fun processEmail(
        email: EmailMessage,
        transactionProducer: org.apache.kafka.clients.producer.KafkaProducer<String, Transaction>,
        processedProducer: org.apache.kafka.clients.producer.KafkaProducer<String, EmailMessage>,
        s3: S3Client,
    ) {
        val subject = email.getSubject().toString()
        val body = email.getBodyText().toString()
        val messageId = email.getMessageId().toString()

        logger.info("Processing email: subject='{}', from='{}'", subject, email.getFromAddress())

        // Extract digest date from subject
        val digestDate = extractDigestDate(subject)
        if (digestDate == null) {
            logger.warn("Could not extract digest date from subject '{}', skipping", subject)
            publishProcessed(processedProducer, email)
            moveS3Object(s3, email.getS3Key().toString())
            return
        }

        // Load number → transaction_id mapping from S3
        val mapping = try {
            DigestSender.loadMapping(s3, config.s3Bucket, digestDate)
        } catch (e: Exception) {
            logger.error("Could not load mapping for date {}: {}", digestDate, e.message)
            publishProcessed(processedProducer, email)
            moveS3Object(s3, email.getS3Key().toString())
            return
        }

        // Parse corrections from email body
        val corrections = parseCorrections(body)
        if (corrections.isEmpty()) {
            logger.info("No corrections found in email {}", messageId)
            publishProcessed(processedProducer, email)
            moveS3Object(s3, email.getS3Key().toString())
            return
        }

        logger.info("Found {} corrections in email {}", corrections.size, messageId)

        // Look up transactions and republish with user context
        val allRows = sheetsClient.readAllRows()
        val header = allRows.firstOrNull()?.map { it.toString() } ?: return
        val colIndex = header.withIndex().associate { (i, name) -> name to i }
        val indexed = allRows.drop(1).mapIndexed { i, row ->
            SheetTransaction(i + 2, TransactionMapper.fromSheetRow(row, colIndex, config.googleSheetId))
        }

        for ((number, userContext) in corrections) {
            val transactionId = mapping[number.toString()]
            if (transactionId == null) {
                logger.warn("No mapping for number {} in digest {}", number, digestDate)
                continue
            }

            val sheetTxn = indexed.find {
                it.transaction.getTransactionId().toString() == transactionId
            }
            if (sheetTxn == null) {
                logger.warn("Transaction {} not found in sheet", transactionId)
                continue
            }

            // Rebuild transaction with category cleared and user context as note
            val corrected = Transaction.newBuilder(sheetTxn.transaction)
                .setCategory(null)
                .setCategoryJustification(null)
                .setNote(userContext)
                .build()

            transactionProducer.send(
                ProducerRecord(TopicNames.UNCATEGORIZED, transactionId, corrected)
            )
            logger.info("Republished transaction {} with user context: '{}'", transactionId, userContext)
        }

        transactionProducer.flush()

        // Publish processed acknowledgment
        publishProcessed(processedProducer, email)

        // Move S3 object from ingested/ to processed/
        moveS3Object(s3, email.getS3Key().toString())
    }

    internal fun extractDigestDate(subject: String): String? {
        val match = subjectDateRegex.find(subject) ?: return null
        val dateStr = match.groupValues[1].trim()
        return try {
            val date = LocalDate.parse(dateStr, subjectDateFormatter)
            date.toString() // ISO format: 2026-03-01
        } catch (e: Exception) {
            logger.warn("Could not parse date '{}' from subject", dateStr, e)
            null
        }
    }

    internal fun parseCorrections(body: String): List<Pair<Int, String>> {
        val strippedBody = stripQuotedText(body)
        return strippedBody.lines()
            .mapNotNull { line ->
                correctionRegex.matchEntire(line)?.let { match ->
                    val number = match.groupValues[1].toIntOrNull() ?: return@let null
                    val context = match.groupValues[2].trim()
                    if (context.isNotBlank()) number to context else null
                }
            }
    }

    internal fun stripQuotedText(body: String): String {
        val lines = body.lines()
        val result = mutableListOf<String>()
        for (line in lines) {
            // Stop at "On ... wrote:" reply headers
            if (line.matches(Regex("^On .+ wrote:$"))) break
            // Skip quoted lines
            if (line.trimStart().startsWith(">")) continue
            result.add(line)
        }
        return result.joinToString("\n")
    }

    private fun publishProcessed(
        producer: org.apache.kafka.clients.producer.KafkaProducer<String, EmailMessage>,
        email: EmailMessage,
    ) {
        producer.send(
            ProducerRecord(TopicNames.EMAIL_PROCESSED, email.getMessageId().toString(), email)
        )
    }

    private fun moveS3Object(s3: S3Client, s3Key: String) {
        if (!s3Key.contains("ingested/")) return

        val processedKey = s3Key.replaceFirst("ingested/", "processed/")
        try {
            s3.copyObject(
                CopyObjectRequest.builder()
                    .sourceBucket(config.s3Bucket)
                    .sourceKey(s3Key)
                    .destinationBucket(config.s3Bucket)
                    .destinationKey(processedKey)
                    .build()
            )
            s3.deleteObject(
                DeleteObjectRequest.builder()
                    .bucket(config.s3Bucket)
                    .key(s3Key)
                    .build()
            )
            logger.info("Moved {} → {}", s3Key, processedKey)
        } catch (e: Exception) {
            logger.warn("Failed to move S3 object {}: {}", s3Key, e.message)
        }
    }
}
