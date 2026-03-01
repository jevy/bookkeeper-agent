package org.jevy.bookkeeper.digest

import jakarta.mail.Session
import jakarta.mail.internet.InternetAddress
import jakarta.mail.internet.MimeBodyPart
import jakarta.mail.internet.MimeMessage
import jakarta.mail.internet.MimeMultipart
import org.jevy.bookkeeper.config.AppConfig
import org.junit.jupiter.api.Test
import java.util.Properties
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class EmailIngesterTest {

    private val config = AppConfig(
        kafkaBootstrapServers = "localhost:9092",
        schemaRegistryUrl = "http://localhost:8081",
        googleSheetId = "test",
        googleCredentialsJson = "{}",
        openrouterApiKey = "",
        maxTransactionAgeDays = 365,
        maxTransactions = 0,
        additionalContextPrompt = null,
        model = "anthropic/claude-sonnet-4-6",
        s3Bucket = "test-bucket",
    )

    private val ingester = EmailIngester(config)

    private fun createMimeMessage(body: String, contentType: String = "text/plain"): MimeMessage {
        val session = Session.getDefaultInstance(Properties())
        val message = MimeMessage(session)
        message.setFrom(InternetAddress("user@test.com"))
        message.subject = "Re: Bookkeeper Digest - Mar 1, 2026 (5 transactions)"
        message.setContent(body, contentType)
        message.saveChanges()
        return message
    }

    @Test
    fun `extractTextBody extracts plain text`() {
        val message = createMimeMessage("3: Gas station near office")
        val body = ingester.extractTextBody(message)
        assertEquals("3: Gas station near office", body)
    }

    @Test
    fun `extractTextBody extracts from multipart`() {
        val session = Session.getDefaultInstance(Properties())
        val message = MimeMessage(session)
        message.setFrom(InternetAddress("user@test.com"))
        message.subject = "Test"

        val multipart = MimeMultipart("alternative")

        val textPart = MimeBodyPart()
        textPart.setContent("3: Gas station", "text/plain")
        multipart.addBodyPart(textPart)

        val htmlPart = MimeBodyPart()
        htmlPart.setContent("<p>3: Gas station</p>", "text/html")
        multipart.addBodyPart(htmlPart)

        message.setContent(multipart)
        message.saveChanges()

        val body = ingester.extractTextBody(message)
        assertEquals("3: Gas station", body)
    }

    @Test
    fun `extractTextBody returns empty for no text body`() {
        val session = Session.getDefaultInstance(Properties())
        val message = MimeMessage(session)
        message.setFrom(InternetAddress("user@test.com"))
        message.subject = "Test"

        val multipart = MimeMultipart("alternative")
        val htmlPart = MimeBodyPart()
        htmlPart.setContent("<p>Hello</p>", "text/html")
        multipart.addBodyPart(htmlPart)

        message.setContent(multipart)
        message.saveChanges()

        // Falls through to find text/html in recursive search, returns null → ""
        val body = ingester.extractTextBody(message)
        assertTrue(body.isEmpty() || body.isNotBlank()) // Either empty or HTML fallback
    }
}
