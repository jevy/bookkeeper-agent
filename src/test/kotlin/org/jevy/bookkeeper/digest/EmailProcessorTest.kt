package org.jevy.bookkeeper.digest

import org.jevy.bookkeeper.config.AppConfig
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class EmailProcessorTest {

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

    private val processor = EmailProcessor(config)

    // --- parseCorrections tests ---

    @Test
    fun `parseCorrections extracts numbered corrections with colon`() {
        val body = """
            3: That's the gas station near my office, should be Gas
            5: Cleaning supplies from Amazon, probably Household
        """.trimIndent()

        val corrections = processor.parseCorrections(body)
        assertEquals(2, corrections.size)
        assertEquals(3, corrections[0].first)
        assertEquals("That's the gas station near my office, should be Gas", corrections[0].second)
        assertEquals(5, corrections[1].first)
        assertEquals("Cleaning supplies from Amazon, probably Household", corrections[1].second)
    }

    @Test
    fun `parseCorrections extracts corrections with dot separator`() {
        val body = "3. Gas station near office"

        val corrections = processor.parseCorrections(body)
        assertEquals(1, corrections.size)
        assertEquals(3, corrections[0].first)
        assertEquals("Gas station near office", corrections[0].second)
    }

    @Test
    fun `parseCorrections handles leading whitespace`() {
        val body = "  3: Gas station"

        val corrections = processor.parseCorrections(body)
        assertEquals(1, corrections.size)
        assertEquals(3, corrections[0].first)
    }

    @Test
    fun `parseCorrections skips non-correction lines`() {
        val body = """
            Hey, here are my corrections:
            3: Gas station
            Thanks!
        """.trimIndent()

        val corrections = processor.parseCorrections(body)
        assertEquals(1, corrections.size)
        assertEquals(3, corrections[0].first)
    }

    @Test
    fun `parseCorrections returns empty for no corrections`() {
        val body = "Thanks, looks good!"

        val corrections = processor.parseCorrections(body)
        assertEquals(0, corrections.size)
    }

    @Test
    fun `parseCorrections skips empty context`() {
        val body = "3: "

        val corrections = processor.parseCorrections(body)
        assertEquals(0, corrections.size)
    }

    // --- stripQuotedText tests ---

    @Test
    fun `stripQuotedText removes quoted lines`() {
        val body = """
            3: Gas station
            > Original message line
            > Another quoted line
        """.trimIndent()

        val stripped = processor.stripQuotedText(body)
        assertEquals("3: Gas station", stripped.trim())
    }

    @Test
    fun `stripQuotedText stops at reply header`() {
        val body = """
            3: Gas station
            5: Amazon household
            On Mar 1, 2026 at 7:00 AM digest@bookkeeper.com wrote:
            Here are yesterday's categorizations:
        """.trimIndent()

        val stripped = processor.stripQuotedText(body)
        val lines = stripped.trim().lines()
        assertEquals(2, lines.size)
        assertEquals("3: Gas station", lines[0])
        assertEquals("5: Amazon household", lines[1])
    }

    @Test
    fun `stripQuotedText preserves body with no quoted text`() {
        val body = "3: Gas station"

        val stripped = processor.stripQuotedText(body)
        assertEquals("3: Gas station", stripped.trim())
    }

    // --- extractDigestDate tests ---

    @Test
    fun `extractDigestDate parses standard subject`() {
        val date = processor.extractDigestDate("Bookkeeper Digest - Mar 1, 2026 (12 transactions)")
        assertEquals("2026-03-01", date)
    }

    @Test
    fun `extractDigestDate parses different months`() {
        assertEquals("2026-01-15", processor.extractDigestDate("Bookkeeper Digest - Jan 15, 2026 (5 transactions)"))
        assertEquals("2026-12-25", processor.extractDigestDate("Bookkeeper Digest - Dec 25, 2026 (1 transactions)"))
    }

    @Test
    fun `extractDigestDate returns null for non-digest subject`() {
        assertNull(processor.extractDigestDate("Re: Hello"))
        assertNull(processor.extractDigestDate("Random email subject"))
    }

    @Test
    fun `extractDigestDate returns null for malformed date`() {
        assertNull(processor.extractDigestDate("Bookkeeper Digest - Not a date (3 transactions)"))
    }
}
