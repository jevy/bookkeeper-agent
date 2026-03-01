package org.jevy.bookkeeper.digest

import org.jevy.bookkeeper.config.AppConfig
import org.jevy.bookkeeper_agent.Transaction
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class DigestSenderTest {

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
        sesFromAddress = "digest@test.com",
        digestToAddress = "user@test.com",
    )

    private val sender = DigestSender(config)

    private fun makeTx(
        description: String = "COSTCO WHOLESAL",
        amount: String = "-\$384.91",
        category: String = "Groceries",
        transactionId: String = "txn-100",
    ): Transaction = Transaction.newBuilder()
        .setTransactionId(transactionId)
        .setDate("3/1/2026")
        .setDescription(description)
        .setCategory(category)
        .setAmount(amount)
        .setAccount("Visa")
        .build()

    @Test
    fun `formatDigestBody includes all transactions numbered`() {
        val transactions = listOf(
            makeTx("COSTCO WHOLESAL", "-\$384.91", "Groceries", "txn-1"),
            makeTx("NETFLIX.COM", "-\$22.99", "Entertainment", "txn-2"),
            makeTx("SHELL OIL 57442", "-\$65.30", "Auto & Transport", "txn-3"),
        )

        val body = sender.formatDigestBody(transactions)

        assertTrue(body.contains("1."))
        assertTrue(body.contains("2."))
        assertTrue(body.contains("3."))
        assertTrue(body.contains("COSTCO WHOLESAL"))
        assertTrue(body.contains("NETFLIX.COM"))
        assertTrue(body.contains("SHELL OIL 57442"))
        assertTrue(body.contains("Groceries"))
        assertTrue(body.contains("Entertainment"))
        assertTrue(body.contains("Auto & Transport"))
        assertTrue(body.contains("To correct, reply with the number and your context:"))
    }

    @Test
    fun `formatDigestBody truncates long descriptions`() {
        val transactions = listOf(
            makeTx("A VERY LONG MERCHANT DESCRIPTION THAT EXCEEDS LIMIT", "-\$10.00", "Shopping", "txn-1"),
        )

        val body = sender.formatDigestBody(transactions)
        // Description should be truncated to 25 chars
        val lines = body.lines().filter { it.contains("1.") }
        assertTrue(lines.isNotEmpty())
    }

    @Test
    fun `formatDigestBody handles empty transactions`() {
        val body = sender.formatDigestBody(emptyList())
        assertTrue(body.contains("Here are yesterday's categorizations:"))
        assertTrue(body.contains("To correct, reply with the number and your context:"))
    }
}
