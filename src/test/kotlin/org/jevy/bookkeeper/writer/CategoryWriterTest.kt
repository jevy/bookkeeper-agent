package org.jevy.bookkeeper.writer

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.jevy.bookkeeper.config.AppConfig
import org.jevy.bookkeeper.sheets.SheetsClient
import org.jevy.bookkeeper_agent.Transaction
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class CategoryWriterTest {

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
    )

    private val header = listOf<Any>(
        "Date", "Description", "Category", "Amount", "Account",
        "Account #", "Institution", "Month", "Week", "Transaction ID",
        "Check Number", "Full Description", "Note", "Receipt", "Source",
        "Categorized Date", "Date Added",
    )

    private val sheetsClient = mockk<SheetsClient>(relaxed = true).also {
        every { it.readAllRows("Transactions!1:1") } returns listOf(header)
    }
    private val writer = CategoryWriter(config, sheetsClient)

    @Test
    fun `findRow returns correct row by scanning`() {
        every { sheetsClient.readAllRows("Transactions!J:J") } returns listOf(
            listOf("Transaction ID" as Any), // header (row 1)
            listOf("txn-100" as Any),        // row 2
            listOf("txn-200" as Any),        // row 3
            listOf("txn-300" as Any),        // row 4
        )

        val row = writer.findRow("txn-300")

        assertEquals(4, row)
    }

    @Test
    fun `findRow returns null when transaction not found`() {
        every { sheetsClient.readAllRows("Transactions!J:J") } returns listOf(
            listOf("Transaction ID" as Any),
            listOf("txn-100" as Any),
        )

        val row = writer.findRow("txn-missing")

        assertNull(row)
    }

    @Test
    fun `writeCategory skips when row already has category`() {
        val tx = Transaction.newBuilder()
            .setTransactionId("txn-100")
            .setDate("1/1/2026")
            .setDescription("Test")
            .setCategory("Groceries")
            .setAmount("\$10")
            .setAccount("Visa")
            .build()

        every { sheetsClient.readAllRows("Transactions!J:J") } returns listOf(
            listOf("Transaction ID" as Any),
            listOf("txn-100" as Any), // row 2
        )
        every { sheetsClient.readAllRows("Transactions!C2:C2") } returns
            listOf(listOf("Existing Category" as Any))

        writer.writeCategory(tx)

        verify(exactly = 0) { sheetsClient.writeCell(any(), any()) }
    }

    @Test
    fun `writeCategory writes category and date when row is empty`() {
        val tx = Transaction.newBuilder()
            .setTransactionId("txn-100")
            .setDate("1/1/2026")
            .setDescription("Test")
            .setCategory("Groceries")
            .setAmount("\$10")
            .setAccount("Visa")
            .build()

        every { sheetsClient.readAllRows("Transactions!J:J") } returns listOf(
            listOf("Transaction ID" as Any),
            listOf("txn-100" as Any), // row 2
        )
        every { sheetsClient.readAllRows("Transactions!C2:C2") } returns
            listOf(listOf("" as Any))

        writer.writeCategory(tx)

        verify { sheetsClient.writeCell("Transactions!C2", "Groceries") }
        verify { sheetsClient.writeCell(match { it.startsWith("Transactions!P2") }, any()) }
    }
}
