package org.jevy.bookkeeper.writer

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.jevy.bookkeeper.config.AppConfig
import org.jevy.bookkeeper.sheets.SheetsClient
import org.jevy.bookkeeper.sheets.SheetsUnavailableException
import org.jevy.bookkeeper_agent.Transaction
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertTrue

/**
 * The backfill of 2026-09 dead-lettered 678 records because every Sheets failure,
 * quota refusals included, came back as Rejected. Rejected commits the offset and
 * writes to transactions.write-failed, so a three minute quota blip discarded work
 * that was never wrong. A quota refusal must come back Unavailable instead.
 */
class SheetsSinkTest {

    private val config = AppConfig(
        kafkaBootstrapServers = "localhost:9092",
        schemaRegistryUrl = "http://localhost:8081",
        googleSheetId = "sheet-123",
        googleCredentialsJson = "{}",
        openrouterApiKey = "",
        maxTransactionAgeDays = 365,
        maxTransactions = 0,
        additionalContextPrompt = null,
        model = "",
    )

    private val header = listOf<Any>(
        "Date", "Description", "Category", "Amount", "Account",
        "Account #", "Institution", "Month", "Week", "Transaction ID",
        "Check Number", "Full Description", "Note", "Receipt", "Source",
        "Categorized Date", "Date Added", "Metadata", "Categorized", "Tags", "Migration Notes",
    )

    private fun sheetRow(id: String, description: String, category: String = ""): List<Any> = listOf(
        "1/1/2026", description, category, "-\$50", "Visa",
        "xxxx1234", "TD", "1/1/2026", "12/29/2025", id,
        "", description, "", "", "Yodlee",
        "", "1/1/2026", "", "", "", "",
    )

    private fun tx(id: String, description: String, category: String = "Groceries"): Transaction =
        Transaction.newBuilder()
            .setTransactionId(id)
            .setDate("1/1/2026")
            .setDescription(description)
            .setAmount("-\$50")
            .setAccount("Visa")
            .setCategory(category)
            .build()

    private fun quotaRefusal() = SheetsUnavailableException(
        "Sheets quota still exhausted after 4 attempts on read Transactions!A:U: " +
            "Quota exceeded for quota metric 'Read requests'"
    )

    @Test
    fun `a quota refusal makes the sink unavailable rather than rejecting the record`() {
        val client = mockk<SheetsClient>()
        every { client.readAllRows() } throws quotaRefusal()

        val result = SheetsSink(config, client).write(tx("txn-1", "COSTCO"))

        assertIs<SinkResult.Unavailable>(result, "a 429 must never dead-letter; got $result")
        assertIs<SheetsUnavailableException>(result.cause)
    }

    @Test
    fun `a transaction with no row in the sheet is still rejected`() {
        val client = mockk<SheetsClient>(relaxed = true)
        every { client.readAllRows() } returns listOf(header, sheetRow("txn-other", "SOMEWHERE"))

        val result = SheetsSink(config, client).write(tx("txn-missing", "NOWHERE"))

        assertIs<SinkResult.Rejected>(result)
        assertEquals("row_not_found", result.reason)
    }

    @Test
    fun `a failure that is not a quota refusal is still rejected`() {
        val client = mockk<SheetsClient>()
        every { client.readAllRows() } throws IllegalStateException("malformed sheet")

        val result = SheetsSink(config, client).write(tx("txn-1", "COSTCO"))

        assertIs<SinkResult.Rejected>(result)
        assertEquals("exception", result.reason)
    }

    @Test
    fun `the full sheet is read once and reused for later records`() {
        val client = mockk<SheetsClient>(relaxed = true)
        every { client.readAllRows() } returns
            listOf(header, sheetRow("txn-1", "COSTCO"), sheetRow("txn-2", "LOBLAWS"))
        every { client.readAllRows(match { it != "Transactions!A:U" }) } returns listOf(listOf(""))

        val sink = SheetsSink(config, client)
        sink.write(tx("txn-1", "COSTCO"))
        sink.write(tx("txn-2", "LOBLAWS"))

        // Roughly two reads per record is what exhausted the quota. One cached full-sheet
        // read plus one cell read per record is what makes a 778 record replay finish.
        verify(exactly = 1) { client.readAllRows() }
    }

    @Test
    fun `column letters come from the cached sheet rather than a separate header read`() {
        val client = mockk<SheetsClient>(relaxed = true)
        every { client.readAllRows() } returns listOf(header, sheetRow("txn-1", "COSTCO"))
        every { client.readAllRows(match { it != "Transactions!A:U" }) } returns listOf(listOf(""))

        SheetsSink(config, client).write(tx("txn-1", "COSTCO"))

        verify(exactly = 0) { client.readAllRows("Transactions!1:1") }
        // Category is the third column of the header above.
        verify { client.writeCell("Transactions!C2", "Groceries") }
    }

    @Test
    fun `a row added after the cache was filled is found after one refresh`() {
        val client = mockk<SheetsClient>(relaxed = true)
        var rows = listOf(header, sheetRow("txn-1", "COSTCO"))
        every { client.readAllRows() } answers { rows }
        every { client.readAllRows(match { it != "Transactions!A:U" }) } returns listOf(listOf(""))

        val sink = SheetsSink(config, client)
        sink.write(tx("txn-1", "COSTCO"))
        rows = listOf(header, sheetRow("txn-1", "COSTCO"), sheetRow("txn-2", "LOBLAWS"))

        val result = sink.write(tx("txn-2", "LOBLAWS"))

        assertTrue(result is SinkResult.Written, "a row added after startup must not be dead-lettered; got $result")
        verify(exactly = 2) { client.readAllRows() }
    }
}
