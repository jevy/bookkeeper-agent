package org.jevy.bookkeeper.sheets

import io.mockk.every
import io.mockk.mockk
import io.mockk.spyk
import org.jevy.bookkeeper.config.AppConfig
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class SheetsClientSearchTest {

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

    private fun makeSheetsClient(rows: List<List<Any>>): SheetsClient {
        val client = spyk(SheetsClient(config))
        every { client.readAllRows(any()) } returns rows
        return client
    }

    private val header = listOf<Any>(
        "Date", "Description", "Category", "Amount", "Account",
        "Account #", "Institution", "Month", "Week", "Transaction ID",
        "Check Number", "Full Description", "Note", "Receipt", "Source",
        "Categorized Date", "Date Added", "Metadata", "Categorized", "Tags", "Migration Notes",
    )

    private fun makeDataRow(
        description: String,
        category: String,
        fullDescription: String = description,
    ): List<Any> = listOf(
        "1/1/2026", description, category, "-\$50", "Visa",
        "xxxx1234", "TD", "1/1/2026", "12/29/2025", "txn-${description.hashCode()}",
        "", fullDescription, "", "", "Yodlee",
        "", "1/1/2026", "", "", "", "",
    )

    @Test
    fun `searchByDescription returns matching categorized rows`() {
        val rows = listOf(
            header,
            makeDataRow("COSTCO WHOLESAL", "Groceries", "COSTCO WHOLESALE #1234"),
            makeDataRow("AMAZON", "Shopping"),
            makeDataRow("COSTCO GAS", "Gas"),
        )
        val client = makeSheetsClient(rows)

        val results = client.searchByDescription("costco")

        assertEquals(2, results.size)
        assertTrue(results.all { it["Category"]?.isNotBlank() == true })
        assertTrue(results.any { it["Description"] == "COSTCO WHOLESAL" })
        assertTrue(results.any { it["Description"] == "COSTCO GAS" })
    }

    @Test
    fun `searchByDescription excludes uncategorized rows`() {
        val rows = listOf(
            header,
            makeDataRow("COSTCO WHOLESAL", ""),       // uncategorized
            makeDataRow("COSTCO GAS", "Gas"),           // categorized
        )
        val client = makeSheetsClient(rows)

        val results = client.searchByDescription("costco")

        assertEquals(1, results.size)
        assertEquals("Gas", results.first()["Category"])
    }

    @Test
    fun `searchByDescription matches on full description`() {
        val rows = listOf(
            header,
            makeDataRow("SHORT DESC", "Groceries", "COSTCO WHOLESALE STORE #1234"),
        )
        val client = makeSheetsClient(rows)

        val results = client.searchByDescription("costco")

        assertEquals(1, results.size)
    }

    @Test
    fun `searchByDescription is case insensitive`() {
        val rows = listOf(
            header,
            makeDataRow("Costco Wholesal", "Groceries"),
        )
        val client = makeSheetsClient(rows)

        val results = client.searchByDescription("COSTCO")

        assertEquals(1, results.size)
    }

    @Test
    fun `searchByDescription caps at 20 results`() {
        val dataRows = (1..30).map { makeDataRow("COSTCO $it", "Groceries") }
        val rows = listOf(header) + dataRows
        val client = makeSheetsClient(rows)

        val results = client.searchByDescription("costco")

        assertEquals(20, results.size)
    }

    @Test
    fun `searchByDescription returns empty for no matches`() {
        val rows = listOf(
            header,
            makeDataRow("AMAZON", "Shopping"),
        )
        val client = makeSheetsClient(rows)

        val results = client.searchByDescription("costco")

        assertTrue(results.isEmpty())
    }
}
