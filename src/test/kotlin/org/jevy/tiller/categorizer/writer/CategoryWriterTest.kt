package org.jevy.tiller.categorizer.writer

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.jevy.tiller.categorizer.config.AppConfig
import org.jevy.tiller.categorizer.sheets.SheetsClient
import org.jevy.tiller_categorizer_agent.Transaction
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class CategoryWriterTest {

    private val config = AppConfig(
        kafkaBootstrapServers = "localhost:9092",
        schemaRegistryUrl = "http://localhost:8081",
        googleSheetId = "test",
        googleCredentialsJson = "{}",
        anthropicApiKey = "",
        pollIntervalSeconds = 60,
    )

    private val sheetsClient = mockk<SheetsClient>(relaxed = true)
    private val writer = CategoryWriter(config, sheetsClient)

    @Test
    fun `findRow returns hint row when transaction ID matches`() {
        every { sheetsClient.readAllRows("Transactions!J5:J5") } returns
            listOf(listOf("txn-100" as Any))

        val row = writer.findRow("txn-100", 5)

        assertEquals(5, row)
    }

    @Test
    fun `findRow falls back to scan when hint row does not match`() {
        every { sheetsClient.readAllRows("Transactions!J5:J5") } returns
            listOf(listOf("txn-999" as Any))
        every { sheetsClient.readAllRows("Transactions!J:J") } returns listOf(
            listOf("Transaction ID" as Any), // header (row 1)
            listOf("txn-100" as Any),        // row 2
            listOf("txn-200" as Any),        // row 3
            listOf("txn-300" as Any),        // row 4
        )

        val row = writer.findRow("txn-300", 5)

        assertEquals(4, row) // 0-indexed position 3 + 1 = row 4
    }

    @Test
    fun `findRow returns null when transaction not found`() {
        every { sheetsClient.readAllRows("Transactions!J5:J5") } returns
            listOf(listOf("txn-other" as Any))
        every { sheetsClient.readAllRows("Transactions!J:J") } returns listOf(
            listOf("Transaction ID" as Any),
            listOf("txn-100" as Any),
        )

        val row = writer.findRow("txn-missing", 5)

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
            .setSheetRowNumber(5)
            .build()

        // findRow returns hint
        every { sheetsClient.readAllRows("Transactions!J5:J5") } returns
            listOf(listOf("txn-100" as Any))
        // existing category
        every { sheetsClient.readAllRows("Transactions!C5:C5") } returns
            listOf(listOf("Existing Category" as Any))

        writer.writeCategory(tx)

        // Should NOT write anything
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
            .setSheetRowNumber(5)
            .build()

        every { sheetsClient.readAllRows("Transactions!J5:J5") } returns
            listOf(listOf("txn-100" as Any))
        every { sheetsClient.readAllRows("Transactions!C5:C5") } returns
            listOf(listOf("" as Any))

        writer.writeCategory(tx)

        verify { sheetsClient.writeCell("Transactions!C5", "Groceries") }
        verify { sheetsClient.writeCell(match { it.startsWith("Transactions!P5") }, any()) }
    }
}
