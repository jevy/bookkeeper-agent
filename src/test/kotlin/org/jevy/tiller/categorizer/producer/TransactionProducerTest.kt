package org.jevy.tiller.categorizer.producer

import org.junit.jupiter.api.Test
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull

class TransactionProducerTest {

    private val colIndex = mapOf(
        "Date" to 0, "Description" to 1, "Category" to 2, "Amount" to 3,
        "Account" to 4, "Account #" to 5, "Institution" to 6, "Month" to 7,
        "Week" to 8, "Transaction ID" to 9, "Check Number" to 10,
        "Full Description" to 11, "Note" to 12, "Receipt" to 13, "Source" to 14,
        "Categorized Date" to 15, "Date Added" to 16,
    )

    private fun makeRow(
        date: String = "2/15/2026",
        description: String = "COSTCO WHOLESAL",
        category: String = "",
        amount: String = "-\$384.91",
        account: String = "Chequing",
        accountNumber: String = "xxxx6404",
        institution: String = "TD Bank",
        month: String = "2/1/2026",
        week: String = "2/10/2026",
        transactionId: String = "txn-12345",
        checkNumber: String = "",
        fullDescription: String = "COSTCO WHOLESALE #1234",
        note: String = "",
        receipt: String = "",
        source: String = "Yodlee",
        categorizedDate: String = "",
        dateAdded: String = "2/15/2026 10:00:00",
    ): List<Any> = listOf(
        date, description, category, amount, account, accountNumber,
        institution, month, week, transactionId, checkNumber,
        fullDescription, note, receipt, source, categorizedDate, dateAdded,
    )

    @Test
    fun `rowToTransaction maps uncategorized row correctly`() {
        val row = makeRow()
        val tx = TransactionProducer.rowToTransaction(row, colIndex)

        assertNotNull(tx)
        assertEquals("txn-12345", tx.getTransactionId().toString())
        assertEquals("2/15/2026", tx.getDate().toString())
        assertEquals("COSTCO WHOLESAL", tx.getDescription().toString())
        assertNull(tx.getCategory())
        assertEquals("-\$384.91", tx.getAmount().toString())
        assertEquals("Chequing", tx.getAccount().toString())
        assertEquals("xxxx6404", tx.getAccountNumber().toString())
        assertEquals("TD Bank", tx.getInstitution().toString())
        assertEquals("COSTCO WHOLESALE #1234", tx.getFullDescription().toString())
        assertEquals("Yodlee", tx.getSource().toString())
    }

    @Test
    fun `rowToTransaction returns null for already-categorized row`() {
        val row = makeRow(category = "Groceries")
        val tx = TransactionProducer.rowToTransaction(row, colIndex)

        assertNull(tx)
    }

    @Test
    fun `rowToTransaction returns null when transaction ID is missing`() {
        val row = makeRow(transactionId = "")
        val tx = TransactionProducer.rowToTransaction(row, colIndex)

        assertNull(tx)
    }

    @Test
    fun `rowToTransaction skips transactions older than maxAgeDays`() {
        val fmt = DateTimeFormatter.ofPattern("M/d/yyyy")
        val oldDate = LocalDate.now().minusDays(400).format(fmt)
        val row = makeRow(date = oldDate)
        val tx = TransactionProducer.rowToTransaction(row, colIndex, maxAgeDays = 365)

        assertNull(tx)
    }

    @Test
    fun `rowToTransaction includes transactions within maxAgeDays`() {
        val fmt = DateTimeFormatter.ofPattern("M/d/yyyy")
        val recentDate = LocalDate.now().minusDays(30).format(fmt)
        val row = makeRow(date = recentDate)
        val tx = TransactionProducer.rowToTransaction(row, colIndex, maxAgeDays = 365)

        assertNotNull(tx)
    }

    @Test
    fun `rowToTransaction handles short rows gracefully`() {
        val row = listOf<Any>("2/15/2026", "COSTCO", "", "-\$50", "Visa", "", "", "", "", "txn-short")
        val tx = TransactionProducer.rowToTransaction(row, colIndex)

        assertNotNull(tx)
        assertEquals("txn-short", tx.getTransactionId().toString())
        assertEquals("COSTCO", tx.getDescription().toString())
        assertNull(tx.getFullDescription()) // column 11 missing
        assertNull(tx.getSource()) // column 14 missing
    }
}
