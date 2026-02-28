package org.jevy.bookkeeper.sheets

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class TransactionMapperTest {

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
    fun `fromSheetRow maps all fields correctly`() {
        val tx = TransactionMapper.fromSheetRow(makeRow(), colIndex, "sheet-123")

        assertEquals("txn-12345", tx.getTransactionId().toString())
        assertEquals("2/15/2026", tx.getDate().toString())
        assertEquals("COSTCO WHOLESAL", tx.getDescription().toString())
        assertEquals("", tx.getCategory().toString())
        assertEquals("-\$384.91", tx.getAmount().toString())
        assertEquals("Chequing", tx.getAccount().toString())
        assertEquals("xxxx6404", tx.getAccountNumber().toString())
        assertEquals("TD Bank", tx.getInstitution().toString())
        assertEquals("2/1/2026", tx.getMonth().toString())
        assertEquals("2/10/2026", tx.getWeek().toString())
        assertEquals("COSTCO WHOLESALE #1234", tx.getFullDescription().toString())
        assertEquals("Yodlee", tx.getSource().toString())
        assertEquals("2/15/2026 10:00:00", tx.getDateAdded().toString())
        assertEquals("sheet-123", tx.getOwner().toString())
    }

    @Test
    fun `fromSheetRow generates durable ID when Transaction ID is blank`() {
        val tx = TransactionMapper.fromSheetRow(makeRow(transactionId = ""), colIndex, "sheet-123")

        assertTrue(tx.getTransactionId().toString().startsWith("durable-"))
        assertEquals(24, tx.getTransactionId().toString().length)
    }

    @Test
    fun `fromSheetRow preserves category`() {
        val tx = TransactionMapper.fromSheetRow(makeRow(category = "Groceries"), colIndex)

        assertEquals("Groceries", tx.getCategory().toString())
    }

    @Test
    fun `fromSheetRow handles short rows`() {
        val shortRow = listOf<Any>("2/15/2026", "COSTCO", "", "-\$50", "Visa", "", "", "", "", "txn-short")
        val tx = TransactionMapper.fromSheetRow(shortRow, colIndex)

        assertEquals("txn-short", tx.getTransactionId().toString())
        assertEquals("COSTCO", tx.getDescription().toString())
        assertNull(tx.getFullDescription())
        assertNull(tx.getSource())
    }

    @Test
    fun `fromSheetRow uses null owner when not provided`() {
        val tx = TransactionMapper.fromSheetRow(makeRow(), colIndex)

        assertNull(tx.getOwner())
    }
}
