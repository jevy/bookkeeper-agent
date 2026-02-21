package org.jevy.tiller.categorizer.producer

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull

class TransactionProducerTest {

    // Simulates a Google Sheet row: columns A-Q
    // A=Date, B=Description, C=Category, D=Amount, E=Account, F=Account#,
    // G=Institution, H=Month, I=Week, J=TransactionID, K=CheckNumber,
    // L=FullDescription, M=Note, N=Receipt, O=Source, P=CategorizedDate, Q=DateAdded
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
        val tx = TransactionProducer.rowToTransaction(row, 5)

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
        assertEquals(5, tx.getSheetRowNumber())
    }

    @Test
    fun `rowToTransaction returns null for already-categorized row`() {
        val row = makeRow(category = "Groceries")
        val tx = TransactionProducer.rowToTransaction(row, 5)

        assertNull(tx)
    }

    @Test
    fun `rowToTransaction returns null when transaction ID is missing`() {
        val row = makeRow(transactionId = "")
        val tx = TransactionProducer.rowToTransaction(row, 5)

        assertNull(tx)
    }

    @Test
    fun `rowToTransaction handles short rows gracefully`() {
        // Simulate a row with only first 10 columns (up to Transaction ID)
        val row = listOf<Any>("2/15/2026", "COSTCO", "", "-\$50", "Visa", "", "", "", "", "txn-short")
        val tx = TransactionProducer.rowToTransaction(row, 3)

        assertNotNull(tx)
        assertEquals("txn-short", tx.getTransactionId().toString())
        assertEquals("COSTCO", tx.getDescription().toString())
        assertNull(tx.getFullDescription()) // column 11 missing
        assertNull(tx.getSource()) // column 14 missing
    }
}
