package org.jevy.tiller.categorizer

import org.jevy.tiller_categorizer_agent.Transaction
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class AvroSchemaTest {

    @Test
    fun `Transaction builder round-trips all required fields`() {
        val tx = Transaction.newBuilder()
            .setTransactionId("txn-001")
            .setDate("2/15/2026")
            .setDescription("COSTCO WHOLESAL")
            .setAmount("-\$384.91")
            .setAccount("Chequing")
            .build()

        assertEquals("txn-001", tx.getTransactionId().toString())
        assertEquals("2/15/2026", tx.getDate().toString())
        assertEquals("COSTCO WHOLESAL", tx.getDescription().toString())
        assertEquals("-\$384.91", tx.getAmount().toString())
        assertEquals("Chequing", tx.getAccount().toString())
    }

    @Test
    fun `Transaction nullable fields default to null`() {
        val tx = Transaction.newBuilder()
            .setTransactionId("txn-002")
            .setDate("1/1/2026")
            .setDescription("Test")
            .setAmount("\$10.00")
            .setAccount("Savings")
            .build()

        assertNull(tx.getCategory())
        assertNull(tx.getAccountNumber())
        assertNull(tx.getInstitution())
        assertNull(tx.getMonth())
        assertNull(tx.getWeek())
        assertNull(tx.getCheckNumber())
        assertNull(tx.getFullDescription())
        assertNull(tx.getNote())
        assertNull(tx.getSource())
        assertNull(tx.getDateAdded())
    }

    @Test
    fun `Transaction category can be set`() {
        val tx = Transaction.newBuilder()
            .setTransactionId("txn-003")
            .setDate("1/1/2026")
            .setDescription("Test")
            .setCategory("Groceries")
            .setAmount("\$10.00")
            .setAccount("Savings")
            .build()

        assertEquals("Groceries", tx.getCategory().toString())
    }

    @Test
    fun `Transaction can be copied and modified via newBuilder`() {
        val original = Transaction.newBuilder()
            .setTransactionId("txn-004")
            .setDate("1/1/2026")
            .setDescription("COSTCO")
            .setAmount("-\$50.00")
            .setAccount("Visa")
            .build()

        val categorized = Transaction.newBuilder(original)
            .setCategory("Groceries")
            .build()

        assertEquals("txn-004", categorized.getTransactionId().toString())
        assertEquals("Groceries", categorized.getCategory().toString())
        assertEquals("COSTCO", categorized.getDescription().toString())
    }
}
