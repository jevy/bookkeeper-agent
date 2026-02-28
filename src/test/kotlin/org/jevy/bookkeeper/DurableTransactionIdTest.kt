package org.jevy.bookkeeper

import org.jevy.bookkeeper_agent.Transaction
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue

class DurableTransactionIdTest {

    private val owner = "sheet-abc123"

    @Test
    fun `consistent hash for same inputs`() {
        val id1 = DurableTransactionId.generate(owner, "2/26/2026", "Shoppers Drug M", "-$3.00", "TD - Personal Chequing")
        val id2 = DurableTransactionId.generate(owner, "2/26/2026", "Shoppers Drug M", "-$3.00", "TD - Personal Chequing")
        assertEquals(id1, id2)
    }

    @Test
    fun `different hash for different owner`() {
        val id1 = DurableTransactionId.generate("sheet-a", "2/26/2026", "Shoppers Drug M", "-$3.00", "TD - Personal Chequing")
        val id2 = DurableTransactionId.generate("sheet-b", "2/26/2026", "Shoppers Drug M", "-$3.00", "TD - Personal Chequing")
        assertNotEquals(id1, id2)
    }

    @Test
    fun `different hash for different description`() {
        val id1 = DurableTransactionId.generate(owner, "2/26/2026", "Shoppers Drug M", "-$3.00", "TD - Personal Chequing")
        val id2 = DurableTransactionId.generate(owner, "2/26/2026", "Send E-tfr *CeA", "-$3.00", "TD - Personal Chequing")
        assertNotEquals(id1, id2)
    }

    @Test
    fun `different hash for different amount`() {
        val id1 = DurableTransactionId.generate(owner, "2/26/2026", "Shoppers Drug M", "-$3.00", "TD - Personal Chequing")
        val id2 = DurableTransactionId.generate(owner, "2/26/2026", "Shoppers Drug M", "-$80.00", "TD - Personal Chequing")
        assertNotEquals(id1, id2)
    }

    @Test
    fun `different hash for different date`() {
        val id1 = DurableTransactionId.generate(owner, "2/26/2026", "Shoppers Drug M", "-$3.00", "TD - Personal Chequing")
        val id2 = DurableTransactionId.generate(owner, "2/27/2026", "Shoppers Drug M", "-$3.00", "TD - Personal Chequing")
        assertNotEquals(id1, id2)
    }

    @Test
    fun `different hash for different account`() {
        val id1 = DurableTransactionId.generate(owner, "2/26/2026", "Shoppers Drug M", "-$3.00", "TD - Personal Chequing")
        val id2 = DurableTransactionId.generate(owner, "2/26/2026", "Shoppers Drug M", "-$3.00", "TD - Savings")
        assertNotEquals(id1, id2)
    }

    @Test
    fun `case insensitive`() {
        val id1 = DurableTransactionId.generate(owner, "2/26/2026", "Shoppers Drug M", "-$3.00", "TD - Personal Chequing")
        val id2 = DurableTransactionId.generate(owner, "2/26/2026", "SHOPPERS DRUG M", "-$3.00", "TD - Personal Chequing")
        assertEquals(id1, id2)
    }

    @Test
    fun `whitespace trimming`() {
        val id1 = DurableTransactionId.generate(owner, "2/26/2026", "Shoppers Drug M", "-$3.00", "TD - Personal Chequing")
        val id2 = DurableTransactionId.generate(owner, "  2/26/2026  ", "  Shoppers Drug M  ", "  -$3.00  ", "  TD - Personal Chequing  ")
        assertEquals(id1, id2)
    }

    @Test
    fun `amount normalization - dollar sign`() {
        val id1 = DurableTransactionId.generate(owner, "2/26/2026", "Test", "-$3.00", "Acct")
        val id2 = DurableTransactionId.generate(owner, "2/26/2026", "Test", "-3.00", "Acct")
        assertEquals(id1, id2)
    }

    @Test
    fun `amount normalization - spaces in amount`() {
        val id1 = DurableTransactionId.generate(owner, "2/26/2026", "Test", "-$3.00", "Acct")
        val id2 = DurableTransactionId.generate(owner, "2/26/2026", "Test", "- $3.00", "Acct")
        assertEquals(id1, id2)
    }

    @Test
    fun `amount normalization - commas`() {
        val id1 = DurableTransactionId.generate(owner, "2/26/2026", "Test", "-$1,234.56", "Acct")
        val id2 = DurableTransactionId.generate(owner, "2/26/2026", "Test", "-1234.56", "Acct")
        assertEquals(id1, id2)
    }

    @Test
    fun `prefix and length`() {
        val id = DurableTransactionId.generate(owner, "2/26/2026", "Shoppers Drug M", "-$3.00", "TD - Personal Chequing")
        assertTrue(id.startsWith("durable-"))
        assertEquals(24, id.length) // "durable-" (8) + 16 hex chars
    }

    @Test
    fun `example transaction 1`() {
        val id = DurableTransactionId.generate(owner, "2/26/2026", "Shoppers Drug M", "-$3.00", "TD - Personal Chequing")
        assertTrue(id.startsWith("durable-"))
        assertEquals(24, id.length)
    }

    @Test
    fun `example transaction 2`() {
        val id = DurableTransactionId.generate(owner, "2/26/2026", "Send E-tfr *CeA", "-$80.00", "TD - Personal Chequing")
        assertTrue(id.startsWith("durable-"))
        assertEquals(24, id.length)
    }

    @Test
    fun `two example transactions produce different IDs`() {
        val id1 = DurableTransactionId.generate(owner, "2/26/2026", "Shoppers Drug M", "-$3.00", "TD - Personal Chequing")
        val id2 = DurableTransactionId.generate(owner, "2/26/2026", "Send E-tfr *CeA", "-$80.00", "TD - Personal Chequing")
        assertNotEquals(id1, id2)
    }

    @Test
    fun `empty description still produces valid hash`() {
        val id = DurableTransactionId.generate(owner, "2/26/2026", "", "-$3.00", "TD - Personal Chequing")
        assertTrue(id.startsWith("durable-"))
        assertEquals(24, id.length)
    }

    @Test
    fun `real Amazon row without transaction ID`() {
        val id = DurableTransactionId.generate(
            owner = owner,
            date = "2/17/2026",
            description = "[Amazon Item] Amazon Basics Multipurpose Copy Printer Paper, 8.5\" x 11\", 20 lb, 3 Reams, 1500 Sheets, 92 Bright, White",
            amount = "-$37.27",
            account = "Visa - 9472",
        )
        assertTrue(id.startsWith("durable-"))
        assertEquals(24, id.length)
        // Same inputs produce same ID (deterministic)
        val id2 = DurableTransactionId.generate(
            owner = owner,
            date = "2/17/2026",
            description = "[Amazon Item] Amazon Basics Multipurpose Copy Printer Paper, 8.5\" x 11\", 20 lb, 3 Reams, 1500 Sheets, 92 Bright, White",
            amount = "-$37.27",
            account = "Visa - 9472",
        )
        assertEquals(id, id2)
    }

    @Test
    fun `generate from Transaction matches generate from fields`() {
        val tx = Transaction.newBuilder()
            .setTransactionId("ignored")
            .setDate("2/26/2026")
            .setDescription("Shoppers Drug M")
            .setAmount("-$3.00")
            .setAccount("TD - Personal Chequing")
            .setOwner(owner)
            .build()

        val fromFields = DurableTransactionId.generate(owner, "2/26/2026", "Shoppers Drug M", "-$3.00", "TD - Personal Chequing")
        val fromTx = DurableTransactionId.generate(tx)

        assertEquals(fromFields, fromTx)
    }

    @Test
    fun `generate from Transaction with null owner uses empty string`() {
        val tx = Transaction.newBuilder()
            .setTransactionId("ignored")
            .setDate("2/26/2026")
            .setDescription("Test")
            .setAmount("-$3.00")
            .setAccount("Acct")
            .build()

        val fromFields = DurableTransactionId.generate("", "2/26/2026", "Test", "-$3.00", "Acct")
        val fromTx = DurableTransactionId.generate(tx)

        assertEquals(fromFields, fromTx)
    }
}
