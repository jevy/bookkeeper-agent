package org.jevy.bookkeeper.sure

import org.jevy.bookkeeper_agent.Transaction
import org.junit.jupiter.api.Test
import java.time.LocalDate
import kotlin.test.assertEquals

class SureSyncTest {

    private val header: List<Any> = listOf("", "Date", "Description", "Category", "Amount", "Account", "Transaction ID")
    private fun row(id: String, category: String, date: String = "9/21/2026", account: String = "TD - Personal Chequing"): List<Any> =
        listOf("", date, "LOBLAW SUPERMAR", category, "-\$293.00", account, id)

    private val mapped = setOf("TD - Personal Chequing")
    private val cutoff = LocalDate.of(2025, 10, 1)

    private fun select(rows: List<List<Any>>, lastPublished: Map<String, String> = emptyMap()) =
        SureSync.select(listOf(header) + rows, "sheet-1", lastPublished, mapped, cutoff)

    @Test
    fun `an AutoCat or hand categorization never published is synced`() {
        val s = select(listOf(row("t-1", "Groceries")))
        assertEquals(listOf("t-1"), s.toSync.map { it.getTransactionId().toString() })
        assertEquals("Groceries", s.toSync.single().getCategory().toString())
    }

    @Test
    fun `a row already published with the same category costs nothing`() {
        val s = select(listOf(row("t-1", "Groceries")), lastPublished = mapOf("t-1" to "Groceries"))
        assertEquals(emptyList(), s.toSync)
        assertEquals(1, s.inSync)
    }

    @Test
    fun `a row re-categorized in the Sheet since it was published is synced again`() {
        val s = select(listOf(row("t-1", "Restaurants")), lastPublished = mapOf("t-1" to "Groceries"))
        assertEquals(listOf("Restaurants"), s.toSync.map { it.getCategory().toString() })
    }

    @Test
    fun `uncategorized, too old, unparseable dates and unmapped accounts are skipped and counted`() {
        val s = select(listOf(
            row("t-1", ""),
            row("t-2", "Groceries", date = "9/30/2025"),
            row("t-3", "Groceries", date = "not a date"),
            row("t-4", "Groceries", account = "Wealthsimple"),
            row("t-5", "Groceries"),
        ))
        assertEquals(listOf("t-5"), s.toSync.map { it.getTransactionId().toString() })
        assertEquals(1, s.uncategorized)
        assertEquals(2, s.tooOld)
        assertEquals(1, s.unmappedAccount)
    }

    @Test
    fun `latestCategories keeps the last category per key and forgets tombstoned keys`() {
        fun tx(id: String, category: String) = Transaction.newBuilder()
            .setTransactionId(id).setDate("1/1/2026").setDescription("d").setAmount("-\$1").setAccount("a")
            .setCategory(category).build()
        val latest = SureSync.latestCategories(listOf(
            "t-1" to tx("t-1", "Groceries"),
            "t-2" to tx("t-2", "Travel"),
            "t-1" to tx("t-1", "Restaurants"),
            "t-2" to null,
        ))
        assertEquals(mapOf("t-1" to "Restaurants"), latest)
    }
}
