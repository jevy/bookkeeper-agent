package org.jevy.bookkeeper.sure

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.jevy.bookkeeper_agent.Transaction
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import java.time.LocalDate
import kotlin.test.assertEquals
import kotlin.test.assertIs

class TransactionMatcherTest {

    private val client = mockk<SureClient>()
    private val accountMap = mapOf("Chequing" to "acct-1")
    private val matcher = TransactionMatcher(client, accountMap)

    private fun sheetTx(
        id: String = "txn-1", date: String = "2/15/2026", description: String = "COSTCO WHOLESAL",
        amount: String = "-\$384.91", account: String = "Chequing",
    ): Transaction = Transaction.newBuilder()
        .setTransactionId(id).setDate(date).setDescription(description).setCategory("Groceries")
        .setAmount(amount).setAccount(account).build()

    private fun sureTx(id: String, date: String = "2026-02-15", name: String = "COSTCO WHOLESALE #123", cents: Long = -38491) =
        SureTransaction(id, LocalDate.parse(date), name, cents, "acct-1", null, "TRN-$id")

    private fun stubQuery(start: String, end: String, vararg results: SureTransaction) {
        every {
            client.listTransactions("acct-1", LocalDate.parse(start), LocalDate.parse(end), BigDecimal("384.91"), BigDecimal("384.91"))
        } returns results.toList()
    }

    @Test
    fun `rung 2 exact date and amount with one result`() {
        stubQuery("2026-02-15", "2026-02-15", sureTx("s-1"))
        val r = matcher.match(sheetTx())
        assertEquals(MatchResult.Matched(sureTx("s-1"), rung = 2), r)
    }

    @Test
    fun `rung 3 widens to plus or minus three days when exact misses`() {
        stubQuery("2026-02-15", "2026-02-15")
        stubQuery("2026-02-12", "2026-02-18", sureTx("s-1", date = "2026-02-18"))
        assertEquals(MatchResult.Matched(sureTx("s-1", date = "2026-02-18"), rung = 3), matcher.match(sheetTx()))
    }

    @Test
    fun `rung 3 window is inclusive at three days`() {
        stubQuery("2026-02-15", "2026-02-15")
        stubQuery("2026-02-12", "2026-02-18", sureTx("s-1", date = "2026-02-12"))
        assertIs<MatchResult.Matched>(matcher.match(sheetTx()))
    }

    @Test
    fun `rung 4 disambiguates several candidates by description`() {
        stubQuery("2026-02-15", "2026-02-15", sureTx("s-1", name = "COSTCO WHOLESALE"), sureTx("s-2", name = "SHELL GAS"))
        val r = matcher.match(sheetTx(description = "COSTCO WHOLESAL"))
        assertEquals(MatchResult.Matched(sureTx("s-1", name = "COSTCO WHOLESALE"), rung = 4), r)
    }

    @Test
    fun `rung 4 with no clear winner is ambiguous`() {
        stubQuery("2026-02-15", "2026-02-15", sureTx("s-1", name = "COSTCO WHOLESALE"), sureTx("s-2", name = "COSTCO WHOLESALE"))
        assertEquals(MatchResult.Unmatched("ambiguous"), matcher.match(sheetTx()))
    }

    @Test
    fun `no candidates in either window is no_match`() {
        stubQuery("2026-02-15", "2026-02-15")
        stubQuery("2026-02-12", "2026-02-18")
        assertEquals(MatchResult.Unmatched("no_match"), matcher.match(sheetTx()))
    }

    @Test
    fun `unmapped account is rejected before any API call`() {
        assertEquals(MatchResult.Unmatched("account_unmapped"), matcher.match(sheetTx(account = "Mystery")))
        verify(exactly = 0) { client.listTransactions(any(), any(), any(), any(), any()) }
    }

    @Test
    fun `unparseable amount is rejected before any API call`() {
        assertEquals(MatchResult.Unmatched("bad_amount"), matcher.match(sheetTx(amount = "n/a")))
        verify(exactly = 0) { client.listTransactions(any(), any(), any(), any(), any()) }
    }

    @Test
    fun `unparseable date is rejected before any API call`() {
        assertEquals(MatchResult.Unmatched("bad_date"), matcher.match(sheetTx(date = "2026-02-15")))
        verify(exactly = 0) { client.listTransactions(any(), any(), any(), any(), any()) }
    }

    @Test
    fun `rung 1 cache returns the previous match without querying`() {
        stubQuery("2026-02-15", "2026-02-15", sureTx("s-1"))
        matcher.match(sheetTx())
        val second = matcher.match(sheetTx())
        assertEquals(MatchResult.Matched(sureTx("s-1"), rung = 1), second)
        verify(exactly = 1) { client.listTransactions(any(), any(), any(), any(), any()) }
    }

    @Test
    fun `income amount flips sign for the query parameters`() {
        every {
            client.listTransactions("acct-1", LocalDate.parse("2026-02-15"), LocalDate.parse("2026-02-15"), BigDecimal("-1371.24"), BigDecimal("-1371.24"))
        } returns listOf(sureTx("s-1", cents = 137124))
        assertIs<MatchResult.Matched>(matcher.match(sheetTx(amount = "\$1,371.24")))
    }

    @Test
    fun `a candidate whose signed cents disagree is dropped`() {
        // Server-side filter is on the inverted column; belt and braces on the response.
        stubQuery("2026-02-15", "2026-02-15", sureTx("s-1", cents = 38491))
        stubQuery("2026-02-12", "2026-02-18", sureTx("s-1", cents = 38491))
        assertEquals(MatchResult.Unmatched("no_match"), matcher.match(sheetTx()))
    }
}
