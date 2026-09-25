package org.jevy.bookkeeper.sure

import org.jevy.bookkeeper_agent.Transaction
import org.slf4j.LoggerFactory
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeParseException
import java.util.concurrent.ConcurrentHashMap

sealed interface MatchResult {
    data class Matched(val transaction: SureTransaction, val rung: Int) : MatchResult
    /** reason is one of: account_unmapped, bad_amount, bad_date, no_match, ambiguous */
    data class Unmatched(val reason: String) : MatchResult
}

/**
 * Finds the Sure transaction for a Sheet transaction. The two share no key, so this
 * is a ladder: cache, exact (account, date, amount), then a date window, then
 * description similarity to break ties. Ambiguity is never guessed through.
 */
class TransactionMatcher(
    private val client: SureClient,
    private val accountMap: Map<String, String>,
    private val dateWindowDays: Long = 3,
) {
    private val logger = LoggerFactory.getLogger(TransactionMatcher::class.java)
    private val sheetDate = DateTimeFormatter.ofPattern("M/d/yyyy")

    /** Rung 1. Process lifetime only; the PATCH is idempotent so durability buys nothing. */
    private val cache = ConcurrentHashMap<String, SureTransaction>()

    fun match(tx: Transaction): MatchResult {
        val transactionId = tx.getTransactionId().toString()
        cache[transactionId]?.let { return MatchResult.Matched(it, rung = 1) }

        val accountId = accountMap[tx.getAccount().toString().trim()]
            ?: return MatchResult.Unmatched("account_unmapped")

        val sheetAmount = try {
            AmountConvention.parseSheetAmount(tx.getAmount().toString())
        } catch (e: IllegalArgumentException) {
            return MatchResult.Unmatched("bad_amount")
        }
        val date = try {
            LocalDate.parse(tx.getDate().toString().trim(), sheetDate)
        } catch (e: DateTimeParseException) {
            return MatchResult.Unmatched("bad_date")
        }

        val expectedCents = AmountConvention.toCents(sheetAmount)
        val entryAmount = AmountConvention.toSureEntryAmount(sheetAmount)

        fun query(start: LocalDate, end: LocalDate): List<SureTransaction> =
            client.listTransactions(accountId, start, end, entryAmount, entryAmount)
                .filter { it.signedAmountCents == expectedCents }

        // Rung 2: exact date
        val exact = query(date, date)
        decide(exact, rung = 2)?.let { return remember(transactionId, it) }

        // Rung 3: date window (only if rung 2 had nothing at all; several exact hits go to rung 4)
        if (exact.isEmpty()) {
            val windowed = query(date.minusDays(dateWindowDays), date.plusDays(dateWindowDays))
            decide(windowed, rung = 3)?.let { return remember(transactionId, it) }
            if (windowed.isEmpty()) return MatchResult.Unmatched("no_match")
            return disambiguate(tx, windowed)?.let { remember(transactionId, MatchResult.Matched(it, rung = 4)) }
                ?: MatchResult.Unmatched("ambiguous")
        }

        // Rung 4: several exact-date candidates
        return disambiguate(tx, exact)?.let { remember(transactionId, MatchResult.Matched(it, rung = 4)) }
            ?: MatchResult.Unmatched("ambiguous")
    }

    private fun decide(candidates: List<SureTransaction>, rung: Int): MatchResult? =
        if (candidates.size == 1) MatchResult.Matched(candidates[0], rung) else null

    private fun remember(transactionId: String, result: MatchResult): MatchResult {
        if (result is MatchResult.Matched) cache[transactionId] = result.transaction
        return result
    }

    /** Returns the single best candidate by description similarity, or null if there is no clear winner. */
    private fun disambiguate(tx: Transaction, candidates: List<SureTransaction>): SureTransaction? {
        val target = normalize(tx.getDescription().toString())
        val scored = candidates.map { it to similarity(target, normalize(it.name)) }.sortedByDescending { it.second }
        val (best, bestScore) = scored[0]
        val runnerUp = scored.getOrNull(1)?.second ?: 0.0
        val clearWinner = bestScore >= 0.5 && bestScore - runnerUp >= 0.2
        if (!clearWinner) {
            logger.info("Ambiguous match for {}: {}", tx.getTransactionId(), scored.map { "${it.first.name}=${"%.2f".format(it.second)}" })
            return null
        }
        return best
    }

    private fun normalize(s: String) = s.lowercase().replace(Regex("[^a-z0-9 ]"), " ").replace(Regex("\\s+"), " ").trim()

    /** Token overlap (Jaccard) plus a prefix bonus, both in [0,1]; enough to separate COSTCO from SHELL. */
    private fun similarity(a: String, b: String): Double {
        if (a.isEmpty() || b.isEmpty()) return 0.0
        val ta = a.split(' ').toSet()
        val tb = b.split(' ').toSet()
        val jaccard = ta.intersect(tb).size.toDouble() / ta.union(tb).size
        val prefix = if (b.startsWith(a) || a.startsWith(b)) 1.0 else 0.0
        return maxOf(jaccard, prefix)
    }
}
