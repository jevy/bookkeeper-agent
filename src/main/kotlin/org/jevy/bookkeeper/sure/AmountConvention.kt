package org.jevy.bookkeeper.sure

import java.math.BigDecimal
import java.math.RoundingMode

/**
 * The single place that knows how Sheet amounts and Sure amounts relate.
 *
 * Sheet / Avro: expense negative ("-$384.91"), income positive ("$1,371.24").
 * Sure API response `signed_amount_cents`: same sign convention as the Sheet.
 * Sure database column `entries.amount`, which the `min_amount`/`max_amount`
 * query parameters filter on: inverted (expense positive, income negative).
 *
 * So responses are compared with [toCents] directly, and only the query
 * parameters go through [toSureEntryAmount].
 */
object AmountConvention {

    fun parseSheetAmount(raw: String): BigDecimal {
        val cleaned = raw.trim().replace("$", "").replace(",", "")
        require(cleaned.isNotEmpty()) { "Blank amount" }
        return try {
            BigDecimal(cleaned)
        } catch (e: NumberFormatException) {
            throw IllegalArgumentException("Unparseable amount '$raw'", e)
        }
    }

    fun toCents(amount: BigDecimal): Long =
        amount.setScale(2, RoundingMode.HALF_UP).movePointRight(2).longValueExact()

    fun toSureEntryAmount(sheetAmount: BigDecimal): BigDecimal = sheetAmount.negate()
}
