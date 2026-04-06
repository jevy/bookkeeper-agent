package org.jevy.bookkeeper.report

import org.jevy.bookkeeper.config.AppConfig
import org.jevy.bookkeeper.sheets.SheetsClient
import org.slf4j.LoggerFactory
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.ses.SesClient
import software.amazon.awssdk.services.ses.model.*
import java.time.LocalDate
import java.time.YearMonth
import java.time.format.DateTimeFormatter
import kotlin.math.abs

class SpendingReport(private val config: AppConfig, private val mode: String) {

    private val logger = LoggerFactory.getLogger(SpendingReport::class.java)
    private val dateFormatter = DateTimeFormatter.ofPattern("M/d/yyyy")
    private val oneTimeProjectCategories = setOf("Shed", "Renovations")

    data class ParsedTransaction(
        val date: LocalDate,
        val amount: Double,
        val category: String,
        val description: String,
        val isExpense: Boolean,
    )

    fun run() {
        val sheetsClient = SheetsClient(config)

        val transactionRows = sheetsClient.readAllRows("Transactions!A:U")
        val categoryRows = sheetsClient.readAllRows("Categories!A:D")

        val excludedCategories = buildExcludedCategories(categoryRows)
        val transactions = parseTransactions(transactionRows, excludedCategories)

        logger.info("Parsed {} expense transactions after exclusions", transactions.size)

        val htmlBody = when (mode) {
            "weekly" -> buildWeeklyReport(transactions)
            "monthly" -> buildMonthlyReport(transactions)
            else -> throw IllegalArgumentException("Unknown mode: $mode")
        }

        val subject = when (mode) {
            "weekly" -> {
                val now = LocalDate.now()
                val monthName = now.month.name.lowercase().replaceFirstChar { it.uppercase() }
                val dateStr = now.format(DateTimeFormatter.ofPattern("MMM d, yyyy"))
                "$monthName Month-to-Date - $dateStr"
            }
            "monthly" -> {
                val lastMonth = YearMonth.now().minusMonths(1)
                val monthYear = lastMonth.format(DateTimeFormatter.ofPattern("MMMM yyyy"))
                "Monthly Wrap-up - $monthYear"
            }
            else -> "Spending Report"
        }

        sendEmail(subject, htmlBody)
        logger.info("Sent {} report email", mode)
    }

    internal fun buildExcludedCategories(categoryRows: List<List<Any>>): Set<String> {
        if (categoryRows.size <= 1) return oneTimeProjectCategories

        val header = categoryRows.first().map { it.toString() }
        val typeIdx = header.indexOf("Type")
        val hideIdx = header.indexOf("Hide From Reports")
        val nameIdx = header.indexOf("Category").takeIf { it >= 0 } ?: 0

        val excluded = mutableSetOf<String>()
        excluded.addAll(oneTimeProjectCategories)

        for (row in categoryRows.drop(1)) {
            if (row.isEmpty()) continue
            val name = row.getOrNull(nameIdx)?.toString() ?: continue
            val type = if (typeIdx >= 0) row.getOrNull(typeIdx)?.toString() ?: "" else ""
            val hide = if (hideIdx >= 0) row.getOrNull(hideIdx)?.toString() ?: "" else ""

            if (type.equals("Transfer", ignoreCase = true) || hide.equals("Hide", ignoreCase = true)) {
                excluded.add(name)
            }
        }

        logger.info("Excluding {} categories: {}", excluded.size, excluded)
        return excluded
    }

    internal fun parseTransactions(rows: List<List<Any>>, excludedCategories: Set<String>): List<ParsedTransaction> {
        if (rows.size <= 1) return emptyList()

        val header = rows.first().map { it.toString() }
        val dateIdx = header.indexOf("Date")
        val amountIdx = header.indexOf("Amount")
        val categoryIdx = header.indexOf("Category")
        val descIdx = header.indexOf("Description")

        if (dateIdx < 0 || amountIdx < 0) {
            logger.warn("Missing Date or Amount columns in transaction data")
            return emptyList()
        }

        return rows.drop(1).mapNotNull { row ->
            try {
                val dateStr = row.getOrNull(dateIdx)?.toString() ?: return@mapNotNull null
                val amountStr = row.getOrNull(amountIdx)?.toString() ?: return@mapNotNull null
                val category = row.getOrNull(categoryIdx)?.toString() ?: ""
                val description = row.getOrNull(descIdx)?.toString() ?: ""

                if (dateStr.isBlank() || amountStr.isBlank()) return@mapNotNull null
                if (category in excludedCategories) return@mapNotNull null

                val date = LocalDate.parse(dateStr.trim(), dateFormatter)
                val amount = parseAmount(amountStr)

                if (amount == 0.0) return@mapNotNull null

                ParsedTransaction(date, abs(amount), category, description, isExpense = amount < 0)
            } catch (e: Exception) {
                null
            }
        }
    }

    private fun parseAmount(raw: String): Double {
        val cleaned = raw.replace("$", "").replace(",", "").trim()
        return cleaned.toDoubleOrNull() ?: 0.0
    }

    // ---- Weekly Report ----

    internal fun buildWeeklyReport(transactions: List<ParsedTransaction>): String {
        val today = LocalDate.now()
        val mtdStart = today.withDayOfMonth(1)
        val dayOfMonth = today.dayOfMonth

        val lastMonthSameStart = mtdStart.minusMonths(1)
        val lastMonthSameEnd = lastMonthSameStart.plusDays(dayOfMonth.toLong() - 1)
        val lastMonthStart = lastMonthSameStart
        val lastMonthEnd = YearMonth.from(lastMonthStart).atEndOfMonth()

        val mtdExpenses = transactions.filter { it.date in mtdStart..today && it.isExpense }
        val mtdIncome = transactions.filter { it.date in mtdStart..today && !it.isExpense }
        val lastMonthSameExpenses = transactions.filter { it.date in lastMonthSameStart..lastMonthSameEnd && it.isExpense }
        val lastMonthSameIncome = transactions.filter { it.date in lastMonthSameStart..lastMonthSameEnd && !it.isExpense }
        val lastMonthAllExpenses = transactions.filter { it.date in lastMonthStart..lastMonthEnd && it.isExpense }
        val lastMonthAllIncome = transactions.filter { it.date in lastMonthStart..lastMonthEnd && !it.isExpense }

        val twelveMonthsAgo = today.minusMonths(12).withDayOfMonth(1)
        val lastMonthEndDate = mtdStart.minusDays(1)
        val trailing12 = transactions.filter { it.date in twelveMonthsAgo..lastMonthEndDate }

        val mtdExp = mtdExpenses.sumOf { it.amount }
        val mtdInc = mtdIncome.sumOf { it.amount }
        val mtdNet = mtdInc - mtdExp
        val lastSameExp = lastMonthSameExpenses.sumOf { it.amount }
        val lastSameInc = lastMonthSameIncome.sumOf { it.amount }
        val lastSameNet = lastSameInc - lastSameExp
        val lastFinalExp = lastMonthAllExpenses.sumOf { it.amount }
        val lastFinalInc = lastMonthAllIncome.sumOf { it.amount }
        val lastFinalNet = lastFinalInc - lastFinalExp
        val avg12mExp = trailing12.filter { it.isExpense }.sumOf { it.amount } / 12.0
        val avg12mInc = trailing12.filter { !it.isExpense }.sumOf { it.amount } / 12.0
        val avg12mNet = avg12mInc - avg12mExp

        // For category breakdown, use expenses only
        val mtdTxns = mtdExpenses
        val lastMonthSameTxns = lastMonthSameExpenses
        val lastMonthAllTxns = lastMonthAllExpenses

        // Category breakdown
        val allCategories = (mtdTxns.map { it.category } +
                lastMonthSameTxns.map { it.category } +
                lastMonthAllTxns.map { it.category }).toSet()

        data class CatRow(
            val name: String,
            val mtd: Double,
            val lastSame: Double,
            val lastFinal: Double,
            val pctChange: Double?,
        )

        val catRows = allCategories.mapNotNull { cat ->
            val mtd = mtdTxns.filter { it.category == cat }.sumOf { it.amount }
            val lastSame = lastMonthSameTxns.filter { it.category == cat }.sumOf { it.amount }
            val lastFinal = lastMonthAllTxns.filter { it.category == cat }.sumOf { it.amount }

            if (mtd < 5.0 && lastSame < 5.0 && lastFinal < 5.0) return@mapNotNull null

            val pctChange = if (lastSame > 0) ((mtd - lastSame) / lastSame) * 100.0 else null
            CatRow(cat.ifBlank { "Uncategorized" }, mtd, lastSame, lastFinal, pctChange)
        }.sortedByDescending { it.mtd }.take(15)

        // Top 5 largest expenses
        val top5 = mtdTxns.sortedByDescending { it.amount }.take(5)

        val lastMonthName = lastMonthStart.month.name.lowercase().replaceFirstChar { it.uppercase() }
        val currentMonthName = today.month.name.lowercase().replaceFirstChar { it.uppercase() }

        return buildString {
            append(htmlHead("$currentMonthName Month-to-Date"))
            append("""
                <div style="background:#161b22;border-radius:8px;border:1px solid #2d333b;padding:20px;margin-bottom:16px;">
                    <h2 style="color:#f0f3f6;margin:0 0 16px 0;font-size:18px;">Summary</h2>
                    <table style="width:100%;border-collapse:collapse;">
                        <tr style="border-bottom:1px solid #2d333b;">
                            <td style="padding:8px 12px;color:#768390;"></td>
                            <td style="padding:8px 12px;color:#768390;text-align:right;font-size:12px;">MTD (Day $dayOfMonth)</td>
                            <td style="padding:8px 12px;color:#768390;text-align:right;font-size:12px;">$lastMonthName 1-$dayOfMonth</td>
                            <td style="padding:8px 12px;color:#768390;text-align:right;font-size:12px;">$lastMonthName Final</td>
                            <td style="padding:8px 12px;color:#768390;text-align:right;font-size:12px;">12M Avg</td>
                        </tr>
                        <tr>
                            <td style="padding:8px 12px;color:#768390;">Expenses</td>
                            <td style="padding:8px 12px;color:#f0f3f6;font-family:monospace;text-align:right;font-weight:bold;">${formatCurrency(mtdExp)}</td>
                            <td style="padding:8px 12px;font-family:monospace;text-align:right;">${formatCurrencyWithPct(lastSameExp, mtdExp)}</td>
                            <td style="padding:8px 12px;color:#768390;font-family:monospace;text-align:right;">${formatCurrency(lastFinalExp)}</td>
                            <td style="padding:8px 12px;color:#768390;font-family:monospace;text-align:right;">${formatCurrency(avg12mExp)}</td>
                        </tr>
                        <tr>
                            <td style="padding:8px 12px;color:#768390;">Income</td>
                            <td style="padding:8px 12px;color:#f0f3f6;font-family:monospace;text-align:right;font-weight:bold;">${formatCurrency(mtdInc)}</td>
                            <td style="padding:8px 12px;font-family:monospace;text-align:right;">${formatCurrencyWithPct(lastSameInc, mtdInc, incomeMode = true)}</td>
                            <td style="padding:8px 12px;color:#768390;font-family:monospace;text-align:right;">${formatCurrency(lastFinalInc)}</td>
                            <td style="padding:8px 12px;color:#768390;font-family:monospace;text-align:right;">${formatCurrency(avg12mInc)}</td>
                        </tr>
                        <tr style="border-top:1px solid #2d333b;">
                            <td style="padding:8px 12px;color:#768390;font-weight:bold;">Net</td>
                            <td style="padding:8px 12px;font-family:monospace;text-align:right;font-weight:bold;${netStyle(mtdNet)}">${formatSignedCurrency(mtdNet)}</td>
                            <td style="padding:8px 12px;font-family:monospace;text-align:right;${netStyle(lastSameNet)}">${formatSignedCurrency(lastSameNet)}</td>
                            <td style="padding:8px 12px;font-family:monospace;text-align:right;${netStyle(lastFinalNet)}">${formatSignedCurrency(lastFinalNet)}</td>
                            <td style="padding:8px 12px;font-family:monospace;text-align:right;${netStyle(avg12mNet)}">${formatSignedCurrency(avg12mNet)}</td>
                        </tr>
                    </table>
                </div>
            """.trimIndent())

            // Category table
            append("""
                <div style="background:#161b22;border-radius:8px;border:1px solid #2d333b;padding:20px;margin-bottom:16px;">
                    <h2 style="color:#f0f3f6;margin:0 0 16px 0;font-size:18px;">Category Breakdown</h2>
                    <table style="width:100%;border-collapse:collapse;">
                        <tr style="border-bottom:1px solid #2d333b;">
                            <th style="padding:8px 12px;color:#768390;text-align:left;font-weight:normal;font-size:12px;">Category</th>
                            <th style="padding:8px 12px;color:#768390;text-align:right;font-weight:normal;font-size:12px;">MTD</th>
                            <th style="padding:8px 12px;color:#768390;text-align:right;font-weight:normal;font-size:12px;">$lastMonthName (1-$dayOfMonth)</th>
                            <th style="padding:8px 12px;color:#768390;text-align:right;font-weight:normal;font-size:12px;">$lastMonthName Final</th>
                            <th style="padding:8px 12px;color:#768390;text-align:right;font-weight:normal;font-size:12px;">% Change</th>
                        </tr>
            """.trimIndent())

            for (cat in catRows) {
                val changeStr = if (cat.pctChange != null) {
                    val color = when {
                        cat.pctChange > 10 -> "#f85149"
                        cat.pctChange > 0 -> "#d29a28"
                        else -> "#3fb950"
                    }
                    """<span style="color:$color;">${"%+.0f".format(cat.pctChange)}%</span>"""
                } else {
                    """<span style="color:#768390;">--</span>"""
                }

                append("""
                    <tr style="border-bottom:1px solid #2d333b;">
                        <td style="padding:8px 12px;color:#e1e4e8;">${escapeHtml(cat.name)}</td>
                        <td style="padding:8px 12px;color:#f0f3f6;font-family:monospace;text-align:right;">${formatCurrency(cat.mtd)}</td>
                        <td style="padding:8px 12px;color:#e1e4e8;font-family:monospace;text-align:right;">${formatCurrency(cat.lastSame)}</td>
                        <td style="padding:8px 12px;color:#e1e4e8;font-family:monospace;text-align:right;">${formatCurrency(cat.lastFinal)}</td>
                        <td style="padding:8px 12px;font-family:monospace;text-align:right;">$changeStr</td>
                    </tr>
                """.trimIndent())
            }

            append("""
                    </table>
                </div>
            """.trimIndent())

            // Top 5 expenses
            if (top5.isNotEmpty()) {
                append("""
                    <div style="background:#161b22;border-radius:8px;border:1px solid #2d333b;padding:20px;margin-bottom:16px;">
                        <h2 style="color:#f0f3f6;margin:0 0 16px 0;font-size:18px;">Top 5 Expenses</h2>
                        <table style="width:100%;border-collapse:collapse;">
                """.trimIndent())

                for (txn in top5) {
                    append("""
                        <tr style="border-bottom:1px solid #2d333b;">
                            <td style="padding:8px 12px;color:#e1e4e8;">${escapeHtml(txn.description.take(40))}</td>
                            <td style="padding:8px 12px;color:#768390;">${escapeHtml(txn.category)}</td>
                            <td style="padding:8px 12px;color:#f0f3f6;font-family:monospace;text-align:right;">${formatCurrency(txn.amount)}</td>
                        </tr>
                    """.trimIndent())
                }

                append("""
                        </table>
                    </div>
                """.trimIndent())
            }

            append(htmlFoot())
        }
    }

    // ---- Monthly Report ----

    internal fun buildMonthlyReport(transactions: List<ParsedTransaction>): String {
        val lastMonth = YearMonth.now().minusMonths(1)
        val lastMonthStart = lastMonth.atDay(1)
        val lastMonthEnd = lastMonth.atEndOfMonth()
        val prevMonth = lastMonth.minusMonths(1)
        val prevMonthStart = prevMonth.atDay(1)
        val prevMonthEnd = prevMonth.atEndOfMonth()

        val lastMonthAll = transactions.filter { it.date in lastMonthStart..lastMonthEnd }
        val prevMonthAll = transactions.filter { it.date in prevMonthStart..prevMonthEnd }

        val twelveMonthsAgo = lastMonthStart.minusMonths(12)
        val trailing12End = prevMonthEnd
        val trailing12All = transactions.filter { it.date in twelveMonthsAgo..trailing12End }

        val thisExp = lastMonthAll.filter { it.isExpense }.sumOf { it.amount }
        val thisInc = lastMonthAll.filter { !it.isExpense }.sumOf { it.amount }
        val thisNet = thisInc - thisExp
        val prevExp = prevMonthAll.filter { it.isExpense }.sumOf { it.amount }
        val prevInc = prevMonthAll.filter { !it.isExpense }.sumOf { it.amount }
        val prevNet = prevInc - prevExp
        val avg12mExp = trailing12All.filter { it.isExpense }.sumOf { it.amount } / 12.0
        val avg12mInc = trailing12All.filter { !it.isExpense }.sumOf { it.amount } / 12.0
        val avg12mNet = avg12mInc - avg12mExp

        // For category breakdown, expenses only
        val lastMonthTxns = lastMonthAll.filter { it.isExpense }
        val prevMonthTxns = prevMonthAll.filter { it.isExpense }
        val trailing12Txns = trailing12All.filter { it.isExpense }

        val pctVsAvg = if (avg12mExp > 0) ((thisExp - avg12mExp) / avg12mExp) * 100.0 else 0.0

        // 6-month trend
        data class MonthTotal(val label: String, val total: Double)

        val trends = (5 downTo 0).map { i ->
            val ym = lastMonth.minusMonths(i.toLong())
            val start = ym.atDay(1)
            val end = ym.atEndOfMonth()
            val total = transactions.filter { it.date in start..end && it.isExpense }.sumOf { it.amount }
            MonthTotal(ym.format(DateTimeFormatter.ofPattern("MMM")), total)
        }
        val maxTrend = trends.maxOfOrNull { it.total } ?: 1.0

        // Category breakdown
        val allCategories = (lastMonthTxns.map { it.category } +
                prevMonthTxns.map { it.category }).toSet()

        data class CatRow(
            val name: String,
            val thisMonth: Double,
            val prevMonth: Double,
            val avg12m: Double,
            val pctVsAvg: Double?,
            val proportion: Double,
        )

        val catRows = allCategories.mapNotNull { cat ->
            val thisMo = lastMonthTxns.filter { it.category == cat }.sumOf { it.amount }
            val prevMo = prevMonthTxns.filter { it.category == cat }.sumOf { it.amount }
            val catTrailing = trailing12Txns.filter { it.category == cat }.sumOf { it.amount }
            val catAvg = if (catTrailing > 0) catTrailing / 12.0 else 0.0

            if (thisMo < 5.0 && prevMo < 5.0 && catAvg < 5.0) return@mapNotNull null

            val pct = if (catAvg > 0) ((thisMo - catAvg) / catAvg) * 100.0 else null
            val proportion = if (thisExp > 0) (thisMo / thisExp) * 100.0 else 0.0
            CatRow(cat.ifBlank { "Uncategorized" }, thisMo, prevMo, catAvg, pct, proportion)
        }.sortedByDescending { it.thisMonth }.take(18)

        // Top 10 expenses
        val top10 = lastMonthTxns.sortedByDescending { it.amount }.take(10)

        val lastMonthName = lastMonth.format(DateTimeFormatter.ofPattern("MMMM"))
        val prevMonthName = prevMonth.format(DateTimeFormatter.ofPattern("MMMM"))

        return buildString {
            append(htmlHead("Monthly Wrap-up: $lastMonthName"))
            append("""
                <div style="background:#161b22;border-radius:8px;border:1px solid #2d333b;padding:20px;margin-bottom:16px;">
                    <h2 style="color:#f0f3f6;margin:0 0 16px 0;font-size:18px;">Summary</h2>
                    <table style="width:100%;border-collapse:collapse;">
                        <tr style="border-bottom:1px solid #2d333b;">
                            <td style="padding:8px 12px;color:#768390;"></td>
                            <td style="padding:8px 12px;color:#768390;text-align:right;font-size:12px;">$lastMonthName</td>
                            <td style="padding:8px 12px;color:#768390;text-align:right;font-size:12px;">$prevMonthName</td>
                            <td style="padding:8px 12px;color:#768390;text-align:right;font-size:12px;">12M Avg</td>
                        </tr>
                        <tr>
                            <td style="padding:8px 12px;color:#768390;">Expenses</td>
                            <td style="padding:8px 12px;color:#f0f3f6;font-family:monospace;text-align:right;font-weight:bold;">${formatCurrency(thisExp)}</td>
                            <td style="padding:8px 12px;font-family:monospace;text-align:right;">${formatCurrencyWithPct(prevExp, thisExp)}</td>
                            <td style="padding:8px 12px;font-family:monospace;text-align:right;">${formatCurrencyWithPct(avg12mExp, thisExp)}</td>
                        </tr>
                        <tr>
                            <td style="padding:8px 12px;color:#768390;">Income</td>
                            <td style="padding:8px 12px;color:#f0f3f6;font-family:monospace;text-align:right;font-weight:bold;">${formatCurrency(thisInc)}</td>
                            <td style="padding:8px 12px;font-family:monospace;text-align:right;">${formatCurrencyWithPct(prevInc, thisInc, incomeMode = true)}</td>
                            <td style="padding:8px 12px;font-family:monospace;text-align:right;">${formatCurrencyWithPct(avg12mInc, thisInc, incomeMode = true)}</td>
                        </tr>
                        <tr style="border-top:1px solid #2d333b;">
                            <td style="padding:8px 12px;color:#768390;font-weight:bold;">Net</td>
                            <td style="padding:8px 12px;font-family:monospace;text-align:right;font-weight:bold;${netStyle(thisNet)}">${formatSignedCurrency(thisNet)}</td>
                            <td style="padding:8px 12px;font-family:monospace;text-align:right;${netStyle(prevNet)}">${formatSignedCurrency(prevNet)}</td>
                            <td style="padding:8px 12px;font-family:monospace;text-align:right;${netStyle(avg12mNet)}">${formatSignedCurrency(avg12mNet)}</td>
                        </tr>
                    </table>
                </div>
            """.trimIndent())

            // 6-month trend
            append("""
                <div style="background:#161b22;border-radius:8px;border:1px solid #2d333b;padding:20px;margin-bottom:16px;">
                    <h2 style="color:#f0f3f6;margin:0 0 16px 0;font-size:18px;">6-Month Trend</h2>
                    <table style="width:100%;border-collapse:collapse;">
            """.trimIndent())

            for (t in trends) {
                val barWidth = if (maxTrend > 0) ((t.total / maxTrend) * 100).toInt().coerceIn(0, 100) else 0
                append("""
                    <tr>
                        <td style="padding:6px 12px;color:#768390;width:50px;">${t.label}</td>
                        <td style="padding:6px 12px;">
                            <div style="background:#388bfd;height:16px;border-radius:3px;width:${barWidth}%;"></div>
                        </td>
                        <td style="padding:6px 12px;color:#f0f3f6;font-family:monospace;text-align:right;width:100px;">${formatCurrency(t.total)}</td>
                    </tr>
                """.trimIndent())
            }

            append("""
                    </table>
                </div>
            """.trimIndent())

            // Category breakdown
            append("""
                <div style="background:#161b22;border-radius:8px;border:1px solid #2d333b;padding:20px;margin-bottom:16px;">
                    <h2 style="color:#f0f3f6;margin:0 0 16px 0;font-size:18px;">Category Breakdown</h2>
                    <table style="width:100%;border-collapse:collapse;">
                        <tr style="border-bottom:1px solid #2d333b;">
                            <th style="padding:8px 12px;color:#768390;text-align:left;font-weight:normal;font-size:12px;">Category</th>
                            <th style="padding:8px 12px;color:#768390;text-align:right;font-weight:normal;font-size:12px;">$lastMonthName</th>
                            <th style="padding:8px 12px;color:#768390;text-align:right;font-weight:normal;font-size:12px;">$prevMonthName</th>
                            <th style="padding:8px 12px;color:#768390;text-align:right;font-weight:normal;font-size:12px;">12M Avg</th>
                            <th style="padding:8px 12px;color:#768390;text-align:right;font-weight:normal;font-size:12px;">vs Avg</th>
                            <th style="padding:8px 12px;color:#768390;text-align:left;font-weight:normal;font-size:12px;width:80px;"></th>
                        </tr>
            """.trimIndent())

            for (cat in catRows) {
                val changeStr = if (cat.pctVsAvg != null) {
                    val color = when {
                        cat.pctVsAvg > 10 -> "#f85149"
                        cat.pctVsAvg > 0 -> "#d29a28"
                        else -> "#3fb950"
                    }
                    """<span style="color:$color;">${"%+.0f".format(cat.pctVsAvg)}%</span>"""
                } else {
                    """<span style="color:#768390;">--</span>"""
                }
                val barWidth = cat.proportion.toInt().coerceIn(0, 100)

                append("""
                    <tr style="border-bottom:1px solid #2d333b;">
                        <td style="padding:8px 12px;color:#e1e4e8;">${escapeHtml(cat.name)}</td>
                        <td style="padding:8px 12px;color:#f0f3f6;font-family:monospace;text-align:right;">${formatCurrency(cat.thisMonth)}</td>
                        <td style="padding:8px 12px;color:#e1e4e8;font-family:monospace;text-align:right;">${formatCurrency(cat.prevMonth)}</td>
                        <td style="padding:8px 12px;color:#e1e4e8;font-family:monospace;text-align:right;">${formatCurrency(cat.avg12m)}</td>
                        <td style="padding:8px 12px;font-family:monospace;text-align:right;">$changeStr</td>
                        <td style="padding:8px 12px;">
                            <div style="background:#388bfd;height:10px;border-radius:2px;width:${barWidth}%;"></div>
                        </td>
                    </tr>
                """.trimIndent())
            }

            append("""
                    </table>
                </div>
            """.trimIndent())

            // Top 10 expenses
            if (top10.isNotEmpty()) {
                append("""
                    <div style="background:#161b22;border-radius:8px;border:1px solid #2d333b;padding:20px;margin-bottom:16px;">
                        <h2 style="color:#f0f3f6;margin:0 0 16px 0;font-size:18px;">Top 10 Expenses</h2>
                        <table style="width:100%;border-collapse:collapse;">
                """.trimIndent())

                for ((i, txn) in top10.withIndex()) {
                    append("""
                        <tr style="border-bottom:1px solid #2d333b;">
                            <td style="padding:8px 12px;color:#768390;font-family:monospace;width:24px;">${i + 1}.</td>
                            <td style="padding:8px 12px;color:#e1e4e8;">${escapeHtml(txn.description.take(40))}</td>
                            <td style="padding:8px 12px;color:#768390;">${escapeHtml(txn.category)}</td>
                            <td style="padding:8px 12px;color:#f0f3f6;font-family:monospace;text-align:right;">${formatCurrency(txn.amount)}</td>
                        </tr>
                    """.trimIndent())
                }

                append("""
                        </table>
                    </div>
                """.trimIndent())
            }

            append(htmlFoot())
        }
    }

    // ---- Email ----

    private fun sendEmail(subject: String, htmlBody: String) {
        val addresses = config.reportToAddresses
            .split(",")
            .map { it.trim() }
            .filter { it.isNotBlank() }

        if (addresses.isEmpty()) {
            logger.warn("No report recipients configured (REPORT_TO_ADDRESSES), skipping email")
            return
        }

        val ses = SesClient.builder()
            .region(Region.of(config.awsRegion))
            .credentialsProvider(DefaultCredentialsProvider.create())
            .build()

        ses.use { client ->
            client.sendEmail(
                SendEmailRequest.builder()
                    .source(config.sesFromAddress)
                    .destination(Destination.builder().toAddresses(addresses).build())
                    .message(
                        Message.builder()
                            .subject(Content.builder().data(subject).charset("UTF-8").build())
                            .body(
                                Body.builder()
                                    .html(Content.builder().data(htmlBody).charset("UTF-8").build())
                                    .build()
                            )
                            .build()
                    )
                    .build()
            )
        }
        logger.info("Sent report email to {}", addresses)
    }

    // ---- HTML Helpers ----

    private fun htmlHead(title: String): String = """
        <!DOCTYPE html>
        <html>
        <head><meta charset="UTF-8"></head>
        <body style="margin:0;padding:0;background-color:#0f1117;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Helvetica,Arial,sans-serif;color:#f0f3f6;">
        <div style="max-width:600px;margin:0 auto;padding:20px;">
            <h1 style="color:#f0f3f6;font-size:22px;margin:0 0 20px 0;">$title</h1>
    """.trimIndent()

    private fun htmlFoot(): String = """
            <div style="text-align:center;padding:16px;color:#768390;font-size:12px;">
                Generated by Bookkeeper Agent
            </div>
        </div>
        </body>
        </html>
    """.trimIndent()

    private fun formatCurrency(amount: Double): String {
        return "$%,.0f".format(amount)
    }

    /**
     * Formats a comparison value with an inline percentage vs the current value.
     * For expenses: spending up = red, down = green.
     * For income: income up = green, down = red.
     */
    private fun formatCurrencyWithPct(comparisonVal: Double, currentVal: Double, incomeMode: Boolean = false): String {
        if (comparisonVal == 0.0 && currentVal == 0.0) return """<span style="color:#768390;">—</span>"""
        val pct = if (comparisonVal > 0) ((currentVal - comparisonVal) / comparisonVal) * 100.0 else null
        val pctHtml = if (pct != null) {
            val color = if (incomeMode) {
                // Income: more is green, less is red
                if (pct >= 0) "#3fb950" else "#f85149"
            } else {
                // Expenses: more is red, less is green
                when {
                    pct > 10 -> "#f85149"
                    pct > 0 -> "#d29a28"
                    else -> "#3fb950"
                }
            }
            """ <span style="color:$color;font-size:11px;">${"%+.0f".format(pct)}%</span>"""
        } else ""
        return """<span style="color:#e1e4e8;">${formatCurrency(comparisonVal)}</span>$pctHtml"""
    }

    private fun formatSignedCurrency(amount: Double): String {
        val sign = if (amount >= 0) "+" else "-"
        return "$sign$${"%,.0f".format(abs(amount))}"
    }

    private fun netStyle(amount: Double): String {
        return if (amount >= 0) "color:#3fb950;" else "color:#f85149;"
    }

    private fun formatPctChange(current: Double, previous: Double): String {
        if (previous == 0.0) return if (current > 0) "NEW" else "--"
        val pct = ((current - previous) / previous) * 100.0
        return "%+.1f%%".format(pct)
    }

    private fun pctStyle(current: Double, previous: Double): String {
        if (previous == 0.0) return "color:#768390;"
        val pct = ((current - previous) / previous) * 100.0
        return spendingPctStyle(pct)
    }

    private fun spendingPctStyle(pct: Double): String {
        val color = when {
            pct > 10 -> "#f85149"
            pct > 0 -> "#d29a28"
            else -> "#3fb950"
        }
        return "color:$color;"
    }

    private fun escapeHtml(text: String): String {
        return text
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace("\"", "&quot;")
    }
}
