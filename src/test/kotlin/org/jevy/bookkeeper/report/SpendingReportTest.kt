package org.jevy.bookkeeper.report

import org.jevy.bookkeeper.config.AppConfig
import org.junit.jupiter.api.Test
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class SpendingReportTest {

    private val config = AppConfig(
        kafkaBootstrapServers = "localhost:9092",
        schemaRegistryUrl = "http://localhost:8081",
        googleSheetId = "test",
        googleCredentialsJson = "{}",
        openrouterApiKey = "",
        maxTransactionAgeDays = 365,
        maxTransactions = 0,
        additionalContextPrompt = null,
        model = "anthropic/claude-sonnet-4-6",
    )

    private val weeklyReport = SpendingReport(config, "weekly")
    private val monthlyReport = SpendingReport(config, "monthly")

    private val categoryHeader = listOf<Any>("Category", "Group", "Type", "Hide From Reports")

    private val transactionHeader = listOf<Any>(
        "Date", "Description", "Category", "Amount", "Account",
        "Account #", "Institution", "Month", "Week", "Transaction ID",
        "Check Number", "Full Description", "Note", "Receipt", "Source",
        "Categorized Date", "Date Added", "Metadata", "Categorized", "Tags", "Migration Notes",
    )

    private fun makeRow(
        date: String,
        description: String,
        category: String,
        amount: String,
    ): List<Any> = listOf(
        date, description, category, amount, "Visa",
        "xxxx1234", "TD", "", "", "txn-${description.hashCode()}",
        "", description, "", "", "Yodlee",
        "", "", "", "", "", "",
    )

    private fun makeCategoryRow(
        name: String,
        group: String = "Living",
        type: String = "Expense",
        hide: String = "",
    ): List<Any> = listOf(name, group, type, hide)

    // --- buildExcludedCategories tests ---

    @Test
    fun `excludes Transfer type categories`() {
        val rows = listOf(
            categoryHeader,
            makeCategoryRow("Groceries", type = "Expense"),
            makeCategoryRow("Transfer", type = "Transfer"),
            makeCategoryRow("Income Taxes", hide = "Hide"),
        )
        val excluded = weeklyReport.buildExcludedCategories(rows)
        assertTrue("Transfer" in excluded)
        assertTrue("Income Taxes" in excluded)
        assertTrue("Groceries" !in excluded)
    }

    @Test
    fun `always excludes one-time project categories`() {
        val rows = listOf(
            categoryHeader,
            makeCategoryRow("Shed", type = "Expense"),
            makeCategoryRow("Renovations", type = "Expense"),
        )
        val excluded = weeklyReport.buildExcludedCategories(rows)
        assertTrue("Shed" in excluded)
        assertTrue("Renovations" in excluded)
    }

    @Test
    fun `handles empty category rows`() {
        val excluded = weeklyReport.buildExcludedCategories(listOf(categoryHeader))
        // Should still have project categories
        assertTrue("Shed" in excluded)
        assertTrue("Renovations" in excluded)
    }

    // --- parseTransactions tests ---

    @Test
    fun `parses expense transactions correctly`() {
        val rows = listOf(
            transactionHeader,
            makeRow("3/15/2026", "COSTCO", "Groceries", "-\$384.91"),
        )
        val txns = weeklyReport.parseTransactions(rows, emptySet())
        assertEquals(1, txns.size)
        assertEquals("COSTCO", txns[0].description)
        assertEquals("Groceries", txns[0].category)
        assertEquals(384.91, txns[0].amount, 0.01)
        assertEquals(true, txns[0].isExpense)
    }

    @Test
    fun `parses positive amounts as income`() {
        val rows = listOf(
            transactionHeader,
            makeRow("3/15/2026", "PAYCHECK", "Income", "\$5000.00"),
        )
        val txns = weeklyReport.parseTransactions(rows, emptySet())
        assertEquals(1, txns.size)
        assertEquals(false, txns[0].isExpense)
        assertEquals(5000.0, txns[0].amount, 0.01)
    }

    @Test
    fun `excludes specified categories`() {
        val rows = listOf(
            transactionHeader,
            makeRow("3/15/2026", "COSTCO", "Groceries", "-\$50.00"),
            makeRow("3/15/2026", "TD ATM", "Transfer", "-\$500.00"),
        )
        val txns = weeklyReport.parseTransactions(rows, setOf("Transfer"))
        assertEquals(1, txns.size)
        assertEquals("Groceries", txns[0].category)
    }

    @Test
    fun `handles amounts with commas`() {
        val rows = listOf(
            transactionHeader,
            makeRow("3/15/2026", "BIG PURCHASE", "Shopping", "-\$1,234.56"),
        )
        val txns = weeklyReport.parseTransactions(rows, emptySet())
        assertEquals(1, txns.size)
        assertEquals(1234.56, txns[0].amount, 0.01)
    }

    @Test
    fun `skips rows with blank dates`() {
        val rows = listOf(
            transactionHeader,
            makeRow("", "COSTCO", "Groceries", "-\$50.00"),
        )
        val txns = weeklyReport.parseTransactions(rows, emptySet())
        assertEquals(0, txns.size)
    }

    @Test
    fun `skips rows with unparseable dates`() {
        val rows = listOf(
            transactionHeader,
            makeRow("not-a-date", "COSTCO", "Groceries", "-\$50.00"),
        )
        val txns = weeklyReport.parseTransactions(rows, emptySet())
        assertEquals(0, txns.size)
    }

    @Test
    fun `skips rows with blank amounts`() {
        val rows = listOf(
            transactionHeader,
            makeRow("3/15/2026", "COSTCO", "Groceries", ""),
        )
        val txns = weeklyReport.parseTransactions(rows, emptySet())
        assertEquals(0, txns.size)
    }

    // --- buildWeeklyReport tests ---

    @Test
    fun `weekly report contains MTD header`() {
        val today = LocalDate.now()
        val txns = listOf(
            SpendingReport.ParsedTransaction(today, 100.0, "Groceries", "COSTCO", isExpense = true),
        )
        val html = weeklyReport.buildWeeklyReport(txns)
        val monthName = today.month.name.lowercase().replaceFirstChar { it.uppercase() }
        assertTrue(html.contains(monthName))
        assertTrue(html.contains("Month-to-Date"))
    }

    @Test
    fun `weekly report shows category breakdown`() {
        val today = LocalDate.now()
        val txns = listOf(
            SpendingReport.ParsedTransaction(today, 100.0, "Groceries", "COSTCO", isExpense = true),
            SpendingReport.ParsedTransaction(today, 50.0, "Restaurants", "TIM HORTONS", isExpense = true),
        )
        val html = weeklyReport.buildWeeklyReport(txns)
        assertTrue(html.contains("Groceries"))
        assertTrue(html.contains("Restaurants"))
        assertTrue(html.contains("Category Breakdown"))
    }

    @Test
    fun `weekly report compares to same period last month`() {
        val today = LocalDate.now()
        val lastMonthSameDay = today.minusMonths(1)
        val lastMonthName = lastMonthSameDay.month.name.lowercase().replaceFirstChar { it.uppercase() }

        val txns = listOf(
            SpendingReport.ParsedTransaction(today, 200.0, "Groceries", "COSTCO", isExpense = true),
            SpendingReport.ParsedTransaction(lastMonthSameDay, 150.0, "Groceries", "METRO", isExpense = true),
        )
        val html = weeklyReport.buildWeeklyReport(txns)
        // Should reference last month by name
        assertTrue(html.contains(lastMonthName))
    }

    @Test
    fun `weekly report includes top expenses`() {
        val today = LocalDate.now()
        val txns = listOf(
            SpendingReport.ParsedTransaction(today, 500.0, "Travel", "AIR CANADA", isExpense = true),
            SpendingReport.ParsedTransaction(today, 100.0, "Groceries", "COSTCO", isExpense = true),
        )
        val html = weeklyReport.buildWeeklyReport(txns)
        assertTrue(html.contains("Top 5 Expenses"))
        assertTrue(html.contains("AIR CANADA"))
    }

    @Test
    fun `weekly report handles empty transaction list`() {
        val html = weeklyReport.buildWeeklyReport(emptyList())
        // Should still generate valid HTML without crashing
        assertTrue(html.contains("Month-to-Date"))
        assertTrue(html.contains("</html>"))
    }

    // --- buildMonthlyReport tests ---

    @Test
    fun `monthly report covers last complete month`() {
        val lastMonth = LocalDate.now().minusMonths(1)
        val lastMonthName = lastMonth.month.name.lowercase().replaceFirstChar { it.uppercase() }

        val txns = listOf(
            SpendingReport.ParsedTransaction(lastMonth.withDayOfMonth(15), 300.0, "Groceries", "COSTCO", isExpense = true),
        )
        val html = monthlyReport.buildMonthlyReport(txns)
        assertTrue(html.contains(lastMonthName))
        assertTrue(html.contains("Monthly Wrap-up"))
    }

    @Test
    fun `monthly report shows 6-month trend`() {
        val txns = (1L..6L).map { i ->
            val date = LocalDate.now().minusMonths(i).withDayOfMonth(15)
            SpendingReport.ParsedTransaction(date, 1000.0 * i, "Groceries", "COSTCO", isExpense = true)
        }
        val html = monthlyReport.buildMonthlyReport(txns)
        assertTrue(html.contains("6-Month Trend"))
    }

    @Test
    fun `monthly report shows top 10 expenses`() {
        val lastMonth = LocalDate.now().minusMonths(1)
        val txns = (1..12).map { i ->
            SpendingReport.ParsedTransaction(
                isExpense = true,
                date = lastMonth.withDayOfMonth(i.coerceAtMost(28)),
                amount = i * 100.0,
                category = "Cat$i",
                description = "Desc$i",
            )
        }
        val html = monthlyReport.buildMonthlyReport(txns)
        assertTrue(html.contains("Top 10 Expenses"))
        assertTrue(html.contains("Desc12")) // Highest amount
    }

    @Test
    fun `monthly report compares to previous month and 12M average`() {
        val lastMonth = LocalDate.now().minusMonths(1).withDayOfMonth(15)
        val prevMonth = LocalDate.now().minusMonths(2).withDayOfMonth(15)
        val prevMonthName = prevMonth.month.name.lowercase().replaceFirstChar { it.uppercase() }

        val txns = listOf(
            SpendingReport.ParsedTransaction(lastMonth, 500.0, "Groceries", "COSTCO", isExpense = true),
            SpendingReport.ParsedTransaction(prevMonth, 400.0, "Groceries", "METRO", isExpense = true),
        )
        val html = monthlyReport.buildMonthlyReport(txns)
        assertTrue(html.contains("12-Month Average"))
        assertTrue(html.contains(prevMonthName))
    }

    @Test
    fun `monthly report handles empty transaction list`() {
        val html = monthlyReport.buildMonthlyReport(emptyList())
        assertTrue(html.contains("Monthly Wrap-up"))
        assertTrue(html.contains("</html>"))
    }

    // --- Edge cases ---

    @Test
    fun `handles transactions with zero amount`() {
        val rows = listOf(
            transactionHeader,
            makeRow("3/15/2026", "REFUND", "Groceries", "\$0.00"),
        )
        val txns = weeklyReport.parseTransactions(rows, emptySet())
        assertEquals(0, txns.size) // Zero is not negative, so filtered out
    }

    @Test
    fun `handles category rows with missing columns`() {
        val rows = listOf(
            categoryHeader,
            listOf<Any>("Groceries"), // Short row - missing group, type, hide
        )
        val excluded = weeklyReport.buildExcludedCategories(rows)
        // Should not crash, Groceries is not excluded
        assertTrue("Groceries" !in excluded)
    }

    @Test
    fun `handles transaction rows shorter than header`() {
        val rows = listOf(
            transactionHeader,
            listOf<Any>("3/15/2026", "COSTCO"), // Short row - missing most columns
        )
        // Should not crash
        val txns = weeklyReport.parseTransactions(rows, emptySet())
        // May or may not parse depending on amount being missing
        assertTrue(txns.isEmpty())
    }
}
