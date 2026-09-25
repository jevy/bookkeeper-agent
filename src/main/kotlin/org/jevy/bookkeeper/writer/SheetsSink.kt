package org.jevy.bookkeeper.writer

import org.jevy.bookkeeper.DurableTransactionId
import org.jevy.bookkeeper.config.AppConfig
import org.jevy.bookkeeper.kafka.TopicNames
import org.jevy.bookkeeper.sheets.SheetTransaction
import org.jevy.bookkeeper.sheets.SheetsClient
import org.jevy.bookkeeper.sheets.TransactionMapper
import org.jevy.bookkeeper_agent.Transaction
import org.slf4j.LoggerFactory
import java.time.LocalDate
import java.time.format.DateTimeFormatter

class RowNotFoundException(message: String) : RuntimeException(message)

/** Writes categories back to the Google Sheet. The Sheet is the source of truth. */
class SheetsSink(
    private val config: AppConfig,
    private val sheetsClient: SheetsClient = SheetsClient(config),
) : CategorySink {

    private val logger = LoggerFactory.getLogger(SheetsSink::class.java)

    override val name = "writer"
    override val consumerGroup = "category-writer"
    override val sourceTopic = TopicNames.CATEGORIZED
    override val dlqTopic = TopicNames.WRITE_FAILED

    // Resolve column letters from header row on first use
    private val columnLetters: Map<String, String> by lazy {
        val header = sheetsClient.readAllRows("Transactions!1:1").firstOrNull()?.map { it.toString() } ?: emptyList()
        header.withIndex().associate { (i, name) -> name to indexToColumnLetter(i) }.also {
            logger.info("Resolved column letters: Category={}, Transaction ID={}, Categorized Date={}",
                it["Category"], it["Transaction ID"], it["Categorized Date"])
        }
    }

    private fun indexToColumnLetter(index: Int): String {
        var result = ""
        var i = index
        while (i >= 0) {
            result = ('A' + i % 26) + result
            i = i / 26 - 1
        }
        return result
    }

    override fun write(tx: Transaction): SinkResult = try {
        writeCategory(tx)
    } catch (e: RowNotFoundException) {
        SinkResult.Rejected("row_not_found", e)
    } catch (e: Exception) {
        SinkResult.Rejected("exception", e)
    }

    /** Throws [RowNotFoundException] when the transaction has no row in the Sheet. */
    internal fun writeCategory(transaction: Transaction): SinkResult {
        val category = transaction.getCategory()?.toString()
        if (category.isNullOrBlank()) {
            logger.warn("Transaction {} has no category, skipping", transaction.getTransactionId())
            return SinkResult.Skipped("no_category")
        }

        val transactionId = transaction.getTransactionId().toString()

        val allRows = sheetsClient.readAllRows()
        if (allRows.isEmpty()) throw RowNotFoundException("No rows found for transaction $transactionId")

        val header = allRows.first().map { it.toString() }
        val colIndex = header.withIndex().associate { (i, name) -> name to i }
        val indexed = allRows.drop(1).mapIndexed { i, row ->
            SheetTransaction(i + 2, TransactionMapper.fromSheetRow(row, colIndex, config.googleSheetId))
        }

        val rowNumber = findRow(transaction, indexed)
            ?: throw RowNotFoundException("Could not find row for transaction $transactionId")

        val categoryCol = columnLetters["Category"] ?: "C"
        val categorizedDateCol = columnLetters["Categorized Date"] ?: "P"

        // Check if already categorized. Skip only if the existing category matches.
        val rows = sheetsClient.readAllRows("Transactions!${categoryCol}$rowNumber:${categoryCol}$rowNumber")
        val existing = rows.firstOrNull()?.firstOrNull()?.toString() ?: ""
        if (existing.isNotBlank() && existing == category) {
            logger.info("Transaction {} already has category '{}', skipping", transactionId, existing)
            // landed = true: the Sheet holds this category, so it still flows on to transactions.written
            return SinkResult.Skipped("already_categorized", landed = true)
        }

        if (existing.isNotBlank() && existing != category) {
            logger.info("Transaction {} category changing from '{}' to '{}'", transactionId, existing, category)
        }

        sheetsClient.writeCell("Transactions!${categoryCol}$rowNumber", category)
        val categorizedDate = transaction.getCategorizationDate()?.toString()
            ?: LocalDate.now().format(DateTimeFormatter.ofPattern("M/d/yyyy"))
        sheetsClient.writeCell("Transactions!${categorizedDateCol}$rowNumber", categorizedDate)

        val note = transaction.getNote()?.toString()
        if (!note.isNullOrBlank()) {
            val noteCol = columnLetters["Note"] ?: "N"
            sheetsClient.writeCell("Transactions!${noteCol}$rowNumber", note)
            logger.info("Wrote note to row {} for transaction {}", rowNumber, transactionId)
        }

        logger.info("Wrote category '{}' to row {} for transaction {}", category, rowNumber, transactionId)
        return SinkResult.Written
    }

    internal fun findRow(target: Transaction, rows: List<SheetTransaction>): Int? {
        val targetId = target.getTransactionId().toString()
        val isDurable = targetId.startsWith("durable-")

        // Tier 1: match by Transaction ID attribute
        if (!isDurable) {
            rows.find { it.transaction.getTransactionId().toString() == targetId }
                ?.let { return it.rowNumber }
        }

        // Tier 2: match by content-based durable ID
        val expectedId = DurableTransactionId.generate(target)
        rows.find { DurableTransactionId.generate(it.transaction) == expectedId }
            ?.let { return it.rowNumber }

        return null
    }
}
