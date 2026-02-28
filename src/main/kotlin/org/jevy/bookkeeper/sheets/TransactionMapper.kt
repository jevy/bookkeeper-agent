package org.jevy.bookkeeper.sheets

import org.jevy.bookkeeper.producer.DurableTransactionId
import org.jevy.bookkeeper_agent.Transaction

data class SheetTransaction(
    val rowNumber: Int,
    val transaction: Transaction,
)

object TransactionMapper {
    fun fromSheetRow(row: List<Any>, colIndex: Map<String, Int>, owner: String? = null): Transaction {
        fun col(name: String): String? = colIndex[name]?.let { row.getOrNull(it)?.toString() }

        val description = col("Description") ?: ""
        val transactionId = col("Transaction ID")?.takeIf { it.isNotBlank() }
            ?: DurableTransactionId.generate(
                owner = owner ?: "",
                date = col("Date") ?: "",
                description = description,
                amount = col("Amount") ?: "",
                account = col("Account") ?: "",
            )

        return Transaction.newBuilder()
            .setTransactionId(transactionId)
            .setDate(col("Date") ?: "")
            .setDescription(description)
            .setCategory(col("Category"))
            .setAmount(col("Amount") ?: "")
            .setAccount(col("Account") ?: "")
            .setAccountNumber(col("Account #"))
            .setInstitution(col("Institution"))
            .setMonth(col("Month"))
            .setWeek(col("Week"))
            .setCheckNumber(col("Check Number"))
            .setFullDescription(col("Full Description"))
            .setNote(col("Note"))
            .setSource(col("Source"))
            .setDateAdded(col("Date Added"))
            .setOwner(owner)
            .build()
    }
}
