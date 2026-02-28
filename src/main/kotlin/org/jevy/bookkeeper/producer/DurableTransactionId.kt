package org.jevy.bookkeeper.producer

import org.jevy.bookkeeper_agent.Transaction
import java.security.MessageDigest

object DurableTransactionId {

    fun generate(transaction: Transaction): String = generate(
        owner = transaction.getOwner()?.toString() ?: "",
        date = transaction.getDate().toString(),
        description = transaction.getDescription().toString(),
        amount = transaction.getAmount().toString(),
        account = transaction.getAccount().toString(),
    )

    fun generate(owner: String, date: String, description: String, amount: String, account: String): String {
        val normalized = listOf(
            owner.trim().lowercase(),
            date.trim().lowercase(),
            description.trim().lowercase(),
            normalizeAmount(amount),
            account.trim().lowercase(),
        ).joinToString("|")

        val digest = MessageDigest.getInstance("SHA-256")
        val hash = digest.digest(normalized.toByteArray(Charsets.UTF_8))
        val hex = hash.joinToString("") { "%02x".format(it) }
        return "durable-${hex.take(16)}"
    }

    private fun normalizeAmount(amount: String): String =
        amount.trim().lowercase().replace("$", "").replace(",", "").replace(" ", "")
}
