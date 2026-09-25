package org.jevy.bookkeeper.kafka

object TopicNames {
    const val UNCATEGORIZED = "transactions.uncategorized"
    const val CATEGORIZED = "transactions.categorized"
    const val PENDING_DIGEST = "transactions.pending-digest"
    const val CATEGORIZATION_FAILED = "transactions.categorization-failed"
    const val WRITE_FAILED = "transactions.write-failed"
    /** Categorizations confirmed to be in the Sheet. Fed by the Sheets writer, consumed by the Sure writer. */
    const val WRITTEN = "transactions.written"
    const val SURE_WRITE_FAILED = "transactions.sure-write-failed"
    const val EMAIL_INBOX = "email.inbox"
    const val EMAIL_PROCESSED = "email.processed"
}
