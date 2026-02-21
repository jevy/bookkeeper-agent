package org.jevy.tiller.categorizer.kafka

object TopicNames {
    const val UNCATEGORIZED = "transactions.uncategorized"
    const val CATEGORIZED = "transactions.categorized"
    const val CATEGORIZATION_FAILED = "transactions.categorization-failed"
}
