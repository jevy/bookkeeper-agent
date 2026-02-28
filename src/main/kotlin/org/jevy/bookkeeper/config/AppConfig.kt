package org.jevy.bookkeeper.config

data class AppConfig(
    val kafkaBootstrapServers: String,
    val schemaRegistryUrl: String,
    val googleSheetId: String,
    val googleCredentialsJson: String,
    val openrouterApiKey: String,
    val maxTransactionAgeDays: Long,
    val maxTransactions: Int,
    val additionalContextPrompt: String?,
    val model: String,
    val metricsPort: Int = 9091,
    val pushgatewayUrl: String? = null,
) {
    companion object {
        fun fromEnv(): AppConfig = AppConfig(
            kafkaBootstrapServers = requireEnv("KAFKA_BOOTSTRAP_SERVERS"),
            schemaRegistryUrl = requireEnv("SCHEMA_REGISTRY_URL"),
            googleSheetId = requireEnv("GOOGLE_SHEET_ID"),
            googleCredentialsJson = requireEnv("GOOGLE_CREDENTIALS_JSON"),
            openrouterApiKey = System.getenv("OPENROUTER_API_KEY") ?: "",
            maxTransactionAgeDays = System.getenv("MAX_TRANSACTION_AGE_DAYS")?.toLongOrNull() ?: 365L,
            maxTransactions = System.getenv("MAX_TRANSACTIONS")?.toIntOrNull() ?: 0,
            additionalContextPrompt = System.getenv("ADDITIONAL_CONTEXT_PROMPT")?.takeIf { it.isNotBlank() },
            model = System.getenv("MODEL") ?: "anthropic/claude-sonnet-4-6",
            metricsPort = System.getenv("METRICS_PORT")?.toIntOrNull() ?: 9091,
            pushgatewayUrl = System.getenv("PUSHGATEWAY_URL")?.takeIf { it.isNotBlank() },
        )

        private fun requireEnv(name: String): String =
            System.getenv(name) ?: throw IllegalStateException("Required environment variable $name is not set")
    }
}
