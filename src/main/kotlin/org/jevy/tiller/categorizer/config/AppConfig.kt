package org.jevy.tiller.categorizer.config

data class AppConfig(
    val kafkaBootstrapServers: String,
    val schemaRegistryUrl: String,
    val googleSheetId: String,
    val googleCredentialsJson: String,
    val anthropicApiKey: String,
    val pollIntervalSeconds: Long,
) {
    companion object {
        fun fromEnv(): AppConfig = AppConfig(
            kafkaBootstrapServers = requireEnv("KAFKA_BOOTSTRAP_SERVERS"),
            schemaRegistryUrl = requireEnv("SCHEMA_REGISTRY_URL"),
            googleSheetId = requireEnv("GOOGLE_SHEET_ID"),
            googleCredentialsJson = requireEnv("GOOGLE_CREDENTIALS_JSON"),
            anthropicApiKey = System.getenv("ANTHROPIC_API_KEY") ?: "",
            pollIntervalSeconds = System.getenv("POLL_INTERVAL_SECONDS")?.toLongOrNull() ?: 300L,
        )

        private fun requireEnv(name: String): String =
            System.getenv(name) ?: throw IllegalStateException("Required environment variable $name is not set")
    }
}
