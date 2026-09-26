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
    val awsRegion: String = "us-east-1",
    val s3Bucket: String = "",
    val sesFromAddress: String = "",
    val digestToAddress: String = "",
    val reportToAddresses: String = "",
    val sureApiUrl: String = "",
    val sureApiKey: String = "",
    val sureEnabled: Boolean = true,
    val sureMaxApiCallsPerSec: Int = 5,
    val sureDryRun: Boolean = false,
    /** Sheet account name to Sure account id. Written after duplicate accounts are merged in Sure. */
    val sureAccountMap: Map<String, String> = emptyMap(),
    /**
     * Sheets reads per second, across every read this process makes. Google allows 60 read
     * requests per minute per user for the whole service account, and the producer and the
     * categorizer's sheet_lookup tool draw on the same budget, so the default claims only
     * half of it. Raise it only when no other component is running against the Sheet.
     */
    val sheetsMaxReadsPerSec: Double = 0.5,
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
            awsRegion = System.getenv("AWS_REGION") ?: "us-east-1",
            s3Bucket = System.getenv("S3_BUCKET") ?: "",
            sesFromAddress = System.getenv("SES_FROM_ADDRESS") ?: "",
            digestToAddress = System.getenv("DIGEST_TO_ADDRESS") ?: "",
            reportToAddresses = System.getenv("REPORT_TO_ADDRESSES") ?: "",
            sureApiUrl = System.getenv("SURE_API_URL")?.trimEnd('/') ?: "",
            sureApiKey = System.getenv("SURE_API_KEY") ?: "",
            sureEnabled = System.getenv("SURE_ENABLED")?.toBooleanStrictOrNull() ?: true,
            sureMaxApiCallsPerSec = System.getenv("SURE_MAX_API_CALLS_PER_SEC")?.toIntOrNull() ?: 5,
            sureDryRun = System.getenv("SURE_DRY_RUN")?.toBooleanStrictOrNull() ?: false,
            sureAccountMap = parseAccountMap(System.getenv("SURE_ACCOUNT_MAP")),
            sheetsMaxReadsPerSec = System.getenv("SHEETS_MAX_READS_PER_SEC")?.toDoubleOrNull()?.takeIf { it > 0 } ?: 0.5,
        )

        private fun requireEnv(name: String): String =
            System.getenv(name) ?: throw IllegalStateException("Required environment variable $name is not set")

        /** Parses "Sheet Account Name=sure-account-uuid;Other=uuid". Blank entries are ignored. */
        fun parseAccountMap(raw: String?): Map<String, String> {
            if (raw.isNullOrBlank()) return emptyMap()
            return raw.split(';')
                .map { it.trim() }
                .filter { it.isNotEmpty() }
                .associate { entry ->
                    val idx = entry.indexOf('=')
                    require(idx > 0) { "SURE_ACCOUNT_MAP entry '$entry' must be 'Sheet account name=sure account id'" }
                    entry.substring(0, idx).trim() to entry.substring(idx + 1).trim()
                }
        }
    }
}
