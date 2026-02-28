package org.jevy.bookkeeper.config

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import kotlin.test.assertEquals

class AppConfigTest {

    @Test
    fun `constructor accepts all fields`() {
        val config = AppConfig(
            kafkaBootstrapServers = "localhost:9092",
            schemaRegistryUrl = "http://localhost:8081",
            googleSheetId = "sheet-123",
            googleCredentialsJson = """{"type":"service_account"}""",
            openrouterApiKey = "sk-test",
            maxTransactionAgeDays = 365,
            maxTransactions = 0,
            additionalContextPrompt = null,
            model = "anthropic/claude-sonnet-4-6",
        )

        assertEquals("localhost:9092", config.kafkaBootstrapServers)
        assertEquals("http://localhost:8081", config.schemaRegistryUrl)
        assertEquals("sheet-123", config.googleSheetId)
        assertEquals("sk-test", config.openrouterApiKey)
        assertEquals(365L, config.maxTransactionAgeDays)
    }

    @Test
    fun `fromEnv throws when required vars are missing`() {
        // KAFKA_BOOTSTRAP_SERVERS is required and should not be set in test env
        assertThrows<IllegalStateException> {
            AppConfig.fromEnv()
        }
    }
}
