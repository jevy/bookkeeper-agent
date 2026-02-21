package org.jevy.tiller.categorizer.config

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
            anthropicApiKey = "sk-test",
            pollIntervalSeconds = 60,
        )

        assertEquals("localhost:9092", config.kafkaBootstrapServers)
        assertEquals("http://localhost:8081", config.schemaRegistryUrl)
        assertEquals("sheet-123", config.googleSheetId)
        assertEquals("sk-test", config.anthropicApiKey)
        assertEquals(60L, config.pollIntervalSeconds)
    }

    @Test
    fun `fromEnv throws when required vars are missing`() {
        // KAFKA_BOOTSTRAP_SERVERS is required and should not be set in test env
        assertThrows<IllegalStateException> {
            AppConfig.fromEnv()
        }
    }
}
