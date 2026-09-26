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

    @Test
    fun `sure fields default to disabled-safe values`() {
        val config = AppConfig(
            kafkaBootstrapServers = "localhost:9092",
            schemaRegistryUrl = "http://localhost:8081",
            googleSheetId = "sheet-123",
            googleCredentialsJson = "{}",
            openrouterApiKey = "",
            maxTransactionAgeDays = 365,
            maxTransactions = 0,
            additionalContextPrompt = null,
            model = "",
        )
        assertEquals("", config.sureApiUrl)
        assertEquals("", config.sureApiKey)
        assertEquals(true, config.sureEnabled)
        assertEquals(5, config.sureMaxApiCallsPerSec)
        assertEquals(false, config.sureDryRun)
        assertEquals(emptyMap(), config.sureAccountMap)
    }

    @Test
    fun `sheets read rate defaults to half the quota per second`() {
        val config = AppConfig(
            kafkaBootstrapServers = "localhost:9092",
            schemaRegistryUrl = "http://localhost:8081",
            googleSheetId = "sheet-123",
            googleCredentialsJson = "{}",
            openrouterApiKey = "",
            maxTransactionAgeDays = 365,
            maxTransactions = 0,
            additionalContextPrompt = null,
            model = "",
        )
        // The producer and the categorizer's sheet_lookup share the same 60 reads/min/user
        // quota, so the writer may claim only part of it.
        assertEquals(0.5, config.sheetsMaxReadsPerSec)
    }

    @Test
    fun `parseAccountMap splits name=id pairs on semicolons and trims`() {
        val map = AppConfig.parseAccountMap(" Chequing (1234) = 11111111-aaaa ; Visa=22222222-bbbb;")
        assertEquals(
            mapOf("Chequing (1234)" to "11111111-aaaa", "Visa" to "22222222-bbbb"),
            map,
        )
    }

    @Test
    fun `parseAccountMap of null or blank is empty`() {
        assertEquals(emptyMap(), AppConfig.parseAccountMap(null))
        assertEquals(emptyMap(), AppConfig.parseAccountMap("   "))
    }

    @Test
    fun `parseAccountMap rejects an entry without an equals sign`() {
        assertThrows<IllegalArgumentException> { AppConfig.parseAccountMap("Visa") }
    }
}
