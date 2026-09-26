package org.jevy.bookkeeper.sheets

import com.google.api.client.http.HttpHeaders
import com.google.api.client.http.HttpResponseException
import org.jevy.bookkeeper.config.AppConfig
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Google answers 429 / RESOURCE_EXHAUSTED / rateLimitExceeded when the 60 reads per
 * minute per user quota is spent. That is transient: the writer must back off and
 * retry, and report the sink unavailable if the quota never frees up, never dead-letter.
 */
class SheetsClientRateLimitTest {

    private val config = AppConfig(
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

    private val sleeps = mutableListOf<Long>()

    /**
     * Retried reads pass back through the throttle, so [sleeps] holds both kinds of wait.
     * A backoff is always at least [SheetsClient.RETRY_BASE_MS]; throttle waits at the
     * rates used here are a millisecond or two.
     */
    private val backoffs get() = sleeps.filter { it >= SheetsClient.RETRY_BASE_MS }

    /** The shape Google actually returns when the read quota is spent. */
    private fun quotaExceeded(): HttpResponseException =
        HttpResponseException.Builder(429, "Too Many Requests", HttpHeaders())
            .setMessage(
                "429 Too Many Requests: Quota exceeded for quota metric 'Read requests' and limit " +
                    "'Read requests per minute per user' of service 'sheets.googleapis.com'"
            )
            .build()

    private fun notFound(): HttpResponseException =
        HttpResponseException.Builder(404, "Not Found", HttpHeaders()).setMessage("404 Not Found").build()

    /** Fails the first [failures] calls with [error], then serves [rows]. */
    private class FakeApi(
        private val failures: Int,
        private val error: () -> Exception,
        private val rows: List<List<Any>> = listOf(listOf("Date", "Category")),
    ) : SheetsApi {
        var reads = 0
        var writes = 0
        override fun read(range: String): List<List<Any>> {
            reads++
            if (reads <= failures) throw error()
            return rows
        }
        override fun write(range: String, value: String) {
            writes++
            if (writes <= failures) throw error()
        }
    }

    private fun client(
        api: SheetsApi,
        maxReadsPerSec: Double = 1000.0,
        maxAttempts: Int = 4,
    ) = SheetsClient(
        config = config,
        api = api,
        maxReadsPerSec = maxReadsPerSec,
        maxAttempts = maxAttempts,
        sleeper = { sleeps += it },
    )

    @Test
    fun `a rate limited read is retried and its rows returned`() {
        val api = FakeApi(failures = 2, error = ::quotaExceeded)

        val rows = client(api).readAllRows()

        assertEquals(listOf(listOf("Date", "Category")), rows)
        assertEquals(3, api.reads)
        assertEquals(listOf(2000L, 4000L), backoffs)
    }

    @Test
    fun `a read whose quota never frees up throws SheetsUnavailableException`() {
        val api = FakeApi(failures = Int.MAX_VALUE, error = ::quotaExceeded)

        val e = assertThrows<SheetsUnavailableException> { client(api).readAllRows() }

        assertEquals(4, api.reads)
        assertTrue(e.message!!.contains("4 attempts"), "expected the attempt count in: ${e.message}")
    }

    @Test
    fun `retries finish well inside the six minute max poll interval`() {
        val api = FakeApi(failures = Int.MAX_VALUE, error = ::quotaExceeded)

        assertThrows<SheetsUnavailableException> { client(api).readAllRows() }

        // KafkaFactory sets max.poll.interval.ms=360000 with max.poll.records=1.
        assertTrue(sleeps.sum() < 60_000, "per-record retries slept ${sleeps.sum()} ms")
    }

    @Test
    fun `a read that fails for a reason other than quota is not retried`() {
        val api = FakeApi(failures = Int.MAX_VALUE, error = ::notFound)

        val e = assertThrows<HttpResponseException> { client(api).readAllRows() }

        assertEquals(404, e.statusCode)
        assertEquals(1, api.reads)
        assertEquals(emptyList(), sleeps)
    }

    @Test
    fun `reads are throttled to the configured rate`() {
        val api = FakeApi(failures = 0, error = ::quotaExceeded)
        val client = client(api, maxReadsPerSec = 2.0)

        client.readAllRows()
        client.readAllRows()

        assertEquals(2, api.reads)
        // 2 reads/sec means one 500 ms gap. The first read is free.
        assertEquals(1, sleeps.size, "expected exactly one throttle wait, got $sleeps")
        assertTrue(sleeps.single() in 400L..500L, "expected a ~500 ms wait, got ${sleeps.single()}")
    }

    @Test
    fun `a rate limited write whose quota never frees up throws SheetsUnavailableException`() {
        val api = FakeApi(failures = Int.MAX_VALUE, error = ::quotaExceeded)

        assertThrows<SheetsUnavailableException> { client(api).writeCell("Transactions!C2", "Groceries") }

        assertEquals(4, api.writes)
    }
}
