package org.jevy.bookkeeper.sure

import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import okhttp3.mockwebserver.SocketPolicy
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.math.BigDecimal
import java.time.LocalDate
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertTrue

class SureClientTest {

    private lateinit var server: MockWebServer
    private val sleeps = mutableListOf<Long>()

    @BeforeEach
    fun setUp() {
        server = MockWebServer()
        server.start()
    }

    @AfterEach
    fun tearDown() = server.shutdown()

    private fun client(maxCallsPerSec: Int = 1000) = SureClient(
        baseUrl = server.url("/").toString().trimEnd('/'),
        apiKey = "secret",
        maxCallsPerSec = maxCallsPerSec,
        sleeper = { sleeps += it },
    )

    private fun txJson(id: String, cents: Long, category: String? = null, page: Int = 1, totalPages: Int = 1) = """
        {"transactions":[{"id":"$id","date":"2026-02-15","name":"COSTCO","signed_amount_cents":$cents,
          "external_id":"TRN-1","account":{"id":"acct-1","name":"Chequing"},
          "category":${if (category == null) "null" else """{"id":"cat-1","name":"$category"}"""}}],
         "pagination":{"page":$page,"per_page":100,"total_count":1,"total_pages":$totalPages}}
    """.trimIndent()

    private val emptyCategories = """{"categories":[],"pagination":{"page":1,"per_page":100,"total_count":0,"total_pages":1}}"""

    @Test
    fun `listTransactions sends api key, filters, and parses the response`() {
        server.enqueue(MockResponse().setBody(txJson("t-1", -38491, "Groceries")))

        val result = client().listTransactions(
            accountId = "acct-1",
            startDate = LocalDate.of(2026, 2, 12),
            endDate = LocalDate.of(2026, 2, 18),
            minEntryAmount = BigDecimal("384.91"),
            maxEntryAmount = BigDecimal("384.91"),
        )

        val req = server.takeRequest()
        assertEquals("secret", req.getHeader("X-Api-Key"))
        assertEquals("/api/v1/transactions", req.requestUrl!!.encodedPath)
        assertEquals("acct-1", req.requestUrl!!.queryParameter("account_id"))
        assertEquals("2026-02-12", req.requestUrl!!.queryParameter("start_date"))
        assertEquals("2026-02-18", req.requestUrl!!.queryParameter("end_date"))
        assertEquals("384.91", req.requestUrl!!.queryParameter("min_amount"))
        assertEquals("384.91", req.requestUrl!!.queryParameter("max_amount"))
        assertEquals("100", req.requestUrl!!.queryParameter("per_page"))
        assertEquals("1", req.requestUrl!!.queryParameter("page"))

        assertEquals(1, result.size)
        val tx = result[0]
        assertEquals("t-1", tx.id)
        assertEquals(LocalDate.of(2026, 2, 15), tx.date)
        assertEquals("COSTCO", tx.name)
        assertEquals(-38491L, tx.signedAmountCents)
        assertEquals("acct-1", tx.accountId)
        assertEquals("cat-1", tx.categoryId)
        assertEquals("TRN-1", tx.externalId)
    }

    @Test
    fun `listTransactions follows pagination`() {
        server.enqueue(MockResponse().setBody(txJson("t-1", -100, page = 1, totalPages = 2)))
        server.enqueue(MockResponse().setBody(txJson("t-2", -100, page = 2, totalPages = 2)))

        val result = client().listTransactions("acct-1", LocalDate.of(2026, 1, 1), LocalDate.of(2026, 1, 1), BigDecimal("1.00"), BigDecimal("1.00"))

        assertEquals(listOf("t-1", "t-2"), result.map { it.id })
        server.takeRequest()
        assertEquals("2", server.takeRequest().requestUrl!!.queryParameter("page"))
    }

    @Test
    fun `null category parses as null categoryId`() {
        server.enqueue(MockResponse().setBody(txJson("t-1", -100, category = null)))
        val tx = client().listTransactions("acct-1", LocalDate.of(2026, 1, 1), LocalDate.of(2026, 1, 1), BigDecimal.ONE, BigDecimal.ONE).single()
        assertNull(tx.categoryId)
    }

    @Test
    fun `listCategories parses id and name`() {
        server.enqueue(MockResponse().setBody("""
            {"categories":[{"id":"c-1","name":"Groceries"},{"id":"c-2","name":"groceries"}],
             "pagination":{"page":1,"per_page":100,"total_count":2,"total_pages":1}}
        """.trimIndent()))
        val cats = client().listCategories()
        assertEquals(listOf(SureCategory("c-1", "Groceries"), SureCategory("c-2", "groceries")), cats)
        assertEquals("/api/v1/categories", server.takeRequest().requestUrl!!.encodedPath)
    }

    @Test
    fun `updateCategory PATCHes only category_id`() {
        server.enqueue(MockResponse().setBody("""{"id":"t-1"}"""))
        client().updateCategory("t-1", "c-1")
        val req = server.takeRequest()
        assertEquals("PATCH", req.method)
        assertEquals("/api/v1/transactions/t-1", req.requestUrl!!.encodedPath)
        assertEquals("secret", req.getHeader("X-Api-Key"))
        val body = req.body.readUtf8()
        assertEquals("""{"transaction":{"category_id":"c-1"}}""", body)
    }

    @Test
    fun `4xx raises SureRequestException without retry`() {
        server.enqueue(MockResponse().setResponseCode(422).setBody("""{"error":"validation_failed"}"""))
        val e = assertThrows<SureRequestException> { client().updateCategory("t-1", "c-1") }
        assertEquals(422, e.status)
        assertEquals(1, server.requestCount)
        assertTrue(sleeps.isEmpty())
    }

    @Test
    fun `401 and 403 raise SureUnavailableException immediately, never SureRequestException`() {
        server.enqueue(MockResponse().setResponseCode(401).setBody("""{"error":"unauthorized"}"""))
        assertThrows<SureUnavailableException> { client().listCategories() }
        assertEquals(1, server.requestCount)
        assertTrue(sleeps.isEmpty())

        server.enqueue(MockResponse().setResponseCode(403))
        assertThrows<SureUnavailableException> { client().updateCategory("t-1", "c-1") }
    }

    @Test
    fun `429 raises SureRateLimitedException at once carrying Retry-After, without spending retries`() {
        server.enqueue(MockResponse().setResponseCode(429).setHeader("Retry-After", "60"))
        val e = assertThrows<SureRateLimitedException> { client().listCategories() }
        assertEquals(java.time.Duration.ofSeconds(60), e.retryAfter)
        assertEquals(1, server.requestCount)
        assertTrue(sleeps.isEmpty())
    }

    @Test
    fun `429 without Retry-After waits 60 s and is still a SureUnavailableException`() {
        server.enqueue(MockResponse().setResponseCode(429))
        val e = assertThrows<SureUnavailableException> { client().listCategories() }
        assertIs<SureRateLimitedException>(e)
        assertEquals(java.time.Duration.ofSeconds(60), e.retryAfter)
    }

    @Test
    fun `5xx retries with backoff then raises SureUnavailableException`() {
        repeat(3) { server.enqueue(MockResponse().setResponseCode(503)) }
        assertThrows<SureUnavailableException> { client().listCategories() }
        assertEquals(3, server.requestCount)
        assertEquals(listOf(1000L, 2000L), sleeps)
    }

    @Test
    fun `5xx then success returns normally`() {
        server.enqueue(MockResponse().setResponseCode(500))
        server.enqueue(MockResponse().setBody(emptyCategories))
        assertEquals(emptyList(), client().listCategories())
        assertEquals(2, server.requestCount)
    }

    @Test
    fun `connection failure raises SureUnavailableException`() {
        repeat(3) { server.enqueue(MockResponse().setSocketPolicy(SocketPolicy.DISCONNECT_AT_START)) }
        assertThrows<SureUnavailableException> { client().listCategories() }
    }

    @Test
    fun `rate limiter spaces calls to at most maxCallsPerSec`() {
        repeat(3) { server.enqueue(MockResponse().setBody(emptyCategories)) }
        val c = client(maxCallsPerSec = 2)  // 500 ms minimum spacing
        val t0 = System.nanoTime()
        repeat(3) { c.listCategories() }
        val elapsedMs = (System.nanoTime() - t0) / 1_000_000
        assertTrue(elapsedMs >= 950, "expected two 500 ms gaps, got ${elapsedMs}ms")
    }
}
