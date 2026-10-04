package org.jevy.bookkeeper.sure

import com.google.gson.JsonObject
import com.google.gson.JsonParser
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import okhttp3.HttpUrl.Companion.toHttpUrl
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import org.slf4j.LoggerFactory
import java.io.IOException
import java.math.BigDecimal
import java.time.Duration
import java.time.LocalDate
import java.util.concurrent.TimeUnit

data class SureTransaction(
    val id: String,
    val date: LocalDate,
    val name: String,
    /** Income positive, expense negative. Same convention as the Sheet. */
    val signedAmountCents: Long,
    val accountId: String,
    val categoryId: String?,
    val externalId: String?,
)

data class SureCategory(val id: String, val name: String)

/** Sure could not be reached or answered 5xx after all retries. Never DLQ on this. */
open class SureUnavailableException(message: String, cause: Throwable? = null) : RuntimeException(message, cause)

/**
 * Sure's API throttle refused the call: Rack::Attack allows 10,000 requests per hour per
 * key when self-hosted. The window is hourly, so a few seconds of backoff never clears it;
 * the caller should stop consuming for [retryAfter] and try again. Sure always answers
 * Retry-After: 60 here, whatever the real time to reset.
 */
class SureRateLimitedException(val retryAfter: Duration, endpoint: String) :
    SureUnavailableException("Sure throttled $endpoint (429); retry after ${retryAfter.seconds}s")

/** Sure answered 4xx. The request itself is wrong; safe to DLQ. */
class SureRequestException(val status: Int, val body: String) : RuntimeException("Sure responded $status: $body")

class SureClient(
    private val baseUrl: String,
    private val apiKey: String,
    private val maxCallsPerSec: Int = 5,
    private val maxAttempts: Int = 3,
    private val http: OkHttpClient = OkHttpClient.Builder()
        .connectTimeout(5, TimeUnit.SECONDS)
        .readTimeout(15, TimeUnit.SECONDS)
        .build(),
    private val sleeper: (Long) -> Unit = Thread::sleep,
    private val meterRegistry: MeterRegistry = SimpleMeterRegistry(),
) {
    private val logger = LoggerFactory.getLogger(SureClient::class.java)
    private val json = "application/json".toMediaType()
    private val minIntervalNanos = 1_000_000_000L / maxCallsPerSec.coerceAtLeast(1)
    private var lastCallNanos = 0L

    fun listTransactions(
        accountId: String,
        startDate: LocalDate,
        endDate: LocalDate,
        minEntryAmount: BigDecimal,
        maxEntryAmount: BigDecimal,
    ): List<SureTransaction> = paginate("transactions") { page ->
        "$baseUrl/api/v1/transactions".toHttpUrl().newBuilder()
            .addQueryParameter("account_id", accountId)
            .addQueryParameter("start_date", startDate.toString())
            .addQueryParameter("end_date", endDate.toString())
            .addQueryParameter("min_amount", minEntryAmount.toPlainString())
            .addQueryParameter("max_amount", maxEntryAmount.toPlainString())
            .addQueryParameter("per_page", "100")
            .addQueryParameter("page", page.toString())
            .build().toString()
    }.map { it.toSureTransaction() }

    fun listCategories(): List<SureCategory> = paginate("categories") { page ->
        "$baseUrl/api/v1/categories?per_page=100&page=$page"
    }.map { SureCategory(it["id"].asString, it["name"].asString) }

    fun updateCategory(transactionId: String, categoryId: String) {
        val body = JsonObject().apply {
            add("transaction", JsonObject().apply { addProperty("category_id", categoryId) })
        }
        val request = Request.Builder()
            .url("$baseUrl/api/v1/transactions/$transactionId")
            .patch(body.toString().toRequestBody(json))
            .build()
        execute(request, "PATCH /transactions/:id")
    }

    /** Fetches every page of a list endpoint, reading `pagination.total_pages`. */
    private fun paginate(arrayField: String, urlForPage: (Int) -> String): List<JsonObject> {
        val out = mutableListOf<JsonObject>()
        var page = 1
        while (true) {
            val response = execute(Request.Builder().url(urlForPage(page)).get().build(), "GET /$arrayField")
            response.getAsJsonArray(arrayField).forEach { out += it.asJsonObject }
            val totalPages = response.getAsJsonObject("pagination")?.get("total_pages")?.asInt ?: 1
            if (page >= totalPages) return out
            page++
        }
    }

    private fun execute(request: Request, endpoint: String): JsonObject {
        var attempt = 0
        while (true) {
            attempt++
            throttle()
            val sample = Timer.start(meterRegistry)
            try {
                http.newCall(request.newBuilder().header("X-Api-Key", apiKey).header("Accept", "application/json").build())
                    .execute().use { resp ->
                        sample.stop(meterRegistry.timer("bookkeeper.sure.api.duration", "endpoint", endpoint, "status", resp.code.toString()))
                        val text = resp.body?.string() ?: ""
                        when {
                            resp.isSuccessful -> return if (text.isBlank()) JsonObject() else JsonParser.parseString(text).asJsonObject
                            // Auth failures are about this process, not this transaction. Retrying will not
                            // fix a rotated key, and DLQing would drain the whole backlog. Exit the loop instead.
                            resp.code == 401 || resp.code == 403 ->
                                throw SureUnavailableException("Sure rejected the API key (${resp.code}) on $endpoint; check SURE_API_KEY and SURE_API_URL")
                            // Sure's throttle counts per hour, so the 1 s / 2 s backoff below would only
                            // spend two more requests against a window that has not reset.
                            resp.code == 429 -> throw SureRateLimitedException(
                                Duration.ofSeconds(resp.header("Retry-After")?.trim()?.toLongOrNull()?.coerceAtLeast(1) ?: 60),
                                endpoint,
                            )
                            // Timeouts and server errors are transient.
                            resp.code == 408 || resp.code in 500..599 ->
                                throw IOException("Sure responded ${resp.code}")
                            else -> throw SureRequestException(resp.code, text)
                        }
                    }
            } catch (e: IOException) {
                if (attempt >= maxAttempts) {
                    throw SureUnavailableException("Sure unavailable after $attempt attempts on $endpoint: ${e.message}", e)
                }
                val backoffMs = 1000L shl (attempt - 1)   // 1000, 2000
                logger.warn("Sure call {} failed (attempt {}/{}): {}. Retrying in {} ms", endpoint, attempt, maxAttempts, e.message, backoffMs)
                sleeper(backoffMs)
            }
        }
    }

    @Synchronized
    private fun throttle() {
        val now = System.nanoTime()
        val wait = lastCallNanos + minIntervalNanos - now
        if (lastCallNanos != 0L && wait > 0) Thread.sleep(wait / 1_000_000, (wait % 1_000_000).toInt())
        lastCallNanos = System.nanoTime()
    }

    private fun JsonObject.toSureTransaction() = SureTransaction(
        id = get("id").asString,
        date = LocalDate.parse(get("date").asString),
        name = get("name")?.takeUnless { it.isJsonNull }?.asString ?: "",
        signedAmountCents = get("signed_amount_cents").asLong,
        accountId = getAsJsonObject("account").get("id").asString,
        categoryId = get("category")?.takeUnless { it.isJsonNull }?.asJsonObject?.get("id")?.asString,
        externalId = get("external_id")?.takeUnless { it.isJsonNull }?.asString,
    )
}
