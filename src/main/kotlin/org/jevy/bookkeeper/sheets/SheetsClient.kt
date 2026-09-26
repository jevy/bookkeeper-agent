package org.jevy.bookkeeper.sheets

import com.google.api.client.http.HttpResponseException
import org.jevy.bookkeeper.config.AppConfig
import org.slf4j.LoggerFactory
import kotlin.math.ceil

/**
 * Google refused the call because the per-minute quota is spent, and backing off did not
 * clear it. Transient: the caller must report its sink unavailable and let the offset stand,
 * never dead-letter the record.
 */
class SheetsUnavailableException(message: String, cause: Throwable? = null) : RuntimeException(message, cause)

/**
 * Every Sheets call the process makes goes through here, so this is where the shared
 * 60 requests/min/user quota is respected: reads are throttled to [maxReadsPerSec] and
 * a quota refusal is retried with exponential backoff before it is called unavailable.
 */
class SheetsClient(
    private val config: AppConfig,
    private val api: SheetsApi = GoogleSheetsApi(config),
    private val maxReadsPerSec: Double = config.sheetsMaxReadsPerSec,
    private val maxAttempts: Int = MAX_ATTEMPTS,
    private val sleeper: (Long) -> Unit = Thread::sleep,
) {

    private val logger = LoggerFactory.getLogger(SheetsClient::class.java)

    companion object {
        /** 4 attempts with [RETRY_BASE_MS] backoff sleeps 2s + 4s + 8s: 14 s worst case per
         *  record, far inside the 360 s max.poll.interval.ms the consumer runs with. */
        internal const val MAX_ATTEMPTS = 4
        internal const val RETRY_BASE_MS = 2000L

        /** True for the 429 / RESOURCE_EXHAUSTED / rateLimitExceeded family, whatever wraps it. */
        internal fun isRateLimited(e: Throwable): Boolean {
            if (e is HttpResponseException && e.statusCode == 429) return true
            val text = generateSequence(e) { it.cause }.take(5).joinToString(" ") { it.message ?: "" }
            return text.contains("RESOURCE_EXHAUSTED") ||
                text.contains("rateLimitExceeded") ||
                text.contains("Quota exceeded")
        }
    }

    private val minIntervalNanos: Long = ceil(1_000_000_000.0 / maxReadsPerSec.coerceAtLeast(0.001)).toLong()
    private var lastReadNanos = 0L

    fun readAllRows(range: String = "Transactions!A:U"): List<List<Any>> =
        withRetry("read $range", throttled = true) { api.read(range) }

    fun writeCell(range: String, value: String) {
        withRetry("write $range", throttled = false) { api.write(range, value) }
        logger.debug("Wrote '{}' to {}", value, range)
    }

    /**
     * Runs [call], retrying only a quota refusal. Any other failure is the caller's
     * problem and propagates on the first attempt: a bad range is not fixed by waiting.
     */
    private fun <T> withRetry(operation: String, throttled: Boolean, call: () -> T): T {
        var attempt = 0
        while (true) {
            attempt++
            if (throttled) throttleRead()
            try {
                return call()
            } catch (e: Exception) {
                if (!isRateLimited(e)) throw e
                if (attempt >= maxAttempts) {
                    throw SheetsUnavailableException(
                        "Sheets quota still exhausted after $attempt attempts on $operation: ${e.message}", e
                    )
                }
                val backoffMs = RETRY_BASE_MS shl (attempt - 1)
                logger.warn("Sheets rate limited on {} (attempt {}/{}), retrying in {} ms", operation, attempt, maxAttempts, backoffMs)
                sleeper(backoffMs)
            }
        }
    }

    @Synchronized
    private fun throttleRead() {
        val wait = lastReadNanos + minIntervalNanos - System.nanoTime()
        if (lastReadNanos != 0L && wait > 0) sleeper(ceil(wait / 1_000_000.0).toLong())
        lastReadNanos = System.nanoTime()
    }

    fun readCategories(): List<Map<String, String>> {
        val rows = readAllRows("Categories!A:D")
        if (rows.size <= 1) return emptyList()

        val header = rows.first().map { it.toString() }
        return rows.drop(1)
            .filter { row -> row.isNotEmpty() && row.first().toString().isNotBlank() }
            .map { row -> header.zip(row.map { it.toString() }).toMap() }
    }

    fun searchByCategory(category: String): List<Map<String, String>> {
        val rows = readAllRows()
        if (rows.isEmpty()) return emptyList()

        val header = rows.first().map { it.toString() }
        val colIndex = header.withIndex().associate { (i, name) -> name to i }
        val categoryIdx = colIndex["Category"] ?: return emptyList()
        val categoryLower = category.lowercase()

        return rows.drop(1)
            .filter { row ->
                val cat = row.getOrNull(categoryIdx)?.toString()?.lowercase() ?: ""
                cat == categoryLower
            }
            .takeLast(20)
            .map { row ->
                header.zip(row.map { it.toString() }).toMap()
            }
    }

    fun searchAutocat(query: String): List<Map<String, String>> {
        val rows = readAllRows("AutoCat!A:G")
        if (rows.size <= 1) return emptyList()

        val header = rows.first().map { it.toString() }
        val colIndex = header.withIndex().associate { (i, name) -> name to i }
        val descContainsIdx = colIndex["Description Contains"] ?: return emptyList()
        val queryLower = query.lowercase()

        return rows.drop(1)
            .filter { row ->
                val descContains = row.getOrNull(descContainsIdx)?.toString()?.lowercase() ?: ""
                descContains.isNotBlank() && queryLower.contains(descContains)
            }
            .map { row -> header.zip(row.map { it.toString() }).toMap() }
    }

    fun searchByDescription(query: String): List<Map<String, String>> {
        val rows = readAllRows()
        if (rows.isEmpty()) return emptyList()

        val header = rows.first().map { it.toString() }
        val colIndex = header.withIndex().associate { (i, name) -> name to i }
        val descIdx = colIndex["Description"] ?: return emptyList()
        val fullDescIdx = colIndex["Full Description"] ?: return emptyList()
        val categoryIdx = colIndex["Category"] ?: return emptyList()
        val queryLower = query.lowercase()

        return rows.drop(1)
            .filter { row ->
                val desc = row.getOrNull(descIdx)?.toString()?.lowercase() ?: ""
                val fullDesc = row.getOrNull(fullDescIdx)?.toString()?.lowercase() ?: ""
                val category = row.getOrNull(categoryIdx)?.toString() ?: ""
                category.isNotBlank() && (desc.contains(queryLower) || fullDesc.contains(queryLower))
            }
            .take(20)
            .map { row ->
                header.zip(row.map { it.toString() }).toMap()
            }
    }
}
