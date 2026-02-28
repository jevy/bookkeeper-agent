package org.jevy.bookkeeper.metrics

import com.google.gson.Gson
import com.google.gson.JsonObject
import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.LoggerFactory
import org.springframework.http.HttpRequest
import org.springframework.http.client.ClientHttpRequestExecution
import org.springframework.http.client.ClientHttpRequestInterceptor
import org.springframework.http.client.ClientHttpResponse
import java.io.ByteArrayInputStream
import java.io.InputStream

class OpenRouterMetricsInterceptor(
    private val meterRegistry: MeterRegistry,
) : ClientHttpRequestInterceptor {

    private val logger = LoggerFactory.getLogger(OpenRouterMetricsInterceptor::class.java)
    private val gson = Gson()

    override fun intercept(
        request: HttpRequest,
        body: ByteArray,
        execution: ClientHttpRequestExecution,
    ): ClientHttpResponse {
        val response = execution.execute(request, body)
        return try {
            val bytes = response.body.readBytes()
            recordMetrics(bytes)
            BufferedClientHttpResponse(response, bytes)
        } catch (e: Exception) {
            logger.debug("Failed to buffer/parse OpenRouter response", e)
            response
        }
    }

    private fun recordMetrics(bytes: ByteArray) {
        try {
            val json = gson.fromJson(String(bytes), JsonObject::class.java) ?: return
            val usage = json.getAsJsonObject("usage") ?: return

            val cost = usage.get("cost")?.asDouble
            if (cost != null) {
                val provider = json.get("provider")?.asString ?: "unknown"
                meterRegistry.counter("bookkeeper.openrouter.cost", "provider", provider).increment(cost)
            }

            val promptDetails = usage.getAsJsonObject("prompt_tokens_details")
            if (promptDetails != null) {
                val cached = promptDetails.get("cached_tokens")?.asDouble
                if (cached != null && cached > 0) {
                    meterRegistry.counter("bookkeeper.openrouter.tokens.cached").increment(cached)
                }
                val cacheWrite = promptDetails.get("cache_write_tokens")?.asDouble
                if (cacheWrite != null && cacheWrite > 0) {
                    meterRegistry.counter("bookkeeper.openrouter.tokens.cache_write").increment(cacheWrite)
                }
            }

            val completionDetails = usage.getAsJsonObject("completion_tokens_details")
            if (completionDetails != null) {
                val reasoning = completionDetails.get("reasoning_tokens")?.asDouble
                if (reasoning != null && reasoning > 0) {
                    meterRegistry.counter("bookkeeper.openrouter.tokens.reasoning").increment(reasoning)
                }
            }
        } catch (e: Exception) {
            logger.debug("Failed to parse OpenRouter usage metrics", e)
        }
    }

    private class BufferedClientHttpResponse(
        private val original: ClientHttpResponse,
        private val bufferedBody: ByteArray,
    ) : ClientHttpResponse by original {
        override fun getBody(): InputStream = ByteArrayInputStream(bufferedBody)
    }
}
