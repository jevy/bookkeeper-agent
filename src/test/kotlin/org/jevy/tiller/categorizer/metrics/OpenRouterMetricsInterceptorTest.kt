package org.jevy.tiller.categorizer.metrics

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Test
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpRequest
import org.springframework.http.HttpStatusCode
import org.springframework.http.client.ClientHttpRequestExecution
import org.springframework.http.client.ClientHttpResponse
import java.io.ByteArrayInputStream
import kotlin.test.assertEquals

class OpenRouterMetricsInterceptorTest {

    private val registry = SimpleMeterRegistry()
    private val interceptor = OpenRouterMetricsInterceptor(registry)

    private fun mockResponse(body: String): ClientHttpResponse {
        val response = mockk<ClientHttpResponse>(relaxed = true)
        every { response.body } returns ByteArrayInputStream(body.toByteArray())
        every { response.statusCode } returns HttpStatusCode.valueOf(200)
        every { response.headers } returns HttpHeaders()
        return response
    }

    private fun execute(body: String): ClientHttpResponse {
        val request = mockk<HttpRequest>(relaxed = true)
        val execution = mockk<ClientHttpRequestExecution>()
        every { execution.execute(any(), any()) } returns mockResponse(body)
        return interceptor.intercept(request, ByteArray(0), execution)
    }

    @Test
    fun `records cost and token metrics from OpenRouter response`() {
        val json = """
        {
          "id": "gen-123",
          "provider": "Google",
          "model": "google/gemini-2.5-flash",
          "usage": {
            "prompt_tokens": 3520,
            "completion_tokens": 45,
            "total_tokens": 3565,
            "cost": 0.000177,
            "prompt_tokens_details": {
              "cached_tokens": 0,
              "cache_read_input_tokens": 0,
              "cache_write_tokens": 0,
              "cache_creation_input_tokens": 0
            },
            "completion_tokens_details": {
              "reasoning_tokens": 0
            }
          },
          "choices": [{"message": {"content": "test"}}]
        }
        """.trimIndent()

        val response = execute(json)

        // Verify the response body is still readable
        val responseBody = response.body.readBytes().decodeToString()
        assert(responseBody.contains("gen-123"))

        // Cost recorded with provider tag
        val cost = registry.counter("tiller.openrouter.cost", "provider", "Google").count()
        assertEquals(0.000177, cost, 0.000001)

        // Zero-value token counters should not be incremented
        assertEquals(0.0, registry.counter("tiller.openrouter.tokens.cached").count())
        assertEquals(0.0, registry.counter("tiller.openrouter.tokens.cache_write").count())
        assertEquals(0.0, registry.counter("tiller.openrouter.tokens.reasoning").count())
    }

    @Test
    fun `records non-zero token metrics`() {
        val json = """
        {
          "provider": "Anthropic",
          "usage": {
            "cost": 0.005,
            "prompt_tokens_details": {
              "cached_tokens": 1500,
              "cache_write_tokens": 3000
            },
            "completion_tokens_details": {
              "reasoning_tokens": 200
            }
          }
        }
        """.trimIndent()

        execute(json)

        assertEquals(0.005, registry.counter("tiller.openrouter.cost", "provider", "Anthropic").count(), 0.000001)
        assertEquals(1500.0, registry.counter("tiller.openrouter.tokens.cached").count())
        assertEquals(3000.0, registry.counter("tiller.openrouter.tokens.cache_write").count())
        assertEquals(200.0, registry.counter("tiller.openrouter.tokens.reasoning").count())
    }

    @Test
    fun `gracefully handles missing usage block`() {
        val json = """{"choices": [{"message": {"content": "hello"}}]}"""

        val response = execute(json)

        // No metrics recorded, no exception
        assertEquals(0.0, registry.counter("tiller.openrouter.cost", "provider", "unknown").count())
        // Response body still readable
        assert(response.body.readBytes().decodeToString().contains("hello"))
    }

    @Test
    fun `gracefully handles malformed JSON`() {
        val response = execute("not valid json {{{")

        // No exception, response still works
        assertEquals(0.0, registry.counter("tiller.openrouter.cost", "provider", "unknown").count())
    }

    @Test
    fun `uses unknown provider when provider field is missing`() {
        val json = """
        {
          "usage": {
            "cost": 0.001
          }
        }
        """.trimIndent()

        execute(json)

        assertEquals(0.001, registry.counter("tiller.openrouter.cost", "provider", "unknown").count(), 0.000001)
    }
}
