package org.jevy.bookkeeper.categorizer.tools

import com.google.gson.Gson
import com.google.gson.JsonParser
import okhttp3.OkHttpClient
import okhttp3.Request
import org.slf4j.LoggerFactory
import java.util.concurrent.TimeUnit

class WebSearchTool(private val braveApiKey: String) {

    private val logger = LoggerFactory.getLogger(WebSearchTool::class.java)
    private val gson = Gson()

    private val httpClient = OkHttpClient.Builder()
        .connectTimeout(10, TimeUnit.SECONDS)
        .readTimeout(30, TimeUnit.SECONDS)
        .build()

    fun execute(query: String): String {
        logger.info("Web search: '{}'", query)

        val request = Request.Builder()
            .url("https://api.search.brave.com/res/v1/web/search?q=${java.net.URLEncoder.encode(query, "UTF-8")}&count=5")
            .header("Accept", "application/json")
            .header("X-Subscription-Token", braveApiKey)
            .get()
            .build()

        val response = httpClient.newCall(request).execute()
        val body = response.body?.string() ?: return "[]"

        if (!response.isSuccessful) {
            logger.warn("Brave Search API error {}: {}", response.code, body)
            return "[]"
        }

        val json = JsonParser.parseString(body).asJsonObject
        val results = json.getAsJsonObject("web")?.getAsJsonArray("results") ?: return "[]"

        val summaries = results.map { result ->
            val obj = result.asJsonObject
            mapOf(
                "title" to (obj.get("title")?.asString ?: ""),
                "description" to (obj.get("description")?.asString ?: ""),
                "url" to (obj.get("url")?.asString ?: ""),
            )
        }

        return gson.toJson(summaries)
    }

    companion object {
        val definition = mapOf(
            "type" to "function",
            "function" to mapOf(
                "name" to "web_search",
                "description" to "Search the web to identify an unfamiliar merchant or transaction description.",
                "parameters" to mapOf(
                    "type" to "object",
                    "properties" to mapOf(
                        "query" to mapOf(
                            "type" to "string",
                            "description" to "Search query",
                        )
                    ),
                    "required" to listOf("query"),
                ),
            ),
        )
    }
}
