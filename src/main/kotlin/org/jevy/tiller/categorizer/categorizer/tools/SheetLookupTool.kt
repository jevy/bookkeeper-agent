package org.jevy.tiller.categorizer.categorizer.tools

import com.google.gson.Gson
import org.jevy.tiller.categorizer.sheets.SheetsClient
import org.slf4j.LoggerFactory

class SheetLookupTool(private val sheetsClient: SheetsClient) {

    private val logger = LoggerFactory.getLogger(SheetLookupTool::class.java)
    private val gson = Gson()

    fun execute(query: String): String {
        logger.info("Sheet lookup: '{}'", query)
        val results = sheetsClient.searchByDescription(query)
        logger.info("Found {} matching transactions", results.size)
        return gson.toJson(results)
    }

    companion object {
        val definition = mapOf(
            "type" to "function",
            "function" to mapOf(
                "name" to "sheet_lookup",
                "description" to "Search past transactions in the Google Sheet by description. Returns rows that have been previously categorized with similar merchant names.",
                "parameters" to mapOf(
                    "type" to "object",
                    "properties" to mapOf(
                        "query" to mapOf(
                            "type" to "string",
                            "description" to "Search term to match against the Description or Full Description columns",
                        )
                    ),
                    "required" to listOf("query"),
                ),
            ),
        )
    }
}
