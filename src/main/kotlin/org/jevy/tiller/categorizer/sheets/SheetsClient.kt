package org.jevy.tiller.categorizer.sheets

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.json.gson.GsonFactory
import com.google.api.services.sheets.v4.Sheets
import com.google.api.services.sheets.v4.SheetsScopes
import com.google.auth.http.HttpCredentialsAdapter
import com.google.auth.oauth2.GoogleCredentials
import org.jevy.tiller.categorizer.config.AppConfig
import org.slf4j.LoggerFactory
import java.io.ByteArrayInputStream
import java.io.File

class SheetsClient(private val config: AppConfig) {

    private val logger = LoggerFactory.getLogger(SheetsClient::class.java)

    private val service: Sheets by lazy {
        val credentials = loadCredentials()
        Sheets.Builder(
            GoogleNetHttpTransport.newTrustedTransport(),
            GsonFactory.getDefaultInstance(),
            HttpCredentialsAdapter(credentials)
        )
            .setApplicationName("tiller-categorizer-agent")
            .build()
    }

    private fun loadCredentials(): GoogleCredentials {
        val json = config.googleCredentialsJson
        val stream = if (File(json).exists()) {
            File(json).inputStream()
        } else {
            ByteArrayInputStream(json.toByteArray())
        }
        return GoogleCredentials.fromStream(stream)
            .createScoped(listOf(SheetsScopes.SPREADSHEETS))
    }

    fun readAllRows(range: String = "Transactions!A:U"): List<List<Any>> {
        val response = service.spreadsheets().values()
            .get(config.googleSheetId, range)
            .execute()
        return response.getValues() ?: emptyList()
    }

    fun writeCell(range: String, value: String) {
        val body = com.google.api.services.sheets.v4.model.ValueRange()
            .setValues(listOf(listOf(value)))
        service.spreadsheets().values()
            .update(config.googleSheetId, range, body)
            .setValueInputOption("USER_ENTERED")
            .execute()
        logger.debug("Wrote '{}' to {}", value, range)
    }

    fun searchByDescription(query: String): List<Map<String, String>> {
        val rows = readAllRows()
        if (rows.isEmpty()) return emptyList()

        val header = rows.first().map { it.toString() }
        val queryLower = query.lowercase()

        return rows.drop(1)
            .filter { row ->
                val desc = row.getOrNull(1)?.toString()?.lowercase() ?: ""
                val fullDesc = row.getOrNull(11)?.toString()?.lowercase() ?: ""
                val category = row.getOrNull(2)?.toString() ?: ""
                category.isNotBlank() && (desc.contains(queryLower) || fullDesc.contains(queryLower))
            }
            .take(20)
            .map { row ->
                header.zip(row.map { it.toString() }).toMap()
            }
    }
}
