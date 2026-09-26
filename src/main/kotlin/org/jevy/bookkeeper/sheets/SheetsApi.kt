package org.jevy.bookkeeper.sheets

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.json.gson.GsonFactory
import com.google.api.services.sheets.v4.Sheets
import com.google.api.services.sheets.v4.SheetsScopes
import com.google.api.services.sheets.v4.model.ValueRange
import com.google.auth.http.HttpCredentialsAdapter
import com.google.auth.oauth2.GoogleCredentials
import org.jevy.bookkeeper.config.AppConfig
import java.io.ByteArrayInputStream
import java.io.File

/**
 * The two raw Sheets calls, with no throttling, retrying or caching. [SheetsClient]
 * owns all of that; this is the seam that keeps Google out of the tests.
 */
interface SheetsApi {
    fun read(range: String): List<List<Any>>
    fun write(range: String, value: String)
}

class GoogleSheetsApi(private val config: AppConfig) : SheetsApi {

    private val service: Sheets by lazy {
        Sheets.Builder(
            GoogleNetHttpTransport.newTrustedTransport(),
            GsonFactory.getDefaultInstance(),
            HttpCredentialsAdapter(loadCredentials())
        )
            .setApplicationName("bookkeeper-agent")
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

    override fun read(range: String): List<List<Any>> =
        service.spreadsheets().values()
            .get(config.googleSheetId, range)
            .execute()
            .getValues() ?: emptyList()

    override fun write(range: String, value: String) {
        service.spreadsheets().values()
            .update(config.googleSheetId, range, ValueRange().setValues(listOf(listOf(value))))
            .setValueInputOption("USER_ENTERED")
            .execute()
    }
}
