package org.jevy.bookkeeper.writer

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.jevy.bookkeeper.config.AppConfig
import org.jevy.bookkeeper.kafka.TopicNames
import org.jevy.bookkeeper.sheets.SheetTransaction
import org.jevy.bookkeeper.sheets.SheetsClient
import org.jevy.bookkeeper_agent.Transaction

/**
 * The Sheets writer process: [SinkWriter] over [SheetsSink] with tombstoning on
 * and publish-on-done to transactions.written, which feeds the Sure writer.
 * Command name, consumer group and metric prefix are unchanged from before the
 * sink split. The internal delegates exist for CategoryWriterTest.
 */
class CategoryWriter(
    config: AppConfig,
    sheetsClient: SheetsClient = SheetsClient(config),
    meterRegistry: MeterRegistry = SimpleMeterRegistry(),
) {
    private val sink = SheetsSink(config, sheetsClient)
    private val writer = SinkWriter(
        config, sink, meterRegistry,
        tombstoneUncategorized = true,
        publishOnDone = TopicNames.WRITTEN,
    )

    fun run(onActivity: () -> Unit = {}, onAlive: (Boolean) -> Unit = {}) = writer.run(onActivity, onAlive)

    /** Throws [RowNotFoundException] when no row matches. */
    internal fun writeCategory(transaction: Transaction) {
        sink.writeCategory(transaction)
    }

    internal fun findRow(target: Transaction, rows: List<SheetTransaction>): Int? = sink.findRow(target, rows)
}
