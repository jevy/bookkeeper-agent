package org.jevy.bookkeeper.sure

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.jevy.bookkeeper.config.AppConfig
import org.jevy.bookkeeper.kafka.TopicNames
import org.jevy.bookkeeper.writer.CategorySink
import org.jevy.bookkeeper.writer.SinkResult
import org.jevy.bookkeeper_agent.Transaction
import org.slf4j.LoggerFactory

/**
 * Mirrors a categorization into Sure: match the Sheet transaction to a Sure
 * transaction, resolve the category name to a Sure id, skip if already set,
 * otherwise PATCH. Sure is a sink only; this never tombstones anything.
 */
class SureSink(
    private val config: AppConfig,
    private val meterRegistry: MeterRegistry = SimpleMeterRegistry(),
    private val client: SureClient = SureClient(config.sureApiUrl, config.sureApiKey, config.sureMaxApiCallsPerSec, meterRegistry = meterRegistry),
    private val matcher: TransactionMatcher = TransactionMatcher(client, config.sureAccountMap),
    private val resolver: CategoryResolver = CategoryResolver(client),
) : CategorySink {

    private val logger = LoggerFactory.getLogger(SureSink::class.java)

    override val name = "sure"
    override val consumerGroup = "sure-writer"
    /** Chained after the Sheet: only categories confirmed in the Sheet reach Sure. */
    override val sourceTopic = TopicNames.WRITTEN
    override val dlqTopic = TopicNames.SURE_WRITE_FAILED

    override fun write(tx: Transaction): SinkResult {
        val transactionId = tx.getTransactionId().toString()
        if (!config.sureEnabled) return SinkResult.Skipped("disabled")

        val category = tx.getCategory()?.toString()
        if (category.isNullOrBlank()) return SinkResult.Skipped("no_category")

        return try {
            val matched = when (val m = matcher.match(tx)) {
                is MatchResult.Unmatched -> return SinkResult.Rejected(m.reason)
                is MatchResult.Matched -> m
            }
            meterRegistry.counter("bookkeeper.sure.match.rung", "match_rung", matched.rung.toString()).increment()

            val categoryId = when (val r = resolver.resolve(category)) {
                is CategoryResolution.Resolved -> r.id
                is CategoryResolution.Ambiguous -> return SinkResult.Rejected("category_ambiguous")
                CategoryResolution.Unknown -> return SinkResult.Rejected("category_unresolved")
            }

            if (matched.transaction.categoryId == categoryId) {
                return SinkResult.Skipped("already_categorized")
            }

            if (config.sureDryRun) {
                logger.info("DRY RUN: would PATCH Sure transaction {} ({}) with category '{}' ({}) for {} via rung {}",
                    matched.transaction.id, matched.transaction.name, category, categoryId, transactionId, matched.rung)
                return SinkResult.Skipped("dry_run")
            }

            client.updateCategory(matched.transaction.id, categoryId)
            // The written counter belongs to SinkWriter (untagged). The per-rung view is bookkeeper.sure.match.rung above.
            logger.info("Wrote category '{}' to Sure transaction {} for {} via rung {}", category, matched.transaction.id, transactionId, matched.rung)
            SinkResult.Written
        } catch (e: SureUnavailableException) {
            SinkResult.Unavailable(e)
        } catch (e: SureRequestException) {
            SinkResult.Rejected("sure_4xx", e)
        }
    }
}
