package org.jevy.bookkeeper.writer

import org.jevy.bookkeeper_agent.Transaction
import java.time.Duration

/**
 * Outcome of one sink write. [SinkWriter] decides what to do with each variant:
 * Written and Skipped commit; Rejected goes to the sink's DLQ then commits;
 * Unavailable aborts the loop without committing so the pod restarts and resumes, unless it
 * carries a retryAfter, in which case the loop pauses for that long and retries the record.
 */
sealed interface SinkResult {
    object Written : SinkResult
    /** [landed] means the target already holds this category, so downstream may treat it as written. */
    data class Skipped(val reason: String, val landed: Boolean = false) : SinkResult
    data class Rejected(val reason: String, val cause: Throwable? = null) : SinkResult
    /**
     * [retryAfter] is for a target that is up but throttling: restarting would not help and
     * would only spend more of the quota, so [SinkWriter] waits it out with the consumer paused.
     */
    data class Unavailable(val cause: Throwable, val retryAfter: Duration? = null) : SinkResult
}

interface CategorySink {
    /** Metric prefix segment, e.g. "writer" gives bookkeeper.writer.* */
    val name: String
    val consumerGroup: String
    /** The topic this sink consumes. Sheets reads categorized; Sure reads written. */
    val sourceTopic: String
    val dlqTopic: String
    fun write(tx: Transaction): SinkResult
}

class SinkUnavailableException(cause: Throwable) : RuntimeException("Sink unavailable: ${cause.message}", cause)
