package org.jevy.bookkeeper.sure

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.jevy.bookkeeper.config.AppConfig
import org.jevy.bookkeeper.kafka.TopicNames
import org.jevy.bookkeeper.writer.SinkResult
import org.jevy.bookkeeper_agent.Transaction
import org.junit.jupiter.api.Test
import java.time.LocalDate
import kotlin.test.assertEquals
import kotlin.test.assertIs

class SureSinkTest {

    private fun config(enabled: Boolean = true, dryRun: Boolean = false) = AppConfig(
        kafkaBootstrapServers = "localhost:9092", schemaRegistryUrl = "http://localhost:8081",
        googleSheetId = "test", googleCredentialsJson = "{}", openrouterApiKey = "",
        maxTransactionAgeDays = 365, maxTransactions = 0, additionalContextPrompt = null, model = "",
        sureApiUrl = "http://sure", sureApiKey = "k", sureEnabled = enabled, sureDryRun = dryRun,
        sureAccountMap = mapOf("Chequing" to "acct-1"),
    )

    private val client = mockk<SureClient>(relaxed = true)
    private val matcher = mockk<TransactionMatcher>()
    private val resolver = mockk<CategoryResolver>()
    private val registry = SimpleMeterRegistry()

    private fun sink(enabled: Boolean = true, dryRun: Boolean = false) =
        SureSink(config(enabled, dryRun), client, matcher, resolver, registry)

    private fun tx(category: String? = "Groceries"): Transaction = Transaction.newBuilder()
        .setTransactionId("txn-1").setDate("2/15/2026").setDescription("COSTCO")
        .setCategory(category).setAmount("-\$384.91").setAccount("Chequing").build()

    private fun sureTx(categoryId: String? = null) =
        SureTransaction("s-1", LocalDate.of(2026, 2, 15), "COSTCO", -38491, "acct-1", categoryId, "TRN-1")

    @Test
    fun `identity`() {
        val s = sink()
        assertEquals("sure", s.name)
        assertEquals("sure-writer", s.consumerGroup)
        assertEquals(TopicNames.WRITTEN, s.sourceTopic)
        assertEquals(TopicNames.SURE_WRITE_FAILED, s.dlqTopic)
    }

    @Test
    fun `happy path matches, resolves, and PATCHes`() {
        every { matcher.match(any()) } returns MatchResult.Matched(sureTx(), rung = 2)
        every { resolver.resolve("Groceries") } returns CategoryResolution.Resolved("c-1")

        assertEquals(SinkResult.Written, sink().write(tx()))

        verify(exactly = 1) { client.updateCategory("s-1", "c-1") }
        assertEquals(1.0, registry.counter("bookkeeper.sure.transactions.written", "match_rung", "2").count())
    }

    @Test
    fun `already carrying the same category is Skipped with no PATCH`() {
        every { matcher.match(any()) } returns MatchResult.Matched(sureTx(categoryId = "c-1"), rung = 2)
        every { resolver.resolve("Groceries") } returns CategoryResolution.Resolved("c-1")

        assertEquals(SinkResult.Skipped("already_categorized"), sink().write(tx()))
        verify(exactly = 0) { client.updateCategory(any(), any()) }
    }

    @Test
    fun `different existing category is overwritten`() {
        every { matcher.match(any()) } returns MatchResult.Matched(sureTx(categoryId = "c-old"), rung = 2)
        every { resolver.resolve("Groceries") } returns CategoryResolution.Resolved("c-1")
        assertEquals(SinkResult.Written, sink().write(tx()))
        verify { client.updateCategory("s-1", "c-1") }
    }

    @Test
    fun `no category on the event is Skipped`() {
        assertEquals(SinkResult.Skipped("no_category"), sink().write(tx(category = null)))
        verify(exactly = 0) { matcher.match(any()) }
    }

    @Test
    fun `unmatched is Rejected with the matcher reason`() {
        every { matcher.match(any()) } returns MatchResult.Unmatched("no_match")
        val r = sink().write(tx())
        assertIs<SinkResult.Rejected>(r)
        assertEquals("no_match", r.reason)
        verify(exactly = 0) { resolver.resolve(any()) }
    }

    @Test
    fun `unknown category is Rejected category_unresolved`() {
        every { matcher.match(any()) } returns MatchResult.Matched(sureTx(), rung = 2)
        every { resolver.resolve("Groceries") } returns CategoryResolution.Unknown
        assertEquals("category_unresolved", (sink().write(tx()) as SinkResult.Rejected).reason)
        verify(exactly = 0) { client.updateCategory(any(), any()) }
    }

    @Test
    fun `ambiguous category is Rejected category_ambiguous`() {
        every { matcher.match(any()) } returns MatchResult.Matched(sureTx(), rung = 2)
        every { resolver.resolve("Groceries") } returns CategoryResolution.Ambiguous(listOf("c-1", "c-2"))
        assertEquals("category_ambiguous", (sink().write(tx()) as SinkResult.Rejected).reason)
    }

    @Test
    fun `Sure 4xx on PATCH is Rejected`() {
        every { matcher.match(any()) } returns MatchResult.Matched(sureTx(), rung = 2)
        every { resolver.resolve("Groceries") } returns CategoryResolution.Resolved("c-1")
        every { client.updateCategory(any(), any()) } throws SureRequestException(422, "nope")
        assertEquals("sure_4xx", (sink().write(tx()) as SinkResult.Rejected).reason)
    }

    @Test
    fun `Sure unavailable anywhere is Unavailable, never Rejected`() {
        every { matcher.match(any()) } throws SureUnavailableException("connection refused")
        assertIs<SinkResult.Unavailable>(sink().write(tx()))

        every { matcher.match(any()) } returns MatchResult.Matched(sureTx(), rung = 2)
        every { resolver.resolve(any()) } throws SureUnavailableException("503")
        assertIs<SinkResult.Unavailable>(sink().write(tx()))

        every { resolver.resolve(any()) } returns CategoryResolution.Resolved("c-1")
        every { client.updateCategory(any(), any()) } throws SureUnavailableException("timeout")
        assertIs<SinkResult.Unavailable>(sink().write(tx()))
    }

    @Test
    fun `disabled skips without touching Sure`() {
        assertEquals(SinkResult.Skipped("disabled"), sink(enabled = false).write(tx()))
        verify(exactly = 0) { matcher.match(any()) }
    }

    @Test
    fun `dry run matches and resolves but never PATCHes`() {
        every { matcher.match(any()) } returns MatchResult.Matched(sureTx(), rung = 2)
        every { resolver.resolve("Groceries") } returns CategoryResolution.Resolved("c-1")
        assertEquals(SinkResult.Skipped("dry_run"), sink(dryRun = true).write(tx()))
        verify(exactly = 0) { client.updateCategory(any(), any()) }
        assertEquals(1.0, registry.counter("bookkeeper.sure.match.rung", "match_rung", "2").count())
    }
}
