package org.jevy.bookkeeper.kafka

import org.apache.kafka.common.config.TopicConfig
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class TopicInitializerTest {

    private fun spec(name: String): TopicSpec =
        assertNotNull(TopicInitializer.topics.find { it.name == name }, "missing TopicSpec for $name")

    @Test
    fun `categorized topic resets retention so replay-from-zero is guaranteed`() {
        val s = spec(TopicNames.CATEGORIZED)
        assertEquals(TopicConfig.CLEANUP_POLICY_COMPACT, s.config[TopicConfig.CLEANUP_POLICY_CONFIG])
        assertTrue(TopicConfig.RETENTION_MS_CONFIG in s.deleteConfigs)
    }

    @Test
    fun `written topic is compact with retention reset, like categorized`() {
        val s = spec(TopicNames.WRITTEN)
        assertEquals("transactions.written", s.name)
        assertEquals(TopicConfig.CLEANUP_POLICY_COMPACT, s.config[TopicConfig.CLEANUP_POLICY_CONFIG])
        assertTrue(TopicConfig.RETENTION_MS_CONFIG in s.deleteConfigs)
    }

    @Test
    fun `sure-write-failed mirrors write-failed`() {
        val sure = spec(TopicNames.SURE_WRITE_FAILED)
        val sheets = spec(TopicNames.WRITE_FAILED)
        assertEquals("transactions.sure-write-failed", sure.name)
        assertEquals(sheets.config, sure.config)
        assertEquals(sheets.deleteConfigs, sure.deleteConfigs)
        assertEquals(sheets.partitions, sure.partitions)
    }
}
