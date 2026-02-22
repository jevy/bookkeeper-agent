package org.jevy.tiller.categorizer.kafka

import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.AlterConfigOp
import org.apache.kafka.clients.admin.ConfigEntry
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.errors.TopicExistsException
import org.slf4j.LoggerFactory
import java.util.concurrent.ExecutionException

private val logger = LoggerFactory.getLogger("org.jevy.tiller.categorizer.kafka.TopicInitializer")

data class TopicSpec(
    val name: String,
    val config: Map<String, String>,
)

object TopicInitializer {

    private val topics = listOf(
        TopicSpec(
            name = TopicNames.UNCATEGORIZED,
            config = mapOf(TopicConfig.CLEANUP_POLICY_CONFIG to TopicConfig.CLEANUP_POLICY_COMPACT),
        ),
        TopicSpec(
            name = TopicNames.CATEGORIZED,
            config = mapOf(TopicConfig.CLEANUP_POLICY_CONFIG to TopicConfig.CLEANUP_POLICY_COMPACT),
        ),
        TopicSpec(
            name = TopicNames.CATEGORIZATION_FAILED,
            config = mapOf(
                TopicConfig.CLEANUP_POLICY_CONFIG to TopicConfig.CLEANUP_POLICY_DELETE,
                TopicConfig.RETENTION_MS_CONFIG to "604800000", // 7 days
            ),
        ),
    )

    fun run(bootstrapServers: String) {
        logger.info("Initializing topics on {}", bootstrapServers)

        val props = mapOf(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers)
        AdminClient.create(props).use { admin ->
            for (spec in topics) {
                createOrUpdate(admin, spec)
            }
        }

        logger.info("Topic initialization complete")
    }

    private fun createOrUpdate(admin: AdminClient, spec: TopicSpec) {
        val created = tryCreate(admin, spec)
        if (!created) {
            ensureConfig(admin, spec)
        }
    }

    private fun tryCreate(admin: AdminClient, spec: TopicSpec): Boolean {
        val newTopic = NewTopic(spec.name, 1, 1.toShort()).configs(spec.config)
        return try {
            admin.createTopics(listOf(newTopic)).all().get()
            logger.info("Created topic {} with config {}", spec.name, spec.config)
            true
        } catch (e: ExecutionException) {
            if (e.cause is TopicExistsException) {
                logger.info("Topic {} already exists", spec.name)
                false
            } else {
                throw e
            }
        }
    }

    private fun ensureConfig(admin: AdminClient, spec: TopicSpec) {
        val resource = ConfigResource(ConfigResource.Type.TOPIC, spec.name)
        val currentConfig = admin.describeConfigs(listOf(resource)).all().get()[resource]
            ?: return

        val ops = spec.config.mapNotNull { (key, desired) ->
            val current = currentConfig.get(key)?.value()
            if (current != desired) {
                logger.info("Topic {} config {}: {} -> {}", spec.name, key, current, desired)
                AlterConfigOp(ConfigEntry(key, desired), AlterConfigOp.OpType.SET)
            } else {
                null
            }
        }

        if (ops.isNotEmpty()) {
            admin.incrementalAlterConfigs(mapOf(resource to ops)).all().get()
            logger.info("Updated config for topic {}", spec.name)
        } else {
            logger.info("Topic {} config is already correct", spec.name)
        }
    }
}
