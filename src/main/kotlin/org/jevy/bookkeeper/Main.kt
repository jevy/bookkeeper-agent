package org.jevy.bookkeeper

import org.jevy.bookkeeper.categorizer.CategorizerAgent
import org.jevy.bookkeeper.config.AppConfig
import org.jevy.bookkeeper.digest.DigestSender
import org.jevy.bookkeeper.digest.EmailIngester
import org.jevy.bookkeeper.digest.EmailProcessor
import org.jevy.bookkeeper.kafka.TopicInitializer
import org.jevy.bookkeeper.metrics.Metrics
import org.jevy.bookkeeper.producer.TransactionProducer
import org.jevy.bookkeeper.report.SpendingReport
import org.jevy.bookkeeper.replay.DlqReplayer
import org.jevy.bookkeeper.replay.ReplayMode
import org.jevy.bookkeeper.sure.SureSink
import org.jevy.bookkeeper.writer.CategoryWriter
import org.jevy.bookkeeper.writer.SinkWriter
import org.slf4j.LoggerFactory

private val logger = LoggerFactory.getLogger("org.jevy.bookkeeper.Main")

fun main(args: Array<String>) {
    val command = args.firstOrNull() ?: run {
        System.err.println("Usage: bookkeeper-agent <init|producer|categorizer|writer|sure-writer|dlq-replay [sure]|digest-sender|email-ingester|email-processor|weekly-report|monthly-report>")
        System.exit(1)
        return
    }

    when (command) {
        "init" -> {
            val bootstrapServers = System.getenv("KAFKA_BOOTSTRAP_SERVERS")
                ?: throw IllegalStateException("Required environment variable KAFKA_BOOTSTRAP_SERVERS is not set")
            logger.info("Starting Topic Initializer")
            TopicInitializer.run(bootstrapServers)
        }
        "producer" -> {
            val config = AppConfig.fromEnv()
            logger.info("Starting Transaction Producer")
            TransactionProducer(config, Metrics.registry).run()
            config.pushgatewayUrl?.let { Metrics.pushToGateway(it, "bookkeeper-producer") }
        }
        "categorizer" -> {
            val config = AppConfig.fromEnv()
            Metrics.startHttpServer(config.metricsPort)
            logger.info("Starting Categorizer Agent")
            CategorizerAgent(config, Metrics.registry).run(
                onActivity = Metrics::updateActivity,
                onAlive = Metrics::setConsumerAlive,
            )
        }
        "writer" -> {
            val config = AppConfig.fromEnv()
            Metrics.startHttpServer(config.metricsPort)
            logger.info("Starting Category Writer")
            CategoryWriter(config, meterRegistry = Metrics.registry).run(
                onActivity = Metrics::updateActivity,
                onAlive = Metrics::setConsumerAlive,
            )
        }
        "sure-writer" -> {
            val config = AppConfig.fromEnv()
            require(config.sureApiUrl.isNotBlank()) { "SURE_API_URL is required for sure-writer" }
            require(config.sureApiKey.isNotBlank()) { "SURE_API_KEY is required for sure-writer" }
            Metrics.startHttpServer(config.metricsPort)
            logger.info("Starting Sure Writer (enabled={}, dryRun={}, accounts mapped={})",
                config.sureEnabled, config.sureDryRun, config.sureAccountMap.size)
            SinkWriter(config, SureSink(config, meterRegistry = Metrics.registry), Metrics.registry, tombstoneUncategorized = false).run(
                onActivity = Metrics::updateActivity,
                onAlive = Metrics::setConsumerAlive,
            )
        }
        "dlq-replay" -> {
            val bootstrapServers = System.getenv("KAFKA_BOOTSTRAP_SERVERS")
                ?: throw IllegalStateException("Required environment variable KAFKA_BOOTSTRAP_SERVERS is not set")
            val schemaRegistryUrl = System.getenv("SCHEMA_REGISTRY_URL")
                ?: throw IllegalStateException("Required environment variable SCHEMA_REGISTRY_URL is not set")
            val config = AppConfig(
                kafkaBootstrapServers = bootstrapServers,
                schemaRegistryUrl = schemaRegistryUrl,
                googleSheetId = "",
                googleCredentialsJson = "",
                openrouterApiKey = "",
                maxTransactionAgeDays = 0,
                maxTransactions = 0,
                additionalContextPrompt = null,
                model = "",
            )
            val mode = when (args.getOrNull(1)) {
                null -> ReplayMode.RECATEGORIZE
                "sure" -> ReplayMode.SURE
                else -> throw IllegalArgumentException("Unknown dlq-replay mode '${args[1]}'. Use no argument or 'sure'.")
            }
            logger.info("Starting DLQ Replayer ({})", mode)
            DlqReplayer(config, mode).run()
        }
        "digest-sender" -> {
            val config = AppConfig.fromEnv()
            logger.info("Starting Digest Sender")
            DigestSender(config).run()
        }
        "email-ingester" -> {
            val config = AppConfig.fromEnv()
            logger.info("Starting Email Ingester")
            EmailIngester(config).run()
        }
        "email-processor" -> {
            val config = AppConfig.fromEnv()
            Metrics.startHttpServer(config.metricsPort)
            logger.info("Starting Email Processor")
            EmailProcessor(config).run(
                onActivity = Metrics::updateActivity,
                onAlive = Metrics::setConsumerAlive,
            )
        }
        "weekly-report" -> {
            val config = AppConfig.fromEnv()
            logger.info("Starting Weekly Report")
            SpendingReport(config, "weekly").run()
        }
        "monthly-report" -> {
            val config = AppConfig.fromEnv()
            logger.info("Starting Monthly Report")
            SpendingReport(config, "monthly").run()
        }
        else -> {
            System.err.println("Unknown command: $command")
            System.err.println("Usage: bookkeeper-agent <init|producer|categorizer|writer|sure-writer|dlq-replay [sure]|digest-sender|email-ingester|email-processor|weekly-report|monthly-report>")
            System.exit(1)
        }
    }
}
