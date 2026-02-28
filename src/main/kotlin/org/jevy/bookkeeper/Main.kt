package org.jevy.bookkeeper

import org.jevy.bookkeeper.categorizer.CategorizerAgent
import org.jevy.bookkeeper.config.AppConfig
import org.jevy.bookkeeper.kafka.TopicInitializer
import org.jevy.bookkeeper.metrics.Metrics
import org.jevy.bookkeeper.producer.TransactionProducer
import org.jevy.bookkeeper.writer.CategoryWriter
import org.slf4j.LoggerFactory

private val logger = LoggerFactory.getLogger("org.jevy.bookkeeper.Main")

fun main(args: Array<String>) {
    val command = args.firstOrNull() ?: run {
        System.err.println("Usage: bookkeeper-agent <init|producer|categorizer|writer>")
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
        else -> {
            System.err.println("Unknown command: $command")
            System.err.println("Usage: bookkeeper-agent <init|producer|categorizer|writer>")
            System.exit(1)
        }
    }
}
