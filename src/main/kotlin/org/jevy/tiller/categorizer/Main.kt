package org.jevy.tiller.categorizer

import org.jevy.tiller.categorizer.config.AppConfig
import org.jevy.tiller.categorizer.kafka.TopicInitializer
import org.jevy.tiller.categorizer.producer.TransactionProducer
import org.jevy.tiller.categorizer.categorizer.CategorizerAgent
import org.jevy.tiller.categorizer.writer.CategoryWriter
import org.slf4j.LoggerFactory

private val logger = LoggerFactory.getLogger("org.jevy.tiller.categorizer.Main")

fun main(args: Array<String>) {
    val command = args.firstOrNull() ?: run {
        System.err.println("Usage: tiller-categorizer-agent <init|producer|categorizer|writer>")
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
            TransactionProducer(config).run()
        }
        "categorizer" -> {
            val config = AppConfig.fromEnv()
            logger.info("Starting Categorizer Agent")
            CategorizerAgent(config).run()
        }
        "writer" -> {
            val config = AppConfig.fromEnv()
            logger.info("Starting Category Writer")
            CategoryWriter(config).run()
        }
        else -> {
            System.err.println("Unknown command: $command")
            System.err.println("Usage: tiller-categorizer-agent <init|producer|categorizer|writer>")
            System.exit(1)
        }
    }
}
