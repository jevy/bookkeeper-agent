package org.jevy.tiller.categorizer.categorizer

import com.google.gson.Gson
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.observation.DefaultMeterObservationHandler
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.micrometer.observation.ObservationRegistry
import org.springframework.ai.chat.observation.ChatModelMeterObservationHandler
import org.apache.kafka.clients.producer.ProducerRecord
import org.jevy.tiller.categorizer.categorizer.tools.SheetLookupTool
import org.jevy.tiller.categorizer.categorizer.tools.WebSearchTool
import org.jevy.tiller.categorizer.config.AppConfig
import org.jevy.tiller.categorizer.kafka.KafkaFactory
import org.jevy.tiller.categorizer.kafka.TopicNames
import org.jevy.tiller.categorizer.sheets.SheetsClient
import org.jevy.tiller_categorizer_agent.Transaction
import org.slf4j.LoggerFactory
import org.springframework.ai.chat.client.ChatClient
import org.springframework.ai.openai.OpenAiChatModel
import org.springframework.ai.openai.OpenAiChatOptions
import org.springframework.ai.openai.api.OpenAiApi
import org.springframework.ai.tool.annotation.Tool
import org.springframework.ai.tool.annotation.ToolParam
import org.springframework.ai.model.tool.ToolCallingManager
import org.springframework.retry.support.RetryTemplate
import java.time.Duration

class CategorizerAgent(
    private val config: AppConfig,
    private val meterRegistry: MeterRegistry = SimpleMeterRegistry(),
) {

    companion object {
        internal const val MAX_RETRIES = 3
        internal const val RETRY_BASE_MS = 1000L
    }

    private val logger = LoggerFactory.getLogger(CategorizerAgent::class.java)
    private val sheetsClient = SheetsClient(config)
    private val sheetLookupTool = SheetLookupTool(sheetsClient)
    private val webSearchTool = WebSearchTool(System.getenv("BRAVE_API_KEY") ?: "")
    private val gson = Gson()

    private val categorizedCounter = meterRegistry.counter("tiller.categorizer.transactions", "result", "categorized")
    private val unknownCounter = meterRegistry.counter("tiller.categorizer.transactions", "result", "unknown")
    private val failedCounter = meterRegistry.counter("tiller.categorizer.failed")
    private val durationTimer = meterRegistry.timer("tiller.categorizer.duration")

    private val observationRegistry = ObservationRegistry.create().apply {
        observationConfig().observationHandler(DefaultMeterObservationHandler(meterRegistry))
        observationConfig().observationHandler(ChatModelMeterObservationHandler(meterRegistry))
    }

    private val chatClient: ChatClient = kotlin.run {
        val api = OpenAiApi.builder()
            .baseUrl("https://openrouter.ai/api")
            .apiKey(config.openrouterApiKey)
            .build()
        val chatModel = OpenAiChatModel(
            api,
            OpenAiChatOptions.builder()
                .model(config.model)
                .maxTokens(1024)
                .build(),
            ToolCallingManager.builder().build(),
            RetryTemplate.defaultInstance(),
            observationRegistry,
        )
        ChatClient.builder(chatModel).build()
    }

    private val systemPrompt: String by lazy {
        val categories = sheetsClient.readCategories()
        val categoryList = categories.joinToString("\n") { row ->
            val name = row["Category"] ?: ""
            val group = row["Group"] ?: ""
            val type = row["Type"] ?: ""
            "- $name ($group, $type)"
        }
        logger.info("Loaded {} categories from sheet", categories.size)

        """
        You are a bookkeeping assistant that categorizes financial transactions.
        You have access to the user's transaction history in a Google Sheet.
        Your job is to determine the correct category for a given transaction.

        Available categories:
        $categoryList

        Rules:
        - Use ONLY the categories listed above. Never invent new categories.
        - First check autocat_lookup to see if there's a matching rule for the transaction description.
        - Look at past transactions with similar descriptions to determine the category.
        - If a merchant is unfamiliar, use web search to understand what the business is.
        - Before giving your final answer, use category_lookup to review the last 20 transactions in your proposed category. Make sure the transaction fits the pattern.
        - If you are less than 70% confident in a category, use "Unknown".

        You have a maximum of 5 tool calls for research (sheet_lookup, web_search, category_lookup, autocat_lookup).
        You MUST call submit_category with your final answer. Do not exceed 5 research calls.
        ${config.additionalContextPrompt?.let { "\nAdditional context about the user:\n$it" } ?: ""}
        """.trimIndent()
    }

    data class CategorizationResult(val category: String, val justification: String?)

    private inner class CategorizerTools {
        var submitResult: CategorizationResult? = null

        @Tool(name = "sheet_lookup", description = "Search past transactions in the Google Sheet by description. Returns rows that have been previously categorized with similar merchant names.")
        fun sheetLookup(
            @ToolParam(description = "Search term to match against the Description or Full Description columns") query: String,
        ): String {
            meterRegistry.counter("tiller.categorizer.tool.calls", "tool", "sheet_lookup").increment()
            return sheetLookupTool.execute(query)
        }

        @Tool(name = "web_search", description = "Search the web to identify an unfamiliar merchant or transaction description.")
        fun webSearch(
            @ToolParam(description = "Search query") query: String,
        ): String {
            meterRegistry.counter("tiller.categorizer.tool.calls", "tool", "web_search").increment()
            return webSearchTool.execute(query)
        }

        @Tool(name = "category_lookup", description = "Retrieve the last 20 transactions for a given category. Use this to verify your proposed category fits by reviewing what other transactions are in it.")
        fun categoryLookup(
            @ToolParam(description = "The exact category name to look up") category: String,
        ): String {
            meterRegistry.counter("tiller.categorizer.tool.calls", "tool", "category_lookup").increment()
            val results = sheetsClient.searchByCategory(category)
            logger.info("Category lookup '{}': {} transactions", category, results.size)
            return gson.toJson(results)
        }

        @Tool(name = "autocat_lookup", description = "Search the AutoCat rules sheet for matching categorization rules. AutoCat rules map transaction descriptions to categories. Check this first to see if a rule already exists for the transaction.")
        fun autocatLookup(
            @ToolParam(description = "The transaction description to match against AutoCat rules") description: String,
        ): String {
            meterRegistry.counter("tiller.categorizer.tool.calls", "tool", "autocat_lookup").increment()
            val results = sheetsClient.searchAutocat(description)
            logger.info("AutoCat lookup '{}': {} rules matched", description, results.size)
            return gson.toJson(results)
        }

        @Tool(name = "submit_category", description = "Submit your final categorization. Call this when you have determined the category for the transaction.")
        fun submitCategory(
            @ToolParam(description = "The exact category name, or 'null' if you cannot determine it") category: String,
            @ToolParam(description = "Brief explanation of why this category was chosen") justification: String?,
        ): String {
            submitResult = CategorizationResult(category, justification)
            return "Category submitted successfully"
        }
    }

    fun run(onActivity: () -> Unit = {}, onAlive: (Boolean) -> Unit = {}) {
        val consumer = KafkaFactory.createConsumer(config, "categorizer-agent")
        val producer = KafkaFactory.createProducer(config)
        val tombstoneProducer = KafkaFactory.createTombstoneProducer(config)

        consumer.subscribe(listOf(TopicNames.UNCATEGORIZED))
        logger.info("Subscribed to {}", TopicNames.UNCATEGORIZED)

        onAlive(true)
        var consecutiveErrors = 0

        try {
            while (true) {
                val records = consumer.poll(Duration.ofSeconds(5))
                onActivity()
                for (record in records) {
                    // Skip tombstones (produced after successful writes or DLQ routing)
                    val transaction = record.value() ?: continue

                    if (transaction.getCategory() != null) {
                        logger.info("Transaction {} already categorized, skipping", transaction.getTransactionId())
                        continue
                    }

                    var result: CategorizationResult? = null
                    var succeeded = false
                    for (attempt in 1..MAX_RETRIES) {
                        try {
                            durationTimer.record(Runnable { result = categorize(transaction) })
                            consecutiveErrors = 0
                            succeeded = true
                            break
                        } catch (e: Exception) {
                            consecutiveErrors++
                            if (attempt < MAX_RETRIES) {
                                val backoffMs = RETRY_BASE_MS * (1L shl (attempt - 1))
                                logger.warn("Categorization attempt {}/{} failed for '{}', retrying in {}ms", attempt, MAX_RETRIES, transaction.getDescription(), backoffMs, e)
                                Thread.sleep(backoffMs)
                            } else {
                                logger.error("Categorization failed after {} attempts for '{}'", MAX_RETRIES, transaction.getDescription(), e)
                            }
                        }
                    }

                    if (succeeded && result != null) {
                        val categorized = Transaction.newBuilder(transaction)
                            .setCategory(result!!.category)
                            .setCategoryJustification(result!!.justification)
                            .build()
                        producer.send(ProducerRecord(TopicNames.CATEGORIZED, categorized.getTransactionId().toString(), categorized))
                        logger.info("Categorized '{}' as '{}' ({})", transaction.getDescription(), result!!.category, result!!.justification)
                        if (result!!.category.equals("Unknown", ignoreCase = true)) unknownCounter.increment()
                        else categorizedCounter.increment()
                    } else {
                        val transactionId = transaction.getTransactionId().toString()
                        producer.send(ProducerRecord(TopicNames.CATEGORIZATION_FAILED, transactionId, transaction))
                        tombstoneProducer.send(ProducerRecord(TopicNames.UNCATEGORIZED, transactionId, null))
                        if (!succeeded) logger.warn("Exhausted retries for '{}', sent to DLQ and tombstoned", transaction.getDescription())
                        else logger.warn("Could not categorize '{}', sent to DLQ and tombstoned", transaction.getDescription())
                        failedCounter.increment()
                    }
                }
                consumer.commitSync()
            }
        } finally {
            onAlive(false)
            logger.error("Consumer loop exited — marking unhealthy")
        }
    }

    internal fun categorize(transaction: Transaction): CategorizationResult? {
        val userMessage =
            "Categorize this transaction:\n" +
                "Description: ${transaction.getDescription()}\n" +
                "Full Description: ${transaction.getFullDescription() ?: "N/A"}\n" +
                "Amount: ${transaction.getAmount()}\n" +
                "Account: ${transaction.getAccount()}\n" +
                "Date: ${transaction.getDate()}"

        val tools = CategorizerTools()

        chatClient.prompt()
            .system(systemPrompt)
            .user(userMessage)
            .tools(tools)
            .call()
            .content()

        val result = tools.submitResult ?: kotlin.run {
            logger.warn("Agent did not call submit_category for '{}'", transaction.getDescription())
            return null
        }
        return if (result.category.isBlank() || result.category.equals("null", ignoreCase = true)) null
        else result
    }
}
