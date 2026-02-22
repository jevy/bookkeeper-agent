package org.jevy.tiller.categorizer.categorizer

import com.anthropic.client.AnthropicClient
import com.anthropic.client.okhttp.AnthropicOkHttpClient
import com.anthropic.core.JsonValue
import com.anthropic.models.messages.ContentBlockParam
import com.anthropic.models.messages.MessageCreateParams
import com.anthropic.models.messages.MessageParam
import com.anthropic.models.messages.Model
import com.anthropic.models.messages.StopReason
import com.anthropic.models.messages.Tool
import com.anthropic.models.messages.ToolResultBlockParam
import org.apache.kafka.clients.producer.ProducerRecord
import org.jevy.tiller.categorizer.categorizer.tools.SheetLookupTool
import org.jevy.tiller.categorizer.categorizer.tools.WebSearchTool
import org.jevy.tiller.categorizer.config.AppConfig
import org.jevy.tiller.categorizer.kafka.KafkaFactory
import org.jevy.tiller.categorizer.kafka.TopicNames
import org.jevy.tiller.categorizer.sheets.SheetsClient
import org.jevy.tiller_categorizer_agent.Transaction
import org.slf4j.LoggerFactory
import java.time.Duration

class CategorizerAgent(private val config: AppConfig) {

    private val logger = LoggerFactory.getLogger(CategorizerAgent::class.java)
    private val sheetsClient = SheetsClient(config)
    private val sheetLookupTool = SheetLookupTool(sheetsClient)
    private val webSearchTool = WebSearchTool(System.getenv("BRAVE_API_KEY") ?: "")

    private val client: AnthropicClient = AnthropicOkHttpClient.builder()
        .apiKey(config.anthropicApiKey)
        .responseValidation(true)
        .build()

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

    private val tools = listOf(
        Tool.builder()
            .name("sheet_lookup")
            .description("Search past transactions in the Google Sheet by description. Returns rows that have been previously categorized with similar merchant names.")
            .inputSchema(
                Tool.InputSchema.builder()
                    .properties(
                        Tool.InputSchema.Properties.builder()
                            .putAdditionalProperty("query", JsonValue.from(mapOf(
                                "type" to "string",
                                "description" to "Search term to match against the Description or Full Description columns",
                            )))
                            .build()
                    )
                    .addRequired("query")
                    .build()
            )
            .build(),
        Tool.builder()
            .name("web_search")
            .description("Search the web to identify an unfamiliar merchant or transaction description.")
            .inputSchema(
                Tool.InputSchema.builder()
                    .properties(
                        Tool.InputSchema.Properties.builder()
                            .putAdditionalProperty("query", JsonValue.from(mapOf(
                                "type" to "string",
                                "description" to "Search query",
                            )))
                            .build()
                    )
                    .addRequired("query")
                    .build()
            )
            .build(),
        Tool.builder()
            .name("category_lookup")
            .description("Retrieve the last 20 transactions for a given category. Use this to verify your proposed category fits by reviewing what other transactions are in it.")
            .inputSchema(
                Tool.InputSchema.builder()
                    .properties(
                        Tool.InputSchema.Properties.builder()
                            .putAdditionalProperty("category", JsonValue.from(mapOf(
                                "type" to "string",
                                "description" to "The exact category name to look up",
                            )))
                            .build()
                    )
                    .addRequired("category")
                    .build()
            )
            .build(),
        Tool.builder()
            .name("autocat_lookup")
            .description("Search the AutoCat rules sheet for matching categorization rules. AutoCat rules map transaction descriptions to categories. Check this first to see if a rule already exists for the transaction.")
            .inputSchema(
                Tool.InputSchema.builder()
                    .properties(
                        Tool.InputSchema.Properties.builder()
                            .putAdditionalProperty("description", JsonValue.from(mapOf(
                                "type" to "string",
                                "description" to "The transaction description to match against AutoCat rules",
                            )))
                            .build()
                    )
                    .addRequired("description")
                    .build()
            )
            .build(),
        Tool.builder()
            .name("submit_category")
            .description("Submit your final categorization. Call this when you have determined the category for the transaction.")
            .inputSchema(
                Tool.InputSchema.builder()
                    .properties(
                        Tool.InputSchema.Properties.builder()
                            .putAdditionalProperty("category", JsonValue.from(mapOf(
                                "type" to "string",
                                "description" to "The exact category name, or null if you cannot determine it",
                            )))
                            .putAdditionalProperty("justification", JsonValue.from(mapOf(
                                "type" to "string",
                                "description" to "Brief explanation of why this category was chosen",
                            )))
                            .build()
                    )
                    .addRequired("category")
                    .addRequired("justification")
                    .build()
            )
            .build(),
    )

    fun run() {
        val consumer = KafkaFactory.createConsumer(config, "categorizer-agent")
        val producer = KafkaFactory.createProducer(config)

        consumer.subscribe(listOf(TopicNames.UNCATEGORIZED))
        logger.info("Subscribed to {}", TopicNames.UNCATEGORIZED)

        while (true) {
            val records = consumer.poll(Duration.ofSeconds(5))
            for (record in records) {
                try {
                    val transaction = record.value()
                    if (transaction.getCategory() != null) {
                        logger.info("Transaction {} already categorized, skipping", transaction.getTransactionId())
                        continue
                    }

                    val result = categorize(transaction)
                    if (result != null) {
                        val categorized = Transaction.newBuilder(transaction)
                            .setCategory(result.category)
                            .setCategoryJustification(result.justification)
                            .build()
                        producer.send(ProducerRecord(TopicNames.CATEGORIZED, categorized.getTransactionId().toString(), categorized))
                        logger.info("Categorized '{}' as '{}' ({})", transaction.getDescription(), result.category, result.justification)
                    } else {
                        producer.send(ProducerRecord(TopicNames.CATEGORIZATION_FAILED, transaction.getTransactionId().toString(), transaction))
                        logger.warn("Could not categorize '{}'", transaction.getDescription())
                    }
                } catch (e: Exception) {
                    logger.error("Error categorizing transaction", e)
                }
            }
            consumer.commitSync()
        }
    }

    data class CategorizationResult(val category: String, val justification: String?)

    internal fun categorize(transaction: Transaction): CategorizationResult? {
        // Store messages as Any since addMessage() accepts both MessageParam and Message
        val messageHistory = mutableListOf<Any>(
            MessageParam.builder()
                .role(MessageParam.Role.USER)
                .content(
                    "Categorize this transaction:\n" +
                        "Description: ${transaction.getDescription()}\n" +
                        "Full Description: ${transaction.getFullDescription() ?: "N/A"}\n" +
                        "Amount: ${transaction.getAmount()}\n" +
                        "Account: ${transaction.getAccount()}\n" +
                        "Date: ${transaction.getDate()}"
                )
                .build()
        )

        repeat(7) { // max tool-use rounds
            val paramsBuilder = MessageCreateParams.builder()
                .model(Model.of(config.anthropicModel))
                .maxTokens(1024L)
                .system(systemPrompt)

            for (tool in tools) {
                paramsBuilder.addTool(tool)
            }

            for (msg in messageHistory) {
                when (msg) {
                    is MessageParam -> paramsBuilder.addMessage(msg)
                    is com.anthropic.models.messages.Message -> paramsBuilder.addMessage(msg)
                }
            }

            val response: com.anthropic.models.messages.Message = client.messages().create(paramsBuilder.build())

            if (response.stopReason().orElse(null) != StopReason.TOOL_USE) {
                // No tool calls — fallback to text response
                val text = response.content()
                    .firstOrNull { it.isText() }
                    ?.asText()
                    ?.text()
                    ?.trim()
                    ?: return null
                logger.warn("Agent responded with text instead of submit_category tool: {}", text)
                return if (text.equals("null", ignoreCase = true)) null
                    else CategorizationResult(category = text, justification = null)
            }

            // Check if submit_category was called — that's our final answer
            val submitCall = response.content()
                .filter { it.isToolUse() }
                .firstOrNull { it.asToolUse().name() == "submit_category" }

            if (submitCall != null) {
                @Suppress("UNCHECKED_CAST")
                val args = submitCall.asToolUse()._input().asObject().get() as Map<String, JsonValue>
                val category = args["category"]!!.asStringOrThrow()
                val justification = args["justification"]?.asStringOrThrow()
                return if (category.equals("null", ignoreCase = true)) null
                    else CategorizationResult(category = category, justification = justification)
            }

            // Add the assistant's response (with tool_use blocks) back to the conversation
            messageHistory.add(response)

            // Execute each tool call and build result blocks
            val toolResultBlocks = response.content()
                .filter { it.isToolUse() }
                .map { block ->
                    val toolUse = block.asToolUse()
                    @Suppress("UNCHECKED_CAST")
                    val args = toolUse._input().asObject().get() as Map<String, JsonValue>

                    val toolResult = when (toolUse.name()) {
                        "sheet_lookup" -> sheetLookupTool.execute(args["query"]!!.asStringOrThrow())
                        "web_search" -> webSearchTool.execute(args["query"]!!.asStringOrThrow())
                        "category_lookup" -> {
                            val category = args["category"]!!.asStringOrThrow()
                            val results = sheetsClient.searchByCategory(category)
                            logger.info("Category lookup '{}': {} transactions", category, results.size)
                            com.google.gson.Gson().toJson(results)
                        }
                        "autocat_lookup" -> {
                            val description = args["description"]!!.asStringOrThrow()
                            val results = sheetsClient.searchAutocat(description)
                            logger.info("AutoCat lookup '{}': {} rules matched", description, results.size)
                            com.google.gson.Gson().toJson(results)
                        }
                        else -> "Unknown tool: ${toolUse.name()}"
                    }

                    logger.debug("Tool '{}' args='{}' result length={}", toolUse.name(), args, toolResult.length)

                    ContentBlockParam.ofToolResult(
                        ToolResultBlockParam.builder()
                            .toolUseId(toolUse.id())
                            .content(toolResult)
                            .build()
                    )
                }

            messageHistory.add(
                MessageParam.builder()
                    .role(MessageParam.Role.USER)
                    .contentOfBlockParams(toolResultBlocks)
                    .build()
            )
        }

        logger.warn("Agent exhausted tool-use rounds for '{}'", transaction.getDescription())
        return null
    }
}
