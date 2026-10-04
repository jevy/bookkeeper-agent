package org.jevy.bookkeeper.sure

sealed interface CategoryResolution {
    data class Resolved(val id: String) : CategoryResolution
    /** More than one Sure category folds to the same name (e.g. "Groceries" and "groceries"). */
    data class Ambiguous(val ids: List<String>) : CategoryResolution
    object Unknown : CategoryResolution
}

/**
 * Sure category name to id. Case-insensitive. Fetched once, refreshed on a miss or an
 * ambiguity so categories created or merged in Sure after startup are picked up. Never
 * creates categories.
 */
class CategoryResolver(private val client: SureClient) {

    private var byFoldedName: Map<String, List<String>>? = null

    private fun fold(name: String) = name.trim().lowercase()

    private fun load(): Map<String, List<String>> =
        client.listCategories().groupBy({ fold(it.name) }, { it.id }).also { byFoldedName = it }

    @Synchronized
    fun resolve(name: String): CategoryResolution {
        val key = fold(name)
        val cached = (byFoldedName ?: load())[key]
        // A miss or an ambiguity may be stale: the category may since have been created, or the
        // duplicates merged, in Sure. Re-fetch once before giving a verdict.
        val ids = cached?.takeIf { it.size == 1 } ?: load()[key] ?: return CategoryResolution.Unknown
        return if (ids.size == 1) CategoryResolution.Resolved(ids[0]) else CategoryResolution.Ambiguous(ids)
    }
}
