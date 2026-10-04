package org.jevy.bookkeeper.sure

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class CategoryResolverTest {

    private val client = mockk<SureClient>()

    @Test
    fun `resolves case-insensitively and trims`() {
        every { client.listCategories() } returns listOf(SureCategory("c-1", "Auto & Gas"))
        val r = CategoryResolver(client)
        assertEquals(CategoryResolution.Resolved("c-1"), r.resolve("auto & gas"))
        assertEquals(CategoryResolution.Resolved("c-1"), r.resolve("  Auto & Gas "))
    }

    @Test
    fun `caches after first fetch`() {
        every { client.listCategories() } returns listOf(SureCategory("c-1", "Groceries"))
        val r = CategoryResolver(client)
        r.resolve("Groceries")
        r.resolve("Groceries")
        verify(exactly = 1) { client.listCategories() }
    }

    @Test
    fun `duplicate folded names are ambiguous, never picked arbitrarily`() {
        every { client.listCategories() } returns listOf(SureCategory("c-1", "Groceries"), SureCategory("c-2", "groceries"))
        assertEquals(CategoryResolution.Ambiguous(listOf("c-1", "c-2")), CategoryResolver(client).resolve("Groceries"))
    }

    @Test
    fun `unknown name refreshes once then reports Unknown`() {
        every { client.listCategories() } returns listOf(SureCategory("c-1", "Groceries"))
        val r = CategoryResolver(client)
        assertEquals(CategoryResolution.Unknown, r.resolve("Not A Category"))
        verify(exactly = 2) { client.listCategories() }
    }

    @Test
    fun `a category added in Sure after startup is found on refresh`() {
        every { client.listCategories() } returnsMany listOf(
            listOf(SureCategory("c-1", "Groceries")),
            listOf(SureCategory("c-1", "Groceries"), SureCategory("c-9", "New Thing")),
        )
        assertEquals(CategoryResolution.Resolved("c-9"), CategoryResolver(client).resolve("New Thing"))
    }

    @Test
    fun `an ambiguity merged away in Sure after startup resolves on refresh`() {
        // Merging "groceries" into "Groceries" in Sure must take effect without restarting the writer.
        every { client.listCategories() } returnsMany listOf(
            listOf(SureCategory("c-1", "Groceries"), SureCategory("c-2", "groceries")),
            listOf(SureCategory("c-1", "Groceries")),
        )
        val r = CategoryResolver(client)
        assertEquals(CategoryResolution.Resolved("c-1"), r.resolve("Groceries"))
    }
}
