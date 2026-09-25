package org.jevy.bookkeeper.sure

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.math.BigDecimal
import kotlin.test.assertEquals

class AmountConventionTest {

    @Test
    fun `parses negative dollar expense`() {
        assertEquals(BigDecimal("-384.91"), AmountConvention.parseSheetAmount("-\$384.91"))
    }

    @Test
    fun `parses positive income with thousands separator`() {
        assertEquals(BigDecimal("1865.61"), AmountConvention.parseSheetAmount("\$1,865.61"))
    }

    @Test
    fun `parses zero`() {
        assertEquals(0, BigDecimal.ZERO.compareTo(AmountConvention.parseSheetAmount("\$0.00")))
    }

    @Test
    fun `parses a plain decimal without a dollar sign`() {
        assertEquals(BigDecimal("10"), AmountConvention.parseSheetAmount("10"))
    }

    @Test
    fun `parses with surrounding whitespace`() {
        assertEquals(BigDecimal("-12.50"), AmountConvention.parseSheetAmount("  -\$12.50 "))
    }

    @Test
    fun `malformed input throws rather than returning zero`() {
        assertThrows<IllegalArgumentException> { AmountConvention.parseSheetAmount("abc") }
        assertThrows<IllegalArgumentException> { AmountConvention.parseSheetAmount("") }
    }

    @Test
    fun `toCents keeps sign and rounds half up`() {
        assertEquals(-38491L, AmountConvention.toCents(BigDecimal("-384.91")))
        assertEquals(137124L, AmountConvention.toCents(BigDecimal("1371.24")))
        assertEquals(1000L, AmountConvention.toCents(BigDecimal("10")))
    }

    @Test
    fun `sure entry amount is the negation of the sheet amount in both directions`() {
        assertEquals(BigDecimal("384.91"), AmountConvention.toSureEntryAmount(BigDecimal("-384.91")))
        assertEquals(BigDecimal("-1371.24"), AmountConvention.toSureEntryAmount(BigDecimal("1371.24")))
    }
}
