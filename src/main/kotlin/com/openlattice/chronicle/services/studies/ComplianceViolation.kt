package com.openlattice.chronicle.services.studies

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@getmethodic.com&gt;
 */
data class ComplianceViolation(
    val reason: ViolationReason,
    val description: String
)

enum class ViolationReason {
    NO_DATA_UPLOADED
}

