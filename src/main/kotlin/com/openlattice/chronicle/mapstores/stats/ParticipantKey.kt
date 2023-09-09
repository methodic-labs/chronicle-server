package com.openlattice.chronicle.mapstores.stats

import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@getmethodic.com&gt;
 */
data class ParticipantKey(
    val studyId: UUID,
    val participantId: String
)