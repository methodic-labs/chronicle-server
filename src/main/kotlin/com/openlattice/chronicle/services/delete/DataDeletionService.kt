package com.openlattice.chronicle.services.delete

import com.codahale.metrics.annotation.Timed
import com.openlattice.chronicle.data.ChronicleDeleteType
import com.openlattice.chronicle.services.enrollment.EnrollmentManager
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
@Service
class DataDeletionService(
    private val enrollmentManager: EnrollmentManager
) : DataDeletionManager {
    companion object {
        private val logger = LoggerFactory.getLogger(DataDeletionService::class.java)
    }

    // get a set of all participants to remove:
    private fun getParticipantsToDelete(
        studyId: UUID,
        participantId: Optional<String>
    ): Set<String> {

        // specific participant
        // no participant was specified, so remove all participants from entity set
        return participantId
            .map { setOf(it) }
            .orElseGet { enrollmentManager.getStudyParticipantIds(studyId) }
    }


    @Timed
    override fun deleteParticipantData(
        organizationId: UUID,
        studyId: UUID,
        participantId: String,
        chronicleDeleteType: ChronicleDeleteType,
    ) {

    }

    /**
     * This services assumes that appropriate security checks have already been enforced at controller level.
     */
    @Timed
    override fun deleteStudyData(
        organizationId: UUID,
        studyId: UUID,
        chronicleDeleteType: ChronicleDeleteType,
    ) {

        // ensure study exists
        check(enrollmentManager.studyExists(studyId)) {
            "Study $studyId in organization $organizationId does not exist."
        }

    }


}
