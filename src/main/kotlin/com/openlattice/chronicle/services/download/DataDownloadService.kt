package com.openlattice.chronicle.services.download

import com.openlattice.chronicle.constants.ParticipantDataType
import org.slf4j.LoggerFactory
import java.util.*

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class DataDownloadService() : DataDownloadManager {
    companion object {
        private val logger = LoggerFactory.getLogger(DataDownloadService::class.java)
    }

    private fun getParticipantDataHelper(
            organizationId: UUID?,
            studyId: UUID,
            participantEKID: UUID,
            dataType: ParticipantDataType,
            token: String?): Iterable<Map<String, Set<Any>>> {

        return try {
            val srcPropertiesToInclude = DownloadTypePropertyTypeFqns.SRC.getValue(dataType)
            val edgePropertiesToInclude = DownloadTypePropertyTypeFqns.EDGE.getValue(dataType)
            TODO("Not yet implemented")

        } catch (e: Exception) {
            // since the response is meant to be a file download, returning "null" will respond with 200 and return
            // an empty file, which is not what we want. the request should not "succeed" when something goes wrong
            // internally. additionally, it doesn't seem right for the request to return a stacktrace. instead,
            // catching all exceptions and throwing a general exception here will result in a failed request with
            // a simple error message to indicate something went wrong during the file download.
            logger.error("failed to download data for participant {}", participantEKID, e)
            throw RuntimeException("failed to download participant data")
        }
    }

    override fun getParticipantData(
            organizationId: UUID?,
            studyId: UUID,
            participantEntityId: UUID,
            dataType: ParticipantDataType,
            token: String?
    ): Iterable<Map<String, Set<Any>>> {

        return getParticipantDataHelper(
                organizationId,
                studyId,
                participantEntityId,
                dataType,
                token
        )
    }



}