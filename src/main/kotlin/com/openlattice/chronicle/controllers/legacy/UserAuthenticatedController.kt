package com.openlattice.chronicle.controllers.legacy

import com.codahale.metrics.annotation.Timed
import com.openlattice.chronicle.UserAuthenticatedApi
import com.openlattice.chronicle.constants.CustomMediaType
import com.openlattice.chronicle.constants.FilenamePrefixConstants
import com.openlattice.chronicle.constants.ParticipantDataType
import com.openlattice.chronicle.data.ChronicleDeleteType
import com.openlattice.chronicle.data.FileType
import com.openlattice.chronicle.services.delete.DataDeletionManager
import com.openlattice.chronicle.services.download.DataDownloadManager
import com.openlattice.chronicle.services.enrollment.EnrollmentManager
import com.openlattice.chronicle.util.ChronicleServerUtil
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.web.bind.annotation.*
import java.util.*
import javax.inject.Inject
import javax.servlet.http.HttpServletResponse

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@RestController
@RequestMapping(UserAuthenticatedApi.CONTROLLER)
class UserAuthenticatedController : UserAuthenticatedApi {
    @Inject
    private lateinit var dataDeletionManager: DataDeletionManager

    @Inject
    private lateinit var dataDownloadManager: DataDownloadManager

    @Inject
    private lateinit var enrollmentManager: EnrollmentManager

    @Timed
    @RequestMapping(
            path = [UserAuthenticatedApi.AUTHENTICATED + UserAuthenticatedApi.STUDY_ID_PATH + UserAuthenticatedApi.PARTICIPANT_ID_PATH],
            method = [RequestMethod.DELETE]
    )
    override fun deleteParticipantAndAllNeighbors(
            @PathVariable(UserAuthenticatedApi.STUDY_ID) studyId: UUID,
            @PathVariable(UserAuthenticatedApi.PARTICIPANT_ID) participantId: String,
            @RequestParam(UserAuthenticatedApi.TYPE) chronicleDeleteType: ChronicleDeleteType
    ): Void? {
        val organizationId = enrollmentManager.getOrganizationIdForLegacyStudy( studyId )
        dataDeletionManager.deleteParticipantData(organizationId, studyId, participantId, chronicleDeleteType)
        return null
    }

    @Timed
    @RequestMapping(
            path = [UserAuthenticatedApi.AUTHENTICATED + UserAuthenticatedApi.STUDY_ID_PATH],
            method = [RequestMethod.DELETE]
    )
    @ResponseStatus(
            HttpStatus.OK
    )
    override fun deleteStudyAndAllNeighbors(
            @PathVariable(UserAuthenticatedApi.STUDY_ID) studyId: UUID,
            @RequestParam(UserAuthenticatedApi.TYPE) chronicleDeleteType: ChronicleDeleteType
    ): Void? {
        val organizationId = enrollmentManager.getOrganizationIdForLegacyStudy( studyId )
        dataDeletionManager.deleteStudyData(organizationId, studyId, chronicleDeleteType)
        return null
    }

    override fun getAllPreprocessedParticipantData(
        studyId: UUID,
        participantId: String,
        fileType: FileType
    ): Iterable<Map<String, Set<Any>>> {
        val token = ChronicleServerUtil.getTokenFromContext()
        return dataDownloadManager
                .getParticipantData(null, studyId, participantId, ParticipantDataType.PREPROCESSED, token)
    }

    @Timed
    @RequestMapping(
            path = [UserAuthenticatedApi.AUTHENTICATED + UserAuthenticatedApi.PARTICIPANT_PATH + UserAuthenticatedApi.DATA_PATH + UserAuthenticatedApi.STUDY_ID_PATH
                    + UserAuthenticatedApi.ENTITY_KEY_ID_PATH
                    + UserAuthenticatedApi.PREPROCESSED_PATH], method = [RequestMethod.GET],
            produces = [MediaType.APPLICATION_JSON_VALUE, CustomMediaType.TEXT_CSV_VALUE]
    )
    fun getAllPreprocessedParticipantData(
        @PathVariable(UserAuthenticatedApi.STUDY_ID) studyId: UUID,
        @PathVariable(UserAuthenticatedApi.ENTITY_KEY_ID) participantId: String,
        @RequestParam(value = UserAuthenticatedApi.FILE_TYPE, required = false) fileType: FileType,
        response: HttpServletResponse
    ): Iterable<Map<String, Set<Any>>> {
        val data = getAllPreprocessedParticipantData(
            studyId,
            participantId,
            fileType
        )
        val fileName = ChronicleServerUtil.getParticipantDataFileName(
            enrollmentManager,
            FilenamePrefixConstants.PREPROCESSED_DATA_PREFIX,
            null,
            studyId,
            participantId
        )
        ChronicleServerUtil.setContentDisposition(response, fileName, fileType)
        ChronicleServerUtil.setDownloadContentType(response, fileType)
        return data
    }

    override fun getAllParticipantData(
        studyId: UUID,
        participantId: String,
        fileType: FileType
    ): Iterable<Map<String, Set<Any>>> {
        val token = ChronicleServerUtil.getTokenFromContext()
        return dataDownloadManager
                .getParticipantData(null, studyId, participantId, ParticipantDataType.RAW_DATA, token)
    }

    @Timed
    @RequestMapping(
            path = [UserAuthenticatedApi.AUTHENTICATED + UserAuthenticatedApi.PARTICIPANT_PATH + UserAuthenticatedApi.DATA_PATH + UserAuthenticatedApi.STUDY_ID_PATH
                    + UserAuthenticatedApi.ENTITY_KEY_ID_PATH], method = [RequestMethod.GET],
            produces = [MediaType.APPLICATION_JSON_VALUE, CustomMediaType.TEXT_CSV_VALUE]
    )
    fun getAllParticipantData(
        @PathVariable(UserAuthenticatedApi.STUDY_ID) studyId: UUID,
        @PathVariable(UserAuthenticatedApi.ENTITY_KEY_ID) participantId: String,
        @RequestParam(value = UserAuthenticatedApi.FILE_TYPE, required = false) fileType: FileType,
        response: HttpServletResponse
    ): Iterable<Map<String, Set<Any>>> {
        val data = getAllParticipantData(
            studyId,
            participantId,
            fileType
        )
        val fileName = ChronicleServerUtil.getParticipantDataFileName(
            enrollmentManager,
            FilenamePrefixConstants.RAW_DATA_PREFIX,
            null,
            studyId,
            participantId
        )
        ChronicleServerUtil.setContentDisposition(response, fileName, fileType)
        ChronicleServerUtil.setDownloadContentType(response, fileType)
        return data
    }

    override fun getAllParticipantAppsUsageData(
        studyId: UUID,
        participantId: String,
        fileType: FileType
    ): Iterable<Map<String, Set<Any>>> {
        val token = ChronicleServerUtil.getTokenFromContext()
        return dataDownloadManager
                .getParticipantData(null, studyId, participantId, ParticipantDataType.USAGE_DATA, token)
    }

    @Timed
    @RequestMapping(
            path = [UserAuthenticatedApi.AUTHENTICATED + UserAuthenticatedApi.PARTICIPANT_PATH + UserAuthenticatedApi.DATA_PATH + UserAuthenticatedApi.STUDY_ID_PATH
                    + UserAuthenticatedApi.ENTITY_KEY_ID_PATH + UserAuthenticatedApi.USAGE_PATH],
            method = [RequestMethod.GET], produces = [MediaType.APPLICATION_JSON_VALUE, CustomMediaType.TEXT_CSV_VALUE]
    )
    fun getAllParticipantAppsUsageData(
        @PathVariable(UserAuthenticatedApi.STUDY_ID) studyId: UUID,
        @PathVariable(UserAuthenticatedApi.ENTITY_KEY_ID) participantId: String,
        @RequestParam(value = UserAuthenticatedApi.FILE_TYPE, required = false) fileType: FileType,
        response: HttpServletResponse
    ): Iterable<Map<String, Set<Any>>> {
        val data = getAllParticipantAppsUsageData(
            studyId,
            participantId,
            fileType
        )
        val fileName = ChronicleServerUtil.getParticipantDataFileName(
            enrollmentManager,
            FilenamePrefixConstants.USAGE_DATA_PREFIX,
            null,
            studyId,
            participantId
        )
        ChronicleServerUtil.setContentDisposition(response, fileName, fileType)
        ChronicleServerUtil.setDownloadContentType(response, fileType)
        return data
    }
}
