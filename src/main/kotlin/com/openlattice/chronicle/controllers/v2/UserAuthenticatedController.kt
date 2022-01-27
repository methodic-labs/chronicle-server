package com.openlattice.chronicle.controllers.v2

import com.codahale.metrics.annotation.Timed
import com.openlattice.chronicle.api.UserAuthenticatedApi
import com.openlattice.chronicle.constants.CustomMediaType
import com.openlattice.chronicle.constants.FilenamePrefixConstants
import com.openlattice.chronicle.constants.ParticipantDataType
import com.openlattice.chronicle.data.ChronicleDeleteType
import com.openlattice.chronicle.data.FileType
import com.openlattice.chronicle.services.delete.DataDeletionManager
import com.openlattice.chronicle.services.download.DataDownloadManager
import com.openlattice.chronicle.services.enrollment.EnrollmentManager
import com.openlattice.chronicle.util.ChronicleServerUtil
import org.springframework.http.MediaType
import org.springframework.web.bind.annotation.*
import java.util.*
import javax.inject.Inject
import javax.servlet.http.HttpServletResponse

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 *
 */
@RestController("userAuthenticatedControllerV2")
@RequestMapping(UserAuthenticatedApi.CONTROLLER)
class UserAuthenticatedController : UserAuthenticatedApi {
    @Inject
    private lateinit var dataDownloadManager: DataDownloadManager

    @Inject
    private lateinit var enrollmentManager: EnrollmentManager

    @Inject
    private lateinit var dataDeletionManager: DataDeletionManager

    @Timed
    @RequestMapping(
        path = [UserAuthenticatedApi.AUTHENTICATED_PATH + UserAuthenticatedApi.ORGANIZATION_ID_PATH + UserAuthenticatedApi.STUDY_ID_PATH + UserAuthenticatedApi.PARTICIPANT_ID_PATH],
        method = [RequestMethod.DELETE]
    )
    override fun deleteParticipantAndAllNeighbors(
        @PathVariable(UserAuthenticatedApi.ORGANIZATION_ID) organizationId: UUID,
        @PathVariable(UserAuthenticatedApi.STUDY_ID) studyId: UUID,
        @PathVariable(UserAuthenticatedApi.PARTICIPANT_ID) participantId: String,
        @RequestParam(UserAuthenticatedApi.TYPE) chronicleDeleteType: ChronicleDeleteType
    ): Void? {
        dataDeletionManager.deleteParticipantData(organizationId, studyId, participantId, chronicleDeleteType)
        return null
    }

    @Timed
    @RequestMapping(
        path = [UserAuthenticatedApi.AUTHENTICATED_PATH + UserAuthenticatedApi.ORGANIZATION_ID_PATH + UserAuthenticatedApi.STUDY_ID_PATH],
        method = [RequestMethod.DELETE]
    )
    override fun deleteStudyAndAllNeighbors(
        @PathVariable(UserAuthenticatedApi.ORGANIZATION_ID) organizationId: UUID,
        @PathVariable(UserAuthenticatedApi.STUDY_ID) studyId: UUID,
        @RequestParam(UserAuthenticatedApi.TYPE) chronicleDeleteType: ChronicleDeleteType
    ): Void? {
        dataDeletionManager.deleteStudyData(organizationId, studyId, chronicleDeleteType)
        return null
    }

    override fun getAllPreprocessedParticipantData(
        organizationId: UUID,
        studyId: UUID,
        participantId: String,
        fileType: FileType
    ): Iterable<Map<String, Set<Any>>> {
        val token = ChronicleServerUtil.getTokenFromContext()
        return dataDownloadManager
            .getParticipantData(
                organizationId,
                studyId,
                participantId,
                ParticipantDataType.PREPROCESSED,
                token
            )
    }

    @Timed
    @RequestMapping(
        path = [UserAuthenticatedApi.AUTHENTICATED_PATH + UserAuthenticatedApi.ORGANIZATION_ID_PATH + UserAuthenticatedApi.STUDY_ID_PATH + UserAuthenticatedApi.ENTITY_KEY_ID_PATH + UserAuthenticatedApi.DATA_PATH
                + UserAuthenticatedApi.PREPROCESSED_PATH], method = [RequestMethod.GET],
        produces = [MediaType.APPLICATION_JSON_VALUE, CustomMediaType.TEXT_CSV_VALUE]
    )
    fun getAllPreprocessedParticipantData(
        @PathVariable(UserAuthenticatedApi.ORGANIZATION_ID) organizationId: UUID,
        @PathVariable(UserAuthenticatedApi.STUDY_ID) studyId: UUID,
        @PathVariable(UserAuthenticatedApi.ENTITY_KEY_ID) participantId: String,
        @RequestParam(value = UserAuthenticatedApi.FILE_TYPE, required = false) fileType: FileType,
        response: HttpServletResponse?
    ): Iterable<Map<String, Set<Any>>> {
        val data = getAllPreprocessedParticipantData(
            organizationId,
            studyId,
            participantId,
            fileType
        )
        val fileName = ChronicleServerUtil.getParticipantDataFileName(
            enrollmentManager,
            FilenamePrefixConstants.PREPROCESSED_DATA_PREFIX,
            organizationId,
            studyId,
            participantId
        )
        ChronicleServerUtil.setContentDisposition(response, fileName, fileType)
        ChronicleServerUtil.setDownloadContentType(response, fileType)
        return data
    }

    override fun getAllParticipantData(
        organizationId: UUID,
        studyId: UUID,
        participantId: String,
        fileType: FileType
    ): Iterable<Map<String, Set<Any>>> {
        val token = ChronicleServerUtil.getTokenFromContext()
        return dataDownloadManager!!.getParticipantData(
            organizationId, studyId, participantId, ParticipantDataType.RAW_DATA, token
        )
    }

    @Timed
    @RequestMapping(
        path = [UserAuthenticatedApi.AUTHENTICATED_PATH + UserAuthenticatedApi.ORGANIZATION_ID_PATH + UserAuthenticatedApi.STUDY_ID_PATH + UserAuthenticatedApi.ENTITY_KEY_ID_PATH + UserAuthenticatedApi.DATA_PATH],
        method = [RequestMethod.GET], produces = [MediaType.APPLICATION_JSON_VALUE, CustomMediaType.TEXT_CSV_VALUE]
    )
    fun getAllParticipantData(
        @PathVariable(UserAuthenticatedApi.ORGANIZATION_ID) organizationId: UUID,
        @PathVariable(UserAuthenticatedApi.STUDY_ID) studyId: UUID,
        @PathVariable(UserAuthenticatedApi.ENTITY_KEY_ID) participantId: String,
        @RequestParam(value = UserAuthenticatedApi.FILE_TYPE, required = false) fileType: FileType,
        response: HttpServletResponse?
    ): Iterable<Map<String, Set<Any>>> {
        val data = getAllParticipantData(
            organizationId,
            studyId,
            participantId,
            fileType
        )
        val fileName = ChronicleServerUtil.getParticipantDataFileName(
            enrollmentManager,
            FilenamePrefixConstants.RAW_DATA_PREFIX,
            organizationId,
            studyId,
            participantId
        )
        ChronicleServerUtil.setContentDisposition(response, fileName, fileType)
        ChronicleServerUtil.setDownloadContentType(response, fileType)
        return data
    }

    override fun getAllParticipantAppsUsageData(
        organizationId: UUID, studyId: UUID, participantId: String, fileType: FileType
    ): Iterable<Map<String, Set<Any>>> {
        val token = ChronicleServerUtil.getTokenFromContext()
        return dataDownloadManager
            .getParticipantData(
                organizationId, studyId, participantId, ParticipantDataType.USAGE_DATA, token
            )
    }

    @Timed
    @RequestMapping(
        path = [UserAuthenticatedApi.AUTHENTICATED_PATH + UserAuthenticatedApi.ORGANIZATION_ID_PATH + UserAuthenticatedApi.STUDY_ID_PATH + UserAuthenticatedApi.ENTITY_KEY_ID_PATH + UserAuthenticatedApi.DATA_PATH
                + UserAuthenticatedApi.USAGE_PATH], method = [RequestMethod.GET],
        produces = [MediaType.APPLICATION_JSON_VALUE, CustomMediaType.TEXT_CSV_VALUE]
    )
    fun getAllParticipantAppsUsageData(
        @PathVariable(UserAuthenticatedApi.ORGANIZATION_ID) organizationId: UUID,
        @PathVariable(UserAuthenticatedApi.STUDY_ID) studyId: UUID,
        @PathVariable(UserAuthenticatedApi.ENTITY_KEY_ID) participantId: String,
        @RequestParam(value = UserAuthenticatedApi.FILE_TYPE, required = false) fileType: FileType,
        response: HttpServletResponse?
    ): Iterable<Map<String, Set<Any>>> {
        val data = getAllParticipantAppsUsageData(
            organizationId,
            studyId,
            participantId,
            fileType
        )
        val fileName = ChronicleServerUtil.getParticipantDataFileName(
            enrollmentManager,
            FilenamePrefixConstants.USAGE_DATA_PREFIX,
            organizationId,
            studyId,
            participantId
        )
        ChronicleServerUtil.setContentDisposition(response, fileName, fileType)
        ChronicleServerUtil.setDownloadContentType(response, fileType)
        return data
    }
}
