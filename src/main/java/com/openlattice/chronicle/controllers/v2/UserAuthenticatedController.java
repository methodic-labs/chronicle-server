package com.openlattice.chronicle.controllers.v2;

import com.codahale.metrics.annotation.Timed;
import com.openlattice.chronicle.api.UserAuthenticatedApi;
import com.openlattice.chronicle.constants.*;
import com.openlattice.chronicle.data.ChronicleDeleteType;
import com.openlattice.chronicle.data.FileType;
import com.openlattice.chronicle.services.delete.DataDeletionManager;
import com.openlattice.chronicle.services.download.DataDownloadManager;
import com.openlattice.chronicle.services.enrollment.EnrollmentManager;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import javax.inject.Inject;
import javax.servlet.http.HttpServletResponse;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static com.openlattice.chronicle.constants.FilenamePrefixConstants.PREPROCESSED_DATA_PREFIX;
import static com.openlattice.chronicle.constants.FilenamePrefixConstants.RAW_DATA_PREFIX;
import static com.openlattice.chronicle.constants.FilenamePrefixConstants.USAGE_DATA_PREFIX;
import static com.openlattice.chronicle.util.ChronicleServerUtil.getParticipantDataFileName;
import static com.openlattice.chronicle.util.ChronicleServerUtil.getTokenFromContext;
import static com.openlattice.chronicle.util.ChronicleServerUtil.setContentDisposition;
import static com.openlattice.chronicle.util.ChronicleServerUtil.setDownloadContentType;

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
@RestController( "userAuthenticatedControllerV2" )
@RequestMapping( UserAuthenticatedApi.CONTROLLER )
public class UserAuthenticatedController implements UserAuthenticatedApi {

    @Inject
    private DataDownloadManager dataDownloadManager;

    @Inject
    private EnrollmentManager enrollmentManager;

    @Inject
    private DataDeletionManager dataDeletionManager;

    @Override
    @Timed
    @RequestMapping(
            path = AUTHENTICATED_PATH + ORGANIZATION_ID_PATH + STUDY_ID_PATH + PARTICIPANT_ID_PATH,
            method = RequestMethod.DELETE
    )
    public Void deleteParticipantAndAllNeighbors(
            @PathVariable( ORGANIZATION_ID ) UUID organizationId,
            @PathVariable( STUDY_ID ) UUID studyId,
            @PathVariable( PARTICIPANT_ID ) String participantId,
            @RequestParam( TYPE ) ChronicleDeleteType chronicleDeleteType
    ) {

        String token = getTokenFromContext();
        dataDeletionManager
                .deleteParticipantAndAllNeighbors( organizationId, studyId, participantId, chronicleDeleteType, token );

        return null;
    }

    @Override
    @Timed
    @RequestMapping(
            path = AUTHENTICATED_PATH + ORGANIZATION_ID_PATH + STUDY_ID_PATH,
            method = RequestMethod.DELETE
    )
    public Void deleteStudyAndAllNeighbors(
            @PathVariable( ORGANIZATION_ID ) UUID organizationId,
            @PathVariable( STUDY_ID ) UUID studyId,
            @RequestParam( TYPE ) ChronicleDeleteType chronicleDeleteType
    ) {

        String token = getTokenFromContext();
        dataDeletionManager.deleteStudyAndAllNeighbors( organizationId, studyId, chronicleDeleteType, token );

        return null;
    }

    @Override
    public Iterable<Map<String, Set<Object>>> getAllPreprocessedParticipantData(
            UUID organizationId,
            UUID studyId,
            UUID participantEntityKeyId,
            FileType fileType ) {

        String token = getTokenFromContext();
        return dataDownloadManager
                .getAllPreprocessedParticipantData( organizationId, studyId, participantEntityKeyId, token );
    }

    @Timed
    @RequestMapping(
            path = AUTHENTICATED_PATH + PARTICIPANT_PATH + DATA_PATH + ORGANIZATION_ID_PATH + STUDY_ID_PATH
                    + ENTITY_KEY_ID_PATH
                    + PREPROCESSED_PATH,
            method = RequestMethod.GET,
            produces = { MediaType.APPLICATION_JSON_VALUE, CustomMediaType.TEXT_CSV_VALUE }
    )
    public Iterable<Map<String, Set<Object>>> getAllPreprocessedParticipantData(
            @PathVariable( ORGANIZATION_ID ) UUID organizationId,
            @PathVariable( STUDY_ID ) UUID studyId,
            @PathVariable( ENTITY_KEY_ID ) UUID participantEntityKeyId,
            @RequestParam( value = FILE_TYPE, required = false ) FileType fileType,
            HttpServletResponse response ) {

        Iterable<Map<String, Set<Object>>> data = getAllPreprocessedParticipantData(
                organizationId,
                studyId,
                participantEntityKeyId,
                fileType
        );

        String fileName = getParticipantDataFileName(
                enrollmentManager,
                PREPROCESSED_DATA_PREFIX,
                organizationId,
                studyId,
                participantEntityKeyId );
        setContentDisposition( response, fileName, fileType );
        setDownloadContentType( response, fileType );
        return data;
    }

    @Override
    public Iterable<Map<String, Set<Object>>> getAllParticipantData(
            UUID organizationId,
            UUID studyId,
            UUID participantEntityKeyId,
            FileType fileType ) {

        String token = getTokenFromContext();
        return dataDownloadManager.getAllParticipantData( organizationId, studyId, participantEntityKeyId, token );
    }

    @Timed
    @RequestMapping(
            path = AUTHENTICATED_PATH + PARTICIPANT_PATH + DATA_PATH + ORGANIZATION_ID_PATH + STUDY_ID_PATH
                    + ENTITY_KEY_ID_PATH,
            method = RequestMethod.GET,
            produces = { MediaType.APPLICATION_JSON_VALUE, CustomMediaType.TEXT_CSV_VALUE }
    )
    public Iterable<Map<String, Set<Object>>> getAllParticipantData(
            @PathVariable( ORGANIZATION_ID ) UUID organizationId,
            @PathVariable( STUDY_ID ) UUID studyId,
            @PathVariable( ENTITY_KEY_ID ) UUID participantEntityKeyId,
            @RequestParam( value = FILE_TYPE, required = false ) FileType fileType,
            HttpServletResponse response ) {

        Iterable<Map<String, Set<Object>>> data = getAllParticipantData(
                organizationId,
                studyId,
                participantEntityKeyId,
                fileType
        );

        String fileName = getParticipantDataFileName(
                enrollmentManager,
                RAW_DATA_PREFIX,
                organizationId,
                studyId,
                participantEntityKeyId );
        setContentDisposition( response, fileName, fileType );
        setDownloadContentType( response, fileType );
        return data;
    }

    @Override
    public Iterable<Map<String, Set<Object>>> getAllParticipantAppsUsageData(
            UUID organizationId, UUID studyId, UUID participantEntityKeyId, FileType fileType ) {

        String token = getTokenFromContext();
        return dataDownloadManager
                .getAllParticipantAppsUsageData( organizationId, studyId, participantEntityKeyId, token );
    }

    @Timed
    @RequestMapping(
            path = AUTHENTICATED_PATH + PARTICIPANT_PATH + DATA_PATH + ORGANIZATION_ID_PATH + STUDY_ID_PATH
                    + ENTITY_KEY_ID_PATH + USAGE_PATH,
            method = RequestMethod.GET,
            produces = { MediaType.APPLICATION_JSON_VALUE, CustomMediaType.TEXT_CSV_VALUE }
    )
    public Iterable<Map<String, Set<Object>>> getAllParticipantAppsUsageData(
            @PathVariable( ORGANIZATION_ID ) UUID organizationId,
            @PathVariable( STUDY_ID ) UUID studyId,
            @PathVariable( ENTITY_KEY_ID ) UUID participantEntityKeyId,
            @RequestParam( value = FILE_TYPE, required = false ) FileType fileType,
            HttpServletResponse response ) {

        Iterable<Map<String, Set<Object>>> data = getAllParticipantAppsUsageData(
                organizationId,
                studyId,
                participantEntityKeyId,
                fileType );

        String fileName = getParticipantDataFileName(
                enrollmentManager,
                USAGE_DATA_PREFIX,
                organizationId,
                studyId,
                participantEntityKeyId );
        setContentDisposition( response, fileName, fileType );
        setDownloadContentType( response, fileType );
        return data;
    }

}
