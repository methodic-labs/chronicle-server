package com.openlattice.chronicle.controllers.legacy;

import com.codahale.metrics.annotation.Timed;
import com.openlattice.chronicle.UserAuthenticatedApi;
import com.openlattice.chronicle.constants.*;
import com.openlattice.chronicle.data.ChronicleDeleteType;
import com.openlattice.chronicle.data.FileType;
import com.openlattice.chronicle.services.delete.DataDeletionManager;
import com.openlattice.chronicle.services.download.DataDownloadManager;
import com.openlattice.chronicle.services.enrollment.EnrollmentManager;
import org.springframework.http.HttpStatus;
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
@RestController
@RequestMapping( UserAuthenticatedApi.CONTROLLER )
public class UserAuthenticatedController implements UserAuthenticatedApi {

    @Inject
    private DataDeletionManager dataDeletionManager;

    @Inject
    private DataDownloadManager dataDownloadManager;

    @Inject
    private EnrollmentManager enrollmentManager;

    @Override
    @Timed
    @RequestMapping(
            path = AUTHENTICATED + STUDY_ID_PATH + PARTICIPANT_ID_PATH,
            method = RequestMethod.DELETE
    )
    public Void deleteParticipantAndAllNeighbors(
            @PathVariable( STUDY_ID ) UUID studyId,
            @PathVariable( PARTICIPANT_ID ) String participantId,
            @RequestParam( TYPE ) ChronicleDeleteType chronicleDeleteType
    ) {
        String token = getTokenFromContext();
        dataDeletionManager.deleteParticipantAndAllNeighbors( null, studyId, participantId, chronicleDeleteType, token );
        return null;
    }

    @Timed
    @RequestMapping(
            path = AUTHENTICATED + STUDY_ID_PATH,
            method = RequestMethod.DELETE
    )
    @ResponseStatus( HttpStatus.OK )
    public Void deleteStudyAndAllNeighbors(
            @PathVariable( STUDY_ID ) UUID studyId,
            @RequestParam( TYPE ) ChronicleDeleteType chronicleDeleteType
    ) {

        String token = getTokenFromContext();
        dataDeletionManager.deleteStudyAndAllNeighbors( null, studyId, chronicleDeleteType, token );
        return null;
    }

    @Override
    public Iterable<Map<String, Set<Object>>> getAllPreprocessedParticipantData(
            UUID studyId,
            UUID participantEntityKeyId,
            FileType fileType ) {

        String token = getTokenFromContext();
        return dataDownloadManager.getAllPreprocessedParticipantData( null, studyId, participantEntityKeyId, token );
    }

    @Timed
    @RequestMapping(
            path = AUTHENTICATED + PARTICIPANT_PATH + DATA_PATH + STUDY_ID_PATH
                    + ENTITY_KEY_ID_PATH
                    + PREPROCESSED_PATH,
            method = RequestMethod.GET,
            produces = { MediaType.APPLICATION_JSON_VALUE, CustomMediaType.TEXT_CSV_VALUE }
    )
    public Iterable<Map<String, Set<Object>>> getAllPreprocessedParticipantData(
            @PathVariable( STUDY_ID ) UUID studyId,
            @PathVariable( ENTITY_KEY_ID ) UUID participantEntityKeyId,
            @RequestParam( value = FILE_TYPE, required = false ) FileType fileType,
            HttpServletResponse response ) {

        Iterable<Map<String, Set<Object>>> data = getAllPreprocessedParticipantData(
                studyId,
                participantEntityKeyId,
                fileType
        );

        String fileName = getParticipantDataFileName(
                enrollmentManager,
                PREPROCESSED_DATA_PREFIX,
                null,
                studyId,
                participantEntityKeyId );
        setContentDisposition( response, fileName, fileType );
        setDownloadContentType( response, fileType );
        return data;
    }

    @Override
    public Iterable<Map<String, Set<Object>>> getAllParticipantData(
            UUID studyId,
            UUID participantEntityKeyId,
            FileType fileType ) {

        String token = getTokenFromContext();
        return dataDownloadManager.getAllParticipantData( null, studyId, participantEntityKeyId, token );
    }

    @Timed
    @RequestMapping(
            path = AUTHENTICATED + PARTICIPANT_PATH + DATA_PATH + STUDY_ID_PATH
                    + ENTITY_KEY_ID_PATH,
            method = RequestMethod.GET,
            produces = { MediaType.APPLICATION_JSON_VALUE, CustomMediaType.TEXT_CSV_VALUE }
    )
    public Iterable<Map<String, Set<Object>>> getAllParticipantData(
            @PathVariable( STUDY_ID ) UUID studyId,
            @PathVariable( ENTITY_KEY_ID ) UUID participantEntityKeyId,
            @RequestParam( value = FILE_TYPE, required = false ) FileType fileType,
            HttpServletResponse response ) {

        Iterable<Map<String, Set<Object>>> data = getAllParticipantData(
                studyId,
                participantEntityKeyId,
                fileType
        );

        String fileName = getParticipantDataFileName(
                enrollmentManager,
                RAW_DATA_PREFIX,
                null,
                studyId,
                participantEntityKeyId );
        setContentDisposition( response, fileName, fileType );
        setDownloadContentType( response, fileType );
        return data;
    }

    @Override
    public Iterable<Map<String, Set<Object>>> getAllParticipantAppsUsageData(
            UUID studyId,
            UUID participantEntityKeyId,
            FileType fileType ) {

        String token = getTokenFromContext();
        return dataDownloadManager.getAllParticipantAppsUsageData( null, studyId, participantEntityKeyId, token );
    }

    @Timed
    @RequestMapping(
            path = AUTHENTICATED + PARTICIPANT_PATH + DATA_PATH + STUDY_ID_PATH
                    + ENTITY_KEY_ID_PATH + USAGE_PATH,
            method = RequestMethod.GET,
            produces = { MediaType.APPLICATION_JSON_VALUE, CustomMediaType.TEXT_CSV_VALUE }
    )
    public Iterable<Map<String, Set<Object>>> getAllParticipantAppsUsageData(
            @PathVariable( STUDY_ID ) UUID studyId,
            @PathVariable( ENTITY_KEY_ID ) UUID participantEntityKeyId,
            @RequestParam( value = FILE_TYPE, required = false ) FileType fileType,
            HttpServletResponse response ) {

        Iterable<Map<String, Set<Object>>> data = getAllParticipantAppsUsageData(
                studyId,
                participantEntityKeyId,
                fileType );

        String fileName = getParticipantDataFileName(
                enrollmentManager,
                USAGE_DATA_PREFIX,
                null,
                studyId,
                participantEntityKeyId );
        setContentDisposition( response, fileName, fileType );
        setDownloadContentType( response, fileType );
        return data;
    }
}
