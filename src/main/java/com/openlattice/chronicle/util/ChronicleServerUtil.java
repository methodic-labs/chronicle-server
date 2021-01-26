package com.openlattice.chronicle.util;

import com.auth0.spring.security.api.authentication.JwtAuthentication;
import com.google.common.base.Preconditions;
import com.openlattice.chronicle.constants.*;
import com.openlattice.chronicle.data.FileType;
import com.openlattice.chronicle.services.enrollment.EnrollmentManager;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.springframework.http.MediaType;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

import javax.servlet.http.HttpServletResponse;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static com.openlattice.chronicle.constants.EdmConstants.PARTICIPANTS_PREFIX;
import static com.openlattice.chronicle.constants.EdmConstants.PERSON_ID_FQN;

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
public class ChronicleServerUtil {

    public static String ORG_STUDY_PARTICIPANT = " - orgId = {}, studyId = {}, participantId = {}";
    public static String ORG_STUDY_PARTICIPANT_DATASOURCE = " - orgId = {}, studyId = {}, participantId = {}, dataSourceId = {}";

    public static String getFirstValueOrNull( Map<FullQualifiedName, Set<Object>> entity, FullQualifiedName fqn ) {
        if ( entity.getOrDefault( fqn, Set.of() ).isEmpty() ) {
            return null;
        }
        return entity.get( fqn ).iterator().next().toString();
    }

    public static UUID getFirstUUIDOrNull( Map<FullQualifiedName, Set<Object>> entity, FullQualifiedName fqn ) {
        String firstValue = getFirstValueOrNull( entity, fqn );
        if ( firstValue == null ) {
            return null;
        }

        try {
            return UUID.fromString( firstValue );
        } catch ( IllegalArgumentException e ) {
            return null;
        }
    }

    @Deprecated(since = "apps v2")
    public static String getParticipantEntitySetName( UUID studyId ) {
        return PARTICIPANTS_PREFIX.concat( studyId.toString() );
    }

    /* --------------------------------DATA DOWNLOAD UTILS ------------------------------------ */

    public static String getParticipantDataFileName(
            EnrollmentManager enrollmentManager,
            String fileNamePrefix,
            UUID organizationId,
            UUID studyId,
            UUID participantEntityKeyId ) {
        String participantId = enrollmentManager
                .getParticipantEntity( organizationId, studyId, participantEntityKeyId )
                .get( PERSON_ID_FQN )
                .stream()
                .findFirst()
                .orElse( "" )
                .toString();
        StringBuilder fileNameBuilder = new StringBuilder()
                .append( fileNamePrefix )
                .append( LocalDate.now().format( DateTimeFormatter.BASIC_ISO_DATE ) )
                .append( "-" )
                .append( participantId );
        return fileNameBuilder.toString();
    }

    public static void setDownloadContentType( HttpServletResponse response, FileType fileType ) {

        if ( fileType == null ) {
            response.setContentType( MediaType.APPLICATION_JSON_VALUE );
            return;
        }

        switch ( fileType ) {
            case csv:
                response.setContentType( CustomMediaType.TEXT_CSV_VALUE );
                break;
            case yaml:
                response.setContentType( CustomMediaType.TEXT_YAML_VALUE );
                break;
            case json:
            default:
                response.setContentType( MediaType.APPLICATION_JSON_VALUE );
                break;
        }
    }

    public static void setContentDisposition( HttpServletResponse response, String fileName, FileType fileType ) {

        if ( fileType == FileType.csv || fileType == FileType.json ) {
            response.setHeader(
                    "Content-Disposition",
                    "attachment; filename=" + fileName + "." + fileType.toString()
            );
        }
    }

    // authentication token

    public static String getTokenFromContext() {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        return ( (JwtAuthentication) authentication ).getToken();
    }
}
