/*
 * Copyright (C) 2018. OpenLattice, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@openlattice.com
 */

package com.openlattice.chronicle.controllers.legacy;

import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Optional;
import com.openlattice.chronicle.ChronicleStudyApi;
import com.openlattice.chronicle.data.ChronicleAppsUsageDetails;
import com.openlattice.chronicle.data.ChronicleQuestionnaire;
import com.openlattice.chronicle.data.ParticipationStatus;
import com.openlattice.chronicle.services.enrollment.EnrollmentManager;
import com.openlattice.chronicle.services.surveys.SurveysManager;
import com.openlattice.chronicle.sources.Datasource;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@RestController
@RequestMapping( ChronicleStudyApi.CONTROLLER )
public class ChronicleStudyController implements ChronicleStudyApi {

    @Inject
    private SurveysManager surveysManager;

    @Inject
    private EnrollmentManager enrollmentManager;

    @Override
    @Timed
    @RequestMapping(
            path = STUDY_ID_PATH + PARTICIPANT_ID_PATH + DATASOURCE_ID_PATH,
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE
    )
    public UUID enrollSource(
            @PathVariable( STUDY_ID ) UUID studyId,
            @PathVariable( PARTICIPANT_ID ) String participantId,
            @PathVariable( DATASOURCE_ID ) String datasourceId,
            @RequestBody Optional<Datasource> datasource ) {

        return enrollmentManager.registerDataSource( null, studyId, participantId, datasourceId, datasource );
    }

    @Override
    @Timed
    @RequestMapping(
            path = STUDY_ID_PATH + PARTICIPANT_ID_PATH + DATASOURCE_ID_PATH,
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE
    )
    public Boolean isKnownDatasource(
            @PathVariable( STUDY_ID ) UUID studyId,
            @PathVariable( PARTICIPANT_ID ) String participantId,
            @PathVariable( DATASOURCE_ID ) String datasourceId ) {
        //  validate that this device belongs to this participant in this study
        //  look up in association entitySet between device and participant, and device and study to see if it exists?
        //  DataApi.getEntity(entitySetId :UUID, entityKeyId :UUID)
        return enrollmentManager.isKnownDatasource( null, studyId, participantId, datasourceId );
    }

    @Timed
    @Override
    @RequestMapping(
            path = PARTICIPANT_PATH + DATA_PATH + STUDY_ID_PATH + PARTICIPANT_ID_PATH + APPS,
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE
    )
    public List<ChronicleAppsUsageDetails> getParticipantAppsUsageData(
            @PathVariable( STUDY_ID ) UUID studyId,
            @PathVariable( PARTICIPANT_ID ) String participantId,
            @RequestParam( value = DATE ) String date ) {
        return surveysManager.getParticipantAppsUsageData( null, studyId, participantId, date );
    }

    @Override
    @Timed
    @RequestMapping(
            path = STUDY_ID_PATH + NOTIFICATIONS,
            method = RequestMethod.GET
    )
    public Boolean isNotificationsEnabled(
            @PathVariable( STUDY_ID ) UUID studyId ) {
        return enrollmentManager.isNotificationsEnabled( null, studyId );
    }

    @Override
    @Timed
    @RequestMapping(
            path = STUDY_ID_PATH + PARTICIPANT_ID_PATH + ENROLLMENT_STATUS,
            method = RequestMethod.GET
    )
    public ParticipationStatus getParticipationStatus(
            @PathVariable( STUDY_ID ) UUID studyId,
            @PathVariable( PARTICIPANT_ID ) String participantId ) {
        return enrollmentManager.getParticipationStatus( null, studyId, participantId );
    }

    @Override
    @RequestMapping(
            path = PARTICIPANT_PATH + DATA_PATH + STUDY_ID_PATH + PARTICIPANT_ID_PATH + APPS,
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE
    )
    public void submitAppUsageSurvey(
            @PathVariable( STUDY_ID ) UUID studyId,
            @PathVariable( PARTICIPANT_ID ) String participantId,
            @RequestBody Map<UUID, Map<FullQualifiedName, Set<Object>>> associationDetails ) {

        surveysManager.submitAppUsageSurvey( null, studyId, participantId, associationDetails );
    }

    @Override
    @Timed
    @RequestMapping(
            path = STUDY_ID_PATH + QUESTIONNAIRE + ENTITY_KEY_ID_PATH,
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE
    )
    public ChronicleQuestionnaire getChronicleQuestionnaire(
            @PathVariable( STUDY_ID ) UUID studyId,
            @PathVariable( ENTITY_KEY_ID ) UUID questionnaireEKID
    ) {
        return surveysManager.getQuestionnaire( null, studyId, questionnaireEKID );
    }

    @Override
    @Timed
    @RequestMapping(
            path = STUDY_ID_PATH + PARTICIPANT_ID_PATH + QUESTIONNAIRE,
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE
    )
    public void submitQuestionnaire(
            @PathVariable( STUDY_ID ) UUID studyId,
            @PathVariable( PARTICIPANT_ID ) String participantId,
            @RequestBody Map<UUID, Map<FullQualifiedName, Set<Object>>> questionnaireResponses ) {

        surveysManager.submitQuestionnaire( null, studyId, participantId, questionnaireResponses );
    }

    @Override
    @Timed
    @RequestMapping(
            path = STUDY_ID_PATH + QUESTIONNAIRES,
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE
    )
    public Map<UUID, Map<FullQualifiedName, Set<Object>>> getStudyQuestionnaires(
            @PathVariable( STUDY_ID ) UUID studyId ) {

        return surveysManager.getStudyQuestionnaires( null, studyId );
    }

    @RequestMapping(
            path = STUDY_ID_PATH + PARTICIPANT_ID_PATH + TIME_USE_DIARY,
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE
    )
    @Override
    public void submitTimeUseDiarySurvey(
            @PathVariable ( STUDY_ID ) UUID studyId,
            @PathVariable ( PARTICIPANT_ID ) String participantId,
            @RequestBody List<Map<FullQualifiedName, Set<Object>>> surveyData
    ) {
        surveysManager.submitTimeUseDiarySurvey( null, studyId, participantId, surveyData );
    }
}
