package com.openlattice.chronicle.services.surveys;

import com.google.common.base.Preconditions;
import com.google.common.collect.*;
import com.openlattice.chronicle.data.ChronicleAppsUsageDetails;
import com.openlattice.chronicle.data.ChronicleQuestionnaire;
import com.openlattice.chronicle.services.ApiCacheManager;
import com.openlattice.chronicle.services.ScheduledTasksManager;
import com.openlattice.chronicle.services.edm.EdmCacheManager;
import com.openlattice.chronicle.services.enrollment.EnrollmentManager;
import com.openlattice.client.ApiClient;
import com.openlattice.data.DataApi;
import com.openlattice.data.DataAssociation;
import com.openlattice.data.DataGraph;
import com.openlattice.data.UpdateType;
import com.openlattice.data.requests.NeighborEntityDetails;
import com.openlattice.search.SearchApi;
import com.openlattice.search.requests.EntityNeighborsFilter;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static com.openlattice.chronicle.constants.AppComponent.CHRONICLE;
import static com.openlattice.chronicle.constants.AppComponent.CHRONICLE_DATA_COLLECTION;
import static com.openlattice.chronicle.constants.AppComponent.CHRONICLE_SURVEYS;
import static com.openlattice.chronicle.constants.CollectionTemplateTypeName.ADDRESSES;
import static com.openlattice.chronicle.constants.CollectionTemplateTypeName.ANSWER;
import static com.openlattice.chronicle.constants.CollectionTemplateTypeName.PART_OF;
import static com.openlattice.chronicle.constants.CollectionTemplateTypeName.QUESTION;
import static com.openlattice.chronicle.constants.CollectionTemplateTypeName.RESPONDS_WITH;
import static com.openlattice.chronicle.constants.CollectionTemplateTypeName.STUDIES;
import static com.openlattice.chronicle.constants.CollectionTemplateTypeName.SURVEY;
import static com.openlattice.chronicle.constants.CollectionTemplateTypeName.USED_BY;
import static com.openlattice.chronicle.constants.CollectionTemplateTypeName.USER_APPS;
import static com.openlattice.chronicle.constants.EdmConstants.ADDRESSES_ES;
import static com.openlattice.chronicle.constants.EdmConstants.ANSWERS_ES;
import static com.openlattice.chronicle.constants.EdmConstants.COMPLETED_DATE_TIME_FQN;
import static com.openlattice.chronicle.constants.EdmConstants.DATE_TIME_FQN;
import static com.openlattice.chronicle.constants.EdmConstants.PART_OF_ES;
import static com.openlattice.chronicle.constants.EdmConstants.QUESTIONNAIRE_ES;
import static com.openlattice.chronicle.constants.EdmConstants.QUESTIONS_ES;
import static com.openlattice.chronicle.constants.EdmConstants.RESPONDS_WITH_ES;
import static com.openlattice.chronicle.constants.EdmConstants.STUDY_ES;
import static com.openlattice.chronicle.constants.EdmConstants.USED_BY_ES;
import static com.openlattice.chronicle.constants.EdmConstants.USER_APPS_ES;
import static com.openlattice.chronicle.constants.EdmConstants.VALUES_FQN;
import static com.openlattice.chronicle.util.ChronicleServerUtil.checkNotNullUUIDs;
import static com.openlattice.chronicle.util.ChronicleServerUtil.getFirstUUIDOrNull;
import static com.openlattice.edm.EdmConstants.ID_FQN;

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
public class SurveysService implements SurveysManager {
    protected static final Logger logger = LoggerFactory.getLogger( SurveysService.class );

    private final ApiCacheManager       apiCacheManager;
    private final EnrollmentManager     enrollmentManager;
    private final ScheduledTasksManager scheduledTasksManager;
    private final EdmCacheManager       edmCacheManager;

    public SurveysService(
            ApiCacheManager apiCacheManager,
            EdmCacheManager edmCacheManager,
            EnrollmentManager enrollmentManager,
            ScheduledTasksManager scheduledTasksManager ) {

        this.edmCacheManager = edmCacheManager;
        this.apiCacheManager = apiCacheManager;
        this.enrollmentManager = enrollmentManager;
        this.scheduledTasksManager = scheduledTasksManager;
    }

    @Override
    public ChronicleQuestionnaire getQuestionnaire(
            UUID organizationId, UUID studyId, UUID questionnaireEKID ) {
        try {
            logger.info( "Retrieving questionnaire: orgId = {} studyId = {}, questionnaire EKID = {}",
                    organizationId,
                    studyId,
                    questionnaireEKID );

            UUID studyEKID = Preconditions
                    .checkNotNull( enrollmentManager.getStudyEntityKeyId( organizationId, studyId ),
                            "invalid study: " + studyId );

            // get apis
            ApiClient apiClient = apiCacheManager.prodApiClientCache.get( ApiClient.class );
            SearchApi searchApi = apiClient.getSearchApi();

            // entity set ids
            UUID questionnaireESID = edmCacheManager
                    .getEntitySetId( organizationId, CHRONICLE_SURVEYS, SURVEY, QUESTIONNAIRE_ES );
            UUID studiesESID = edmCacheManager.getEntitySetId( organizationId, CHRONICLE, STUDIES, STUDY_ES );
            UUID partOfESID = edmCacheManager.getEntitySetId( organizationId, CHRONICLE, PART_OF, PART_OF_ES );
            UUID questionESID = edmCacheManager
                    .getEntitySetId( organizationId, CHRONICLE_SURVEYS, QUESTION, QUESTIONS_ES );

            checkNotNullUUIDs( ImmutableSet.of( questionnaireESID, studiesESID, partOfESID, questionESID ) );

            // Get questionnaires that neighboring study
            Map<UUID, List<NeighborEntityDetails>> neighbors = searchApi.executeFilteredEntityNeighborSearch(
                    studiesESID,
                    new EntityNeighborsFilter(
                            ImmutableSet.of( studyEKID ),
                            Optional.of( ImmutableSet.of( questionnaireESID ) ),
                            Optional.of( ImmutableSet.of( studiesESID ) ),
                            Optional.of( ImmutableSet.of( partOfESID ) )
                    )
            );

            if ( !neighbors.containsKey( studyEKID ) ) {
                throw new IllegalArgumentException( "questionnaire not found" );
            }

            // find questionnaire entity matching given entity key id
            ChronicleQuestionnaire questionnaire = new ChronicleQuestionnaire();

            neighbors.get( studyEKID )
                    .stream()
                    .filter( neighbor -> questionnaireEKID
                            .equals( getFirstUUIDOrNull( neighbor.getNeighborDetails().orElse( Map.of() ),
                                    ID_FQN ) ) )
                    .map( neighbor -> neighbor.getNeighborDetails().get() )
                    .findFirst() // If a study has multiple questionnaires, we are only interested in the one with a matching EKID
                    .ifPresent( questionnaire::setQuestionnaireDetails );

            if ( questionnaire.getQuestionnaireDetails() == null ) {
                logger.info( "questionnaire does not exist - studyId: {}, questionnaireEKID: {}, neighbors: {}",
                        studyId,
                        questionnaireEKID,
                        neighbors.size() );
                throw new IllegalArgumentException(
                        "questionnaire does not exist, studyId: " + studyId + "questionnaire EKID = "
                                + questionnaireEKID );
            }
            logger.info( "retrieved questionnaire: {}", questionnaire.getQuestionnaireDetails().toString() );

            // get questions neighboring questionnaire
            neighbors = searchApi.executeFilteredEntityNeighborSearch(
                    questionnaireESID,
                    new EntityNeighborsFilter(
                            ImmutableSet.of( questionnaireEKID ),
                            Optional.of( ImmutableSet.of( questionESID ) ),
                            Optional.of( ImmutableSet.of( questionnaireESID ) ),
                            Optional.of( ImmutableSet.of( partOfESID ) )
                    )
            );

            List<Map<FullQualifiedName, Set<Object>>> questions = neighbors
                    .getOrDefault( questionnaireEKID, List.of() )
                    .stream()
                    .filter( neighbor -> neighbor.getNeighborDetails().isPresent() )
                    .map( neighbor -> neighbor.getNeighborDetails().get() )
                    .collect( Collectors.toList() );

            questionnaire.setQuestions( questions );

            logger.info( "retrieved {} questions associated with questionnaire {}",
                    questions.size(),
                    questionnaireEKID );

            return questionnaire;

        } catch ( Exception e ) {
            // catch all errors encountered during execution
            logger.error( "unable to retrieve questionnaire: studyId = {}, questionnaire = {}",
                    studyId,
                    questionnaireEKID );
            throw new RuntimeException( "questionnaire not found" );
        }
    }

    @Override
    public Map<UUID, Map<FullQualifiedName, Set<Object>>> getStudyQuestionnaires(
            UUID organizationId, UUID studyId ) {
        try {
            logger.info( "Retrieving questionnaires for study :{}", studyId );

            // If an organization does not have the chronicle_surveys app installed, exit early
            if ( edmCacheManager.getEntitySetIdsByOrgId().get( CHRONICLE_SURVEYS )
                    .containsKey( organizationId ) ) {
                logger.warn( "No questionnaires found for study {}. {} is not installed on organization with id {}. ",
                        studyId,
                        CHRONICLE_SURVEYS.toString(),
                        organizationId );
                return ImmutableMap.of();
            }

            // check if study is valid
            UUID studyEntityKeyId = Preconditions
                    .checkNotNull( enrollmentManager.getStudyEntityKeyId( organizationId, studyId ),
                            "invalid studyId: " + studyId );

            // load apis
            ApiClient apiClient = apiCacheManager.prodApiClientCache.get( ApiClient.class );
            SearchApi searchApi = apiClient.getSearchApi();

            // entity set ids
            UUID questionnaireESID = edmCacheManager
                    .getEntitySetId( organizationId, CHRONICLE_SURVEYS, SURVEY, QUESTIONNAIRE_ES );
            UUID studiesESID = edmCacheManager.getEntitySetId( organizationId, CHRONICLE, STUDIES, STUDY_ES );
            UUID partOfESID = edmCacheManager.getEntitySetId( organizationId, CHRONICLE, PART_OF, PART_OF_ES );

            checkNotNullUUIDs( ImmutableSet.of( questionnaireESID, studiesESID, partOfESID ) );

            // filtered search on questionnaires ES to get neighbors of study
            Map<UUID, List<NeighborEntityDetails>> neighbors = searchApi
                    .executeFilteredEntityNeighborSearch(
                            studiesESID,
                            new EntityNeighborsFilter(
                                    Set.of( studyEntityKeyId ),
                                    Optional.of( Set.of( questionnaireESID ) ),
                                    Optional.of( Set.of( studiesESID ) ),
                                    Optional.of( Set.of( partOfESID ) )
                            )
                    );

            // create a mapping from entity key id -> entity details
            List<NeighborEntityDetails> studyQuestionnaires = neighbors.getOrDefault( studyEntityKeyId, List.of() );
            Map<UUID, Map<FullQualifiedName, Set<Object>>> result = studyQuestionnaires
                    .stream()
                    .filter( neighbor -> neighbor.getNeighborDetails().isPresent() )
                    .map( neighbor -> neighbor.getNeighborDetails().get() )
                    .collect( Collectors.toMap(
                            neighbor -> getFirstUUIDOrNull( neighbor, ID_FQN ),
                            neighbor -> neighbor
                    ) );

            logger.info( "found {} questionnaires for study {}", result.size(), studyId );
            return result;

        } catch ( Exception e ) {
            logger.error( "failed to get questionnaires for study {}", studyId, e );
            throw new RuntimeException( "failed to get questionnaires" );
        }
    }

    @Override
    public void submitQuestionnaire(
            UUID organizationId,
            UUID studyId,
            String participantId,
            Map<UUID, Map<FullQualifiedName, Set<Object>>> questionnaireResponses ) {

        try {
            logger.info( "submitting questionnaire: studyId = {}, participantId = {}", studyId, participantId );

            ApiClient apiClient = apiCacheManager.prodApiClientCache.get( ApiClient.class );
            DataApi dataApi = apiClient.getDataApi();

            // get entity set ids
            UUID participantESID = edmCacheManager.getParticipantEntitySetId( organizationId, studyId );
            UUID answersESID = edmCacheManager
                    .getEntitySetId( organizationId, CHRONICLE_SURVEYS, ANSWER, ANSWERS_ES );
            UUID respondsWithESID = edmCacheManager.getEntitySetId( organizationId,
                    CHRONICLE_SURVEYS,
                    RESPONDS_WITH,
                    RESPONDS_WITH_ES );
            UUID addressesESID = edmCacheManager
                    .getEntitySetId( organizationId, CHRONICLE_SURVEYS, ADDRESSES, ADDRESSES_ES );
            UUID questionsESID = edmCacheManager
                    .getEntitySetId( organizationId, CHRONICLE_SURVEYS, QUESTION, QUESTIONS_ES );

            checkNotNullUUIDs( ImmutableSet
                    .of( participantESID, answersESID, respondsWithESID, addressesESID, questionsESID ) );

            // participant must be valid
            UUID participantEKID = Preconditions
                    .checkNotNull( enrollmentManager
                                    .getParticipantEntityKeyId( organizationId, studyId, participantId ),
                            "participant not found" );

            ListMultimap<UUID, Map<UUID, Set<Object>>> entities = ArrayListMultimap.create();
            ListMultimap<UUID, DataAssociation> associations = ArrayListMultimap.create();

            OffsetDateTime dateTime = OffsetDateTime.now();

            List<UUID> questionEntityKeyIds = new ArrayList<>( questionnaireResponses.keySet() );
            for ( int i = 0; i < questionEntityKeyIds.size(); i++ ) {
                UUID questionEntityKeyId = questionEntityKeyIds.get( i );

                Map<UUID, Set<Object>> answerEntity = ImmutableMap.of(
                        edmCacheManager.getPropertyTypeId( VALUES_FQN ),
                        questionnaireResponses.get( questionEntityKeyId ).get( VALUES_FQN ) );
                entities.put( answersESID, answerEntity );

                // 1. create participant -> respondsWith -> answer association
                Map<UUID, Set<Object>> respondsWithEntity = ImmutableMap.of(
                        edmCacheManager.getPropertyTypeId( DATE_TIME_FQN ),
                        ImmutableSet.of( dateTime )
                );
                associations.put( respondsWithESID, new DataAssociation(
                        participantESID,
                        Optional.empty(),
                        Optional.of( participantEKID ),
                        answersESID,
                        Optional.of( i ),
                        Optional.empty(),
                        respondsWithEntity
                ) );

                // 2. create answer -> addresses -> question association
                Map<UUID, Set<Object>> addressesEntity = ImmutableMap.of(
                        edmCacheManager.getPropertyTypeId( COMPLETED_DATE_TIME_FQN ),
                        ImmutableSet.of( dateTime )
                );
                associations.put( addressesESID, new DataAssociation(
                        answersESID,
                        Optional.of( i ),
                        Optional.empty(),
                        questionsESID,
                        Optional.empty(),
                        Optional.of( questionEntityKeyId ),
                        addressesEntity
                ) );
            }
            DataGraph dataGraph = new DataGraph( entities, associations );
            dataApi.createEntityAndAssociationData( dataGraph );

            logger.info( "submitted questionnaire: studyId = {}, participantId = {}", studyId, participantId );
        } catch ( Exception e ) {
            String errorMsg = "an error occurred while attempting to submit questionnaire";
            logger.error( errorMsg, e );
            throw new RuntimeException( errorMsg );
        }
    }

    @Override
    public void submitAppUsageSurvey(
            UUID organizationId,
            UUID studyId,
            String participantId,
            Map<UUID, Map<FullQualifiedName, Set<Object>>> associationDetails ) {

        logger.info( "submitting app usage survey: participantId = {}, studyId = {}", participantId, studyId );

        DataApi dataApi;
        try {
            ApiClient apiClient = apiCacheManager.prodApiClientCache.get( ApiClient.class );
            dataApi = apiClient.getDataApi();

            // participant must exist
            boolean isKnownParticipant = enrollmentManager.isKnownParticipant( organizationId, studyId, participantId );
            Preconditions.checkArgument( isKnownParticipant, "unknown participant = {}", participantId );

            // get entity set ids
            UUID usedByESID = edmCacheManager
                    .getEntitySetId( organizationId, CHRONICLE_DATA_COLLECTION, USED_BY, USED_BY_ES );
            checkNotNullUUIDs( ImmutableSet.of( usedByESID ) );

            // create association data
            Map<UUID, Map<UUID, Set<Object>>> associationData = new HashMap<>();

            associationDetails
                    .forEach( ( entityKeyId, entity ) -> {
                        associationData.put( entityKeyId, new HashMap<>() );
                        entity.forEach( ( propertyTypeFQN, data ) -> {
                            UUID propertyTypeId = edmCacheManager.getPropertyTypeId( propertyTypeFQN );
                            associationData.get( entityKeyId ).put( propertyTypeId, data );
                        } );
                    } );

            // update association entities
            dataApi.updateEntitiesInEntitySet( usedByESID,
                    associationData,
                    UpdateType.Replace );

            logger.info( "updated {} apps usage associations", associationDetails.size() );

        } catch ( Exception e ) {
            String error = "error updating apps usage: participant = " + participantId + ", studyId = " + studyId
                    + ", orgID = {}" + organizationId;
            logger.error( error, e );
            throw new IllegalStateException( error );
        }
    }

    // return a list of all the apps used by a participant filtered by the current date
    @Override
    public List<ChronicleAppsUsageDetails> getParticipantAppsUsageData(
            UUID organizationId,
            UUID studyId,
            String participantId,
            String date ) {

        logger.info( "Retrieving user apps: participantId = {}, studyId = {}, orgId: {}",
                participantId,
                studyId,
                organizationId );

        SearchApi searchApi;
        try {
            ApiClient apiClient = apiCacheManager.intApiClientCache.get( ApiClient.class );
            searchApi = apiClient.getSearchApi();

            // date must be valid.
            LocalDate.parse( date ); // this will throw a DateTimeParseException if date cannot be parsed

            // participant must exist
            UUID participantEKID = Preconditions
                    .checkNotNull( enrollmentManager
                                    .getParticipantEntityKeyId( organizationId, studyId, participantId ),
                            "participant does not exist" );

            // get entity set ids
            UUID participantsESID = edmCacheManager.getParticipantEntitySetId( organizationId, studyId );
            UUID userAppsESID = edmCacheManager
                    .getEntitySetId( organizationId, CHRONICLE_DATA_COLLECTION, USER_APPS, USER_APPS_ES );
            UUID usedByESID = edmCacheManager
                    .getEntitySetId( organizationId, CHRONICLE_DATA_COLLECTION, USED_BY, USED_BY_ES );

            checkNotNullUUIDs( ImmutableSet.of( participantsESID, userAppsESID, usedByESID ) );

            // search participants to retrieve neighbors in user apps entity set
            Map<UUID, List<NeighborEntityDetails>> participantNeighbors = searchApi.executeFilteredEntityNeighborSearch(
                    participantsESID,
                    new EntityNeighborsFilter(
                            ImmutableSet.of( participantEKID ),
                            Optional.of( ImmutableSet.of( userAppsESID ) ),
                            Optional.of( ImmutableSet.of( participantsESID ) ),
                            Optional.of( ImmutableSet.of( usedByESID ) )
                    )
            );

            if ( participantNeighbors.containsKey( participantEKID ) ) {
                return participantNeighbors.get( participantEKID )
                        .stream()
                        .filter( neighbor -> neighbor.getNeighborDetails().isPresent() )
                        .filter( neighbor -> neighbor
                                .getAssociationDetails()
                                .get( DATE_TIME_FQN )
                                .iterator()
                                .next()
                                .toString()
                                .startsWith( date )
                        )
                        .map( neighbor -> new ChronicleAppsUsageDetails(
                                neighbor.getNeighborDetails().get(),
                                neighbor.getAssociationDetails()
                        ) )
                        .collect( Collectors.toList() );
            }

            logger.warn( "no user apps found" );
            return ImmutableList.of();
        } catch ( ExecutionException e ) {
            logger.error( "error retrieving user apps: participant = {}, studyId = {}, orgId = {}",
                    participantId,
                    studyId,
                    organizationId );
            throw new IllegalStateException( e );
        }
    }
}
