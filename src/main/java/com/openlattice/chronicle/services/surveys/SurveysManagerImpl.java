package com.openlattice.chronicle.services.surveys;

import com.dataloom.streams.StreamUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.*;
import com.openlattice.ApiUtil;
import com.openlattice.chronicle.data.ChronicleAppsUsageDetails;
import com.openlattice.chronicle.data.ChronicleQuestionnaire;
import com.openlattice.chronicle.services.ApiCacheManager;
import com.openlattice.chronicle.services.CommonTasksManager;
import com.openlattice.chronicle.services.ScheduledTasksManager;
import com.openlattice.chronicle.services.enrollment.EnrollmentManager;
import com.openlattice.client.ApiClient;
import com.openlattice.data.*;
import com.openlattice.data.requests.NeighborEntityDetails;
import com.openlattice.edm.EdmApi;
import com.openlattice.search.SearchApi;
import com.openlattice.search.requests.EntityNeighborsFilter;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
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
import static com.openlattice.chronicle.constants.CollectionTemplateTypeName.PARTICIPATED_IN;
import static com.openlattice.chronicle.constants.CollectionTemplateTypeName.PART_OF;
import static com.openlattice.chronicle.constants.CollectionTemplateTypeName.QUESTION;
import static com.openlattice.chronicle.constants.CollectionTemplateTypeName.REGISTERED_FOR;
import static com.openlattice.chronicle.constants.CollectionTemplateTypeName.RESPONDS_WITH;
import static com.openlattice.chronicle.constants.CollectionTemplateTypeName.STUDIES;
import static com.openlattice.chronicle.constants.CollectionTemplateTypeName.SUBMISSION;
import static com.openlattice.chronicle.constants.CollectionTemplateTypeName.SURVEY;
import static com.openlattice.chronicle.constants.CollectionTemplateTypeName.TIME_RANGE;
import static com.openlattice.chronicle.constants.CollectionTemplateTypeName.USED_BY;
import static com.openlattice.chronicle.constants.CollectionTemplateTypeName.USER_APPS;
import static com.openlattice.chronicle.constants.EdmConstants.*;
import static com.openlattice.chronicle.util.ChronicleServerUtil.checkNotNullUUIDs;
import static com.openlattice.chronicle.util.ChronicleServerUtil.getFirstUUIDOrNull;
import static com.openlattice.edm.EdmConstants.ID_FQN;
import static java.util.Map.entry;

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
public class SurveysManagerImpl implements SurveysManager {
    protected static final Logger logger = LoggerFactory.getLogger( SurveysManagerImpl.class );

    private final String TIME_USE_DIARY_TITLE = "Time Use Diary";

    private final ApiCacheManager       apiCacheManager;
    private final EnrollmentManager     enrollmentManager;
    private final CommonTasksManager    commonTasksManager;
    private final ScheduledTasksManager scheduledTasksManager;

    public SurveysManagerImpl(
            ApiCacheManager apiCacheManager,
            EnrollmentManager enrollmentManager,
            CommonTasksManager commonTasksManager,
            ScheduledTasksManager scheduledTasksManager ) {
        this.apiCacheManager = apiCacheManager;
        this.enrollmentManager = enrollmentManager;
        this.commonTasksManager = commonTasksManager;
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
                    .checkNotNull( commonTasksManager.getStudyEntityKeyId( organizationId, studyId ),
                            "invalid study: " + studyId );

            // get apis
            ApiClient apiClient = apiCacheManager.prodApiClientCache.get( ApiClient.class );
            SearchApi searchApi = apiClient.getSearchApi();

            // entity set ids
            UUID questionnaireESID = commonTasksManager
                    .getEntitySetId( organizationId, CHRONICLE_SURVEYS, SURVEY, QUESTIONNAIRE_ES );
            UUID studiesESID = commonTasksManager.getEntitySetId( organizationId, CHRONICLE, STUDIES, STUDY_ES );
            UUID partOfESID = commonTasksManager.getEntitySetId( organizationId, CHRONICLE, PART_OF, PART_OF_ES );
            UUID questionESID = commonTasksManager
                    .getEntitySetId( organizationId, CHRONICLE_SURVEYS, QUESTION, QUESTIONS_ES );

            checkNotNullUUIDs( ImmutableSet.of( questionnaireESID, studiesESID, partOfESID, questionESID ) );

            // Get questionnaires that neighboring study
            Map<UUID, List<NeighborEntityDetails>> neighbors = searchApi.executeFilteredEntityNeighborSearch(
                    studiesESID,
                    new EntityNeighborsFilter(
                            Set.of( studyEKID ),
                            Optional.of( Set.of( questionnaireESID ) ),
                            Optional.of( Set.of( studiesESID ) ),
                            Optional.of( Set.of( partOfESID ) )
                    )
            );

            // find questionnaire entity matching given entity key id
            if ( neighbors.containsKey( studyEKID ) ) {
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
                                Set.of( questionnaireEKID ),
                                Optional.of( Set.of( questionESID ) ),
                                Optional.of( Set.of( questionnaireESID ) ),
                                Optional.of( Set.of( partOfESID ) )
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
            }

        } catch ( Exception e ) {
            // catch all errors encountered during execution
            logger.error( "unable to retrieve questionnaire: studyId = {}, questionnaire = {}",
                    studyId,
                    questionnaireEKID );
            throw new RuntimeException( "questionnaire not found" );
        }

        /*
         * IF we get to this point, the requested questionnaire was not found. We shouldn't return null since
         * the caller would get an "ok" response. Instead send an error response.
         */
        throw new IllegalArgumentException( "questionnaire not found" );
    }

    @Override
    public Map<UUID, Map<FullQualifiedName, Set<Object>>> getStudyQuestionnaires(
            UUID organizationId, UUID studyId ) {
        try {
            logger.info( "Retrieving questionnaires for study :{}", studyId );

            // If an organization does not have the chronicle_surveys app installed, exit early
            if ( scheduledTasksManager.entitySetIdsByOrgId.get( CHRONICLE_SURVEYS ).containsKey( organizationId ) ) {
                logger.warn( "No questionnaires found for study {}. {} is not installed on organization with id {}. ",
                        studyId,
                        CHRONICLE_SURVEYS.toString(),
                        organizationId );
                return ImmutableMap.of();
            }

            // check if study is valid
            UUID studyEntityKeyId = Preconditions
                    .checkNotNull( commonTasksManager.getStudyEntityKeyId( organizationId, studyId ),
                            "invalid studyId: " + studyId );

            // load apis
            ApiClient apiClient = apiCacheManager.prodApiClientCache.get( ApiClient.class );
            SearchApi searchApi = apiClient.getSearchApi();

            // entity set ids
            UUID questionnaireESID = commonTasksManager
                    .getEntitySetId( organizationId, CHRONICLE_SURVEYS, SURVEY, QUESTIONNAIRE_ES );
            UUID studiesESID = commonTasksManager.getEntitySetId( organizationId, CHRONICLE, STUDIES, STUDY_ES );
            UUID partOfESID = commonTasksManager.getEntitySetId( organizationId, CHRONICLE, PART_OF, PART_OF_ES );

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
                    .filter( neighbor -> neighbor.getNeighborId().isPresent() && neighbor.getNeighborDetails()
                            .isPresent() )
                    .collect( Collectors.toMap(
                            neighbor -> neighbor.getNeighborId().get(),
                            neighbor -> neighbor.getNeighborDetails().get()
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
            UUID participantESID = commonTasksManager.getParticipantEntitySetId( organizationId, studyId );
            UUID answersESID = commonTasksManager
                    .getEntitySetId( organizationId, CHRONICLE_SURVEYS, ANSWER, ANSWERS_ES );
            UUID respondsWithESID = commonTasksManager.getEntitySetId( organizationId,
                    CHRONICLE_SURVEYS,
                    RESPONDS_WITH,
                    RESPONDS_WITH_ES );
            UUID addressesESID = commonTasksManager
                    .getEntitySetId( organizationId, CHRONICLE_SURVEYS, ADDRESSES, ADDRESSES_ES );
            UUID questionsESID = commonTasksManager
                    .getEntitySetId( organizationId, CHRONICLE_SURVEYS, QUESTION, QUESTIONS_ES );

            checkNotNullUUIDs( ImmutableSet
                    .of( participantESID, answersESID, respondsWithESID, addressesESID, questionsESID ) );

            // participant must be valid
            UUID participantEKID = Preconditions
                    .checkNotNull( commonTasksManager
                                    .getParticipantEntityKeyId( organizationId, studyId, participantId ),
                            "participant not found" );

            ListMultimap<UUID, Map<UUID, Set<Object>>> entities = ArrayListMultimap.create();
            ListMultimap<UUID, DataAssociation> associations = ArrayListMultimap.create();

            OffsetDateTime dateTime = OffsetDateTime.now();

            List<UUID> questionEntityKeyIds = new ArrayList<>( questionnaireResponses.keySet() );
            for ( int i = 0; i < questionEntityKeyIds.size(); i++ ) {
                UUID questionEntityKeyId = questionEntityKeyIds.get( i );

                Map<UUID, Set<Object>> answerEntity = ImmutableMap.of(
                        commonTasksManager.getPropertyTypeId( VALUES_FQN ),
                        questionnaireResponses.get( questionEntityKeyId ).get( VALUES_FQN ) );
                entities.put( answersESID, answerEntity );

                // 1. create participant -> respondsWith -> answer association
                Map<UUID, Set<Object>> respondsWithEntity = ImmutableMap.of(
                        commonTasksManager.getPropertyTypeId( DATE_TIME_FQN ),
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
                        commonTasksManager.getPropertyTypeId( COMPLETED_DATE_TIME_FQN ),
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
        EdmApi edmApi;
        try {
            ApiClient apiClient = apiCacheManager.prodApiClientCache.get( ApiClient.class );
            dataApi = apiClient.getDataApi();
            edmApi = apiClient.getEdmApi();

            // participant must exist
            boolean isKnownParticipant = enrollmentManager.isKnownParticipant( organizationId, studyId, participantId );
            Preconditions.checkArgument( isKnownParticipant, "unknown participant = {}", participantId );

            // get entity set ids
            UUID usedByESID = commonTasksManager
                    .getEntitySetId( organizationId, CHRONICLE_DATA_COLLECTION, USED_BY, USED_BY_ES );
            checkNotNullUUIDs( ImmutableSet.of( usedByESID ) );

            // create association data
            Map<UUID, Map<UUID, Set<Object>>> associationData = new HashMap<>();

            associationDetails
                    .forEach( ( entityKeyId, entity ) -> {
                        associationData.put( entityKeyId, new HashMap<>() );
                        entity.forEach( ( propertyTypeFQN, data ) -> {
                            UUID propertyTypeId = edmApi.getPropertyTypeId( propertyTypeFQN.getNamespace(),
                                    propertyTypeFQN.getName() );
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
                    .checkNotNull( commonTasksManager
                                    .getParticipantEntityKeyId( organizationId, studyId, participantId ),
                            "participant does not exist" );

            // get entity set ids
            UUID participantsESID = commonTasksManager.getParticipantEntitySetId( organizationId, studyId );
            UUID userAppsESID = commonTasksManager
                    .getEntitySetId( organizationId, CHRONICLE_DATA_COLLECTION, USER_APPS, USER_APPS_ES );
            UUID usedByESID = commonTasksManager
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

    // HELPER METHODS for submitTimeUseDiarySurvey

    private EntityKey getSurveyEntityKey( Map<UUID, Set<Object>> data, Map<String, UUID> entitySetIdMap ) {
        return new EntityKey(
                entitySetIdMap.get( QUESTIONNAIRE_ES ),
                ApiUtil.generateDefaultEntityId( List.of( commonTasksManager.getPropertyTypeId( OL_ID_FQN ) ), data )
        );
    }

    private EntityKey getSurveyPartOfStudyEntityKey( Map<UUID, Set<Object>> data, Map<String, UUID> entitySetIdMap ) {
        return new EntityKey(
                entitySetIdMap.get( PART_OF_ES ),
                ApiUtil.generateDefaultEntityId( List.of( commonTasksManager.getPropertyTypeId( OL_ID_FQN ) ), data )
        );
    }

    private EntityKey getQuestionEntityKey( Map<UUID, Set<Object>> data, Map<String, UUID> entitySetIdMap ) {
        return new EntityKey(
                entitySetIdMap.get( QUESTIONS_ES ),
                ApiUtil.generateDefaultEntityId(
                        List.of( commonTasksManager.getPropertyTypeId( OL_ID_FQN ) ),
                        data
                )
        );
    }

    // unique for startdatetime + enddatetime
    private EntityKey getTimeRangeEntityKey( Map<UUID, Set<Object>> data, Map<String, UUID> entitySetIdMap ) {
        return new EntityKey(
                entitySetIdMap.get( TIMERANGE_ES ),
                ApiUtil.generateDefaultEntityId(
                        List.of( END_DATE_TIME_FQN, START_DATE_TIME_FQN ).stream()
                                .map( commonTasksManager::getPropertyTypeId ).collect(
                                Collectors.toList() ),
                        data
                )
        );
    }

    private EntityKey getSubmissionEntityKey( Map<UUID, Set<Object>> data, Map<String, UUID> entitySetIdMap ) {
        return new EntityKey(
                entitySetIdMap.get( SUBMISSION_ES ),
                ApiUtil.generateDefaultEntityId( List.of( STRING_ID_FQN, OL_ID_FQN, DATE_TIME_FQN ).stream()
                                .map( commonTasksManager::getPropertyTypeId ),
                        data
                )
        );
    }

    private EntityKey getRespondsWithEntityKey( Map<UUID, Set<Object>> data, Map<String, UUID> entitySetIdMap ) {
        return new EntityKey(
                entitySetIdMap.get( RESPONDS_WITH_ES ),
                ApiUtil.generateDefaultEntityId( List.of( OL_ID_FQN, DATE_TIME_FQN, STRING_ID_FQN ).stream()
                                .map( commonTasksManager::getPropertyTypeId ),
                        data
                )
        );
    }

    private EntityKey getParticipatedInEntityKey( Map<UUID, Set<Object>> data, Map<String, UUID> entitySetIdMap ) {
        return new EntityKey(
                entitySetIdMap.get( PARTICIPATED_IN_ES ),
                ApiUtil.generateDefaultEntityId( List.of( STRING_ID_FQN, START_DATE_TIME_FQN, PERSON_ID_FQN )
                                .stream().map( commonTasksManager::getPropertyTypeId ),
                        data
                )
        );
    }

    // question -> partof -> survey: unique for question id (ol.code/ol.id)
    private EntityKey getPartOfEntityKey( Map<UUID, Set<Object>> data, Map<String, UUID> entitySetIdMap ) {
        return new EntityKey(
                entitySetIdMap.get( PART_OF_ES ),
                ApiUtil.generateDefaultEntityId( List.of( commonTasksManager.getPropertyTypeId( OL_ID_FQN ) ), data )
        );
    }

    private EntityKey getStudyEntityKey( UUID entitySetId, UUID studyId ) {
        return new EntityKey(
                entitySetId,
                studyId.toString()
        );
    }

    private EntityKey getParticipantEntityKey( UUID participantESID, String participantId ) {
        return new EntityKey(
                participantESID,
                participantId
        );
    }

    private Map<UUID, Set<Object>> getSurveyEntityData() {
        return ImmutableMap.of(
                commonTasksManager.getPropertyTypeId( OL_ID_FQN ),
                ImmutableSet.of( TIME_USE_DIARY_TITLE )
        );
    }

    private Map<UUID, Set<Object>> getQuestionEntityData( Map<FullQualifiedName, Set<Object>> data ) {
        return ImmutableMap.of(
                commonTasksManager.getPropertyTypeId( OL_ID_FQN ), data.get( OL_ID_FQN ),
                commonTasksManager.getPropertyTypeId( TITLE_FQN ), data.get( TITLE_FQN )
        );
    }

    private Map<UUID, Set<Object>> getAnswerEntity( Map<FullQualifiedName, Set<Object>> data ) {
        return ImmutableMap.of(
                commonTasksManager.getPropertyTypeId( VALUES_FQN ), data.get( VALUES_FQN )
        );
    }

    private Map<UUID, Set<Object>> getRegisteredForEntity( OffsetDateTime dateTime ) {
        return ImmutableMap.of(
                commonTasksManager.getPropertyTypeId( COMPLETED_DATE_TIME_FQN ),
                ImmutableSet.of( dateTime )
        );
    }

    // unique for participant + study + datetime
    private Map<UUID, Set<Object>> getRespondsWithSubmissionEntity(
            UUID studyId,
            String participantId,
            OffsetDateTime dateTime ) {
        Map<UUID, Set<Object>> entity = Maps.newHashMap();

        entity.put( commonTasksManager.getPropertyTypeId( OL_ID_FQN ), ImmutableSet.of( participantId ) );
        entity.put( commonTasksManager.getPropertyTypeId( DATE_TIME_FQN ), ImmutableSet.of( dateTime ) );
        entity.put( commonTasksManager.getPropertyTypeId( STRING_ID_FQN ), ImmutableSet.of( studyId ) );

        return entity;
    }

    // unique for participant + study + datetime
    private Map<UUID, Set<Object>> getParticipatedInEntity(
            UUID studyId,
            String participantId,
            OffsetDateTime dateTime ) {
        return ImmutableMap.of(
                commonTasksManager.getPropertyTypeId( START_DATE_TIME_FQN ), ImmutableSet.of( dateTime ),
                commonTasksManager.getPropertyTypeId( STRING_ID_FQN ), ImmutableSet.of( studyId ),
                commonTasksManager.getPropertyTypeId( PERSON_ID_FQN ), ImmutableSet.of( participantId )
        );
    }

    private Map<UUID, Set<Object>> getTimeRangeEntity( Map<FullQualifiedName, Set<Object>> data ) {
        Set<Object> startDatetime = data.getOrDefault( START_DATE_TIME_FQN, ImmutableSet.of() );
        Set<Object> endDatetime = data.getOrDefault( END_DATE_TIME_FQN, ImmutableSet.of() );

        return ImmutableMap.of(
                commonTasksManager.getPropertyTypeId( START_DATE_TIME_FQN ), startDatetime,
                commonTasksManager.getPropertyTypeId( END_DATE_TIME_FQN ), endDatetime
        );
    }

    // unique for time + studyId + participantId
    private Map<UUID, Set<Object>> getSubmissionEntity( UUID studyId, String participantId, OffsetDateTime dateTime ) {
        Map<UUID, Set<Object>> entity = Maps.newHashMap();

        entity.put( commonTasksManager.getPropertyTypeId( DATE_TIME_FQN ), ImmutableSet.of( dateTime ) );
        entity.put( commonTasksManager.getPropertyTypeId( STRING_ID_FQN ), ImmutableSet.of( studyId ) );
        entity.put( commonTasksManager.getPropertyTypeId( OL_ID_FQN ), ImmutableSet.of( participantId ) );

        return entity;
    }

    private Map<UUID, Set<Object>> getAddressesEntity( OffsetDateTime dateTime ) {
        return ImmutableMap.of(
                commonTasksManager.getPropertyTypeId( COMPLETED_DATE_TIME_FQN ),
                ImmutableSet.of( dateTime )
        );
    }

    private Map<UUID, Set<Object>> getQuestionPartOfEntity( Map<FullQualifiedName, Set<Object>> data ) {
        return ImmutableMap.of(
                commonTasksManager.getPropertyTypeId( OL_ID_FQN ), data.get( OL_ID_FQN )
        );
    }

    // unique for studyId
    private Map<UUID, Set<Object>> getSurveyPartOfEntityData( UUID studyId ) {
        return ImmutableMap.of(
                commonTasksManager.getPropertyTypeId( OL_ID_FQN ), ImmutableSet.of( studyId )
        );
    }

    // associate entityKeys with EKIDS
    private Map<EntityKey, UUID> createEntityKeyIdMap(
            Set<EntityKey> entityKeys,
            Set<Triple<EntityKey, EntityKey, EntityKey>> edgesByEntityKey,
            Map<String, UUID> entitySetIdMap,
            DataIntegrationApi integrationApi,
            UUID studyId,
            UUID studyEKID,
            UUID participantEKID,
            String participantId
    ) {
        Map<EntityKey, UUID> entityKeyIdMap = Maps.newHashMap();

        Set<EntityKey> orderedEntityKeys = Sets.newHashSet( entityKeys );

        edgesByEntityKey.forEach( triple -> {
            orderedEntityKeys.add( triple.getRight() );
            orderedEntityKeys.add( triple.getLeft() );
            orderedEntityKeys.add( triple.getMiddle() );
        } );

        List<EntityKey> entityKeyList = new ArrayList<>( orderedEntityKeys );

        List<UUID> entityKeyIds = integrationApi.getEntityKeyIds( orderedEntityKeys );
        for ( int i = 0; i < entityKeyIds.size(); ++i ) {
            entityKeyIdMap.put( entityKeyList.get( i ), entityKeyIds.get( i ) );
        }

        EntityKey studyEK = getStudyEntityKey( entitySetIdMap.get( STUDY_ES ), studyId );
        entityKeyIdMap.put( studyEK, studyEKID );

        EntityKey participantEK = getParticipantEntityKey( entitySetIdMap.get( PARTICIPANTS_PREFIX ), participantId );
        entityKeyIdMap.put( participantEK, participantEKID );

        return entityKeyIdMap;
    }

    private ListMultimap<UUID, DataAssociation> getDataAssociations(
            Map<FullQualifiedName, Set<Object>> entity,
            Map<EntityKey, UUID> entityKeyIdMap,
            Map<String, UUID> entitySetIdMap,
            UUID participantEKID,
            UUID submissionEKID,
            OffsetDateTime dateTime,
            int index
    ) {
        ListMultimap<UUID, DataAssociation> associations = ArrayListMultimap.create();

        Map<UUID, Set<Object>> questionEntityData = getQuestionEntityData( entity );
        EntityKey questionEK = getQuestionEntityKey( questionEntityData, entitySetIdMap );
        UUID questionEKID = entityKeyIdMap.get( questionEK );

        Map<UUID, Set<Object>> timeRangeEntityData = getTimeRangeEntity( entity );
        EntityKey timeRangeEK = getTimeRangeEntityKey( timeRangeEntityData, entitySetIdMap );
        UUID timeRangeEKID = entityKeyIdMap.get( timeRangeEK );

        // answer -> registeredfor -> timerange
        Map<UUID, Set<Object>> registeredForEntity = getRegisteredForEntity( dateTime );
        associations.put( entitySetIdMap.get( REGISTERED_FOR_ES ), new DataAssociation(
                entitySetIdMap.get( ANSWERS_ES ),
                Optional.of( index ),
                Optional.empty(),
                entitySetIdMap.get( TIMERANGE_ES ),
                Optional.empty(),
                Optional.of( timeRangeEKID ),
                registeredForEntity
        ) );

        // answer -> addresses -> question
        Map<UUID, Set<Object>> addressesEntity = getAddressesEntity( dateTime );
        associations.put( entitySetIdMap.get( ADDRESSES_ES ), new DataAssociation(
                entitySetIdMap.get( ANSWERS_ES ),
                Optional.of( index ),
                Optional.empty(),
                entitySetIdMap.get( QUESTIONS_ES ),
                Optional.empty(),
                Optional.of( questionEKID ),
                addressesEntity
        ) );

        // answer -> registeredfor -> submission
        associations.put( entitySetIdMap.get( REGISTERED_FOR_ES ), new DataAssociation(
                entitySetIdMap.get( ANSWERS_ES ),
                Optional.of( index ),
                Optional.empty(),
                entitySetIdMap.get( SUBMISSION_ES ),
                Optional.empty(),
                Optional.of( submissionEKID ),
                registeredForEntity
        ) );

        // participant -> respondswith -> answer
        Map<UUID, Set<Object>> respondWithEntity = ImmutableMap.of(
                commonTasksManager.getPropertyTypeId( DATE_TIME_FQN ), ImmutableSet.of( dateTime )
        );
        associations.put( entitySetIdMap.get( RESPONDS_WITH_ES ), new DataAssociation(
                entitySetIdMap.get( PARTICIPANTS_PREFIX ),
                Optional.empty(),
                Optional.of( participantEKID ),
                entitySetIdMap.get( ANSWERS_ES ),
                Optional.of( index ),
                Optional.empty(),
                respondWithEntity
        ) );

        return associations;
    }

    // entitySetID -> entityKeyId -> propertyID
    private Map<UUID, Map<UUID, Map<UUID, Set<Object>>>> getEntitiesByESID(
            Map<EntityKey, UUID> entityKeyIdMap,
            Map<EntityKey, Map<UUID, Set<Object>>> entitiesByEK
    ) {
        Map<UUID, Map<UUID, Map<UUID, Set<Object>>>> result = Maps.newHashMap();
        entitiesByEK.forEach( ( entityKey, entity ) -> {
            UUID entitySetId = entityKey.getEntitySetId();
            UUID entityKeyId = entityKeyIdMap.get( entityKey );

            Map<UUID, Map<UUID, Set<Object>>> entities = result.getOrDefault( entitySetId, Maps.newHashMap() );
            entities.put( entityKeyId, entity );

            result.put( entitySetId, entities );
        } );
        return result;
    }

    private Pair<Map<EntityKey, Map<UUID, Set<Object>>>, Set<Triple<EntityKey, EntityKey, EntityKey>>> getEdgesAndEntityKeys(
            Map<String, UUID> entitySetIdMap,
            List<Map<FullQualifiedName, Set<Object>>> surveyData,
            UUID studyId,
            String participantId,
            OffsetDateTime dateTime
    ) {
        Set<Triple<EntityKey, EntityKey, EntityKey>> edgesByEntityKey = Sets.newHashSet(); // <src, association, dst>
        Map<EntityKey, Map<UUID, Set<Object>>> entitiesByEK = Maps.newHashMap();

        // edge: survey -> partof -> study

        Map<UUID, Set<Object>> surveyEntityData = getSurveyEntityData();
        EntityKey surveyEK = getSurveyEntityKey( surveyEntityData, entitySetIdMap );
        entitiesByEK.put( surveyEK, surveyEntityData );

        Map<UUID, Set<Object>> surveyPartOfEntityData = getSurveyPartOfEntityData( studyId );
        EntityKey surveyPartOfEK = getSurveyPartOfStudyEntityKey( surveyPartOfEntityData, entitySetIdMap );
        entitiesByEK.put( surveyPartOfEK, surveyPartOfEntityData );

        EntityKey studyEK = getStudyEntityKey( entitySetIdMap.get( STUDY_ES ), studyId );
        edgesByEntityKey.add( Triple.of( surveyEK, surveyPartOfEK, studyEK ) );

        // edge: person -> participatedin -> survey

        Map<UUID, Set<Object>> participatedInEntityData = getParticipatedInEntity( studyId, participantId, dateTime );
        EntityKey participatedInEK = getParticipatedInEntityKey( participatedInEntityData, entitySetIdMap );
        entitiesByEK.put( participatedInEK, participatedInEntityData );

        EntityKey participantEK = getParticipantEntityKey( entitySetIdMap.get( PARTICIPANTS_PREFIX ), participantId );

        edgesByEntityKey.add( Triple.of( participantEK, participatedInEK, surveyEK ) );

        // edge: person -> respondswith -> submission

        Map<UUID, Set<Object>> submissionEntityData = getSubmissionEntity( studyId, participantId, dateTime );
        EntityKey submissionEK = getSubmissionEntityKey( submissionEntityData, entitySetIdMap );
        submissionEntityData.remove( commonTasksManager.getPropertyTypeId( STRING_ID_FQN ) ); // shouldn't be stored
        submissionEntityData.remove( commonTasksManager.getPropertyTypeId( OL_ID_FQN ) ); // shouldn't be stored
        entitiesByEK.put( submissionEK, submissionEntityData );

        Map<UUID, Set<Object>> respondsWithSubmissionEntity = getRespondsWithSubmissionEntity( studyId,
                participantId,
                dateTime );
        EntityKey respondsWithEK = getRespondsWithEntityKey( respondsWithSubmissionEntity, entitySetIdMap );
        respondsWithSubmissionEntity.remove( commonTasksManager.getPropertyTypeId( STRING_ID_FQN ) ); // shouldn't be stored
        entitiesByEK.put( respondsWithEK, respondsWithSubmissionEntity );

        edgesByEntityKey.add( Triple.of( participantEK, respondsWithEK, submissionEK ) );

        surveyData
                .forEach( entity -> {
                    // edge: question -> partof -> survey
                    Map<UUID, Set<Object>> questionEntityData = getQuestionEntityData( entity );
                    EntityKey questionEK = getQuestionEntityKey( questionEntityData, entitySetIdMap );
                    entitiesByEK.put( questionEK, questionEntityData );

                    Map<UUID, Set<Object>> questionPartOfSurveyEntity = getQuestionPartOfEntity( entity );
                    EntityKey partOfEK = getPartOfEntityKey( questionPartOfSurveyEntity, entitySetIdMap );
                    entitiesByEK.put( partOfEK, questionPartOfSurveyEntity );

                    edgesByEntityKey.add( Triple.of( questionEK, partOfEK, surveyEK ) );

                    //edge: answer -> registeredfor -> timerange
                    Map<UUID, Set<Object>> timeRangeEntityData = getTimeRangeEntity( entity );
                    EntityKey timeRangeEK = getTimeRangeEntityKey( timeRangeEntityData, entitySetIdMap );
                    entitiesByEK.put( timeRangeEK, timeRangeEntityData );
                } );

        return Pair.of( entitiesByEK, edgesByEntityKey );
    }

    private Set<DataEdgeKey> getDataEdgeKeysFromEntityKeys(
            Set<Triple<EntityKey, EntityKey, EntityKey>> edgesByEntityKey,
            Map<EntityKey, UUID> entityKeyIdMap
    ) {
        return StreamUtil.stream( edgesByEntityKey )
                .map( triple -> {
                    UUID srcEKID = entityKeyIdMap.get( triple.getLeft() );
                    UUID edgeEKID = entityKeyIdMap.get( triple.getMiddle() );
                    UUID dstEKID = entityKeyIdMap.get( triple.getRight() );

                    UUID srcESID = triple.getLeft().getEntitySetId();
                    UUID edgeESID = triple.getMiddle().getEntitySetId();
                    UUID dstESID = triple.getRight().getEntitySetId();

                    EntityDataKey src = new EntityDataKey( srcESID, srcEKID );
                    EntityDataKey edge = new EntityDataKey( edgeESID, edgeEKID );
                    EntityDataKey dst = new EntityDataKey( dstESID, dstEKID );

                    return new DataEdgeKey( src, dst, edge );
                } )
                .collect( Collectors.toSet() );
    }

    private Map<String, UUID> createEntitySetIdMap( UUID orgId, UUID studyId ) {

        // get required entity set ids
        UUID participantsESID = commonTasksManager.getParticipantEntitySetId( orgId, studyId );
        UUID answersESID = commonTasksManager.getEntitySetId( orgId, CHRONICLE_SURVEYS, ANSWER, ANSWERS_ES );
        UUID submissionESID = commonTasksManager
                .getEntitySetId( orgId, CHRONICLE_SURVEYS, SUBMISSION, SUBMISSION_ES );
        UUID timeRangeESID = commonTasksManager
                .getEntitySetId( orgId, CHRONICLE_SURVEYS, TIME_RANGE, TIMERANGE_ES );
        UUID questionESID = commonTasksManager.getEntitySetId( orgId, CHRONICLE_SURVEYS, QUESTION, QUESTIONS_ES );
        UUID questionnaireESID = commonTasksManager
                .getEntitySetId( orgId, CHRONICLE_SURVEYS, SURVEY, QUESTIONNAIRE_ES );
        UUID studyESID = commonTasksManager.getEntitySetId( orgId, CHRONICLE, STUDIES, STUDY_ES );
        UUID respondsWithESID = commonTasksManager
                .getEntitySetId( orgId, CHRONICLE_SURVEYS, RESPONDS_WITH, RESPONDS_WITH_ES );
        UUID addressesESID = commonTasksManager.getEntitySetId( orgId, CHRONICLE_SURVEYS, ADDRESSES, ADDRESSES_ES );
        UUID participatedInESID = commonTasksManager
                .getEntitySetId( orgId, CHRONICLE, PARTICIPATED_IN, PARTICIPATED_IN_ES );
        UUID registeredForESID = commonTasksManager
                .getEntitySetId( orgId, CHRONICLE_SURVEYS, REGISTERED_FOR, REGISTERED_FOR_ES );
        UUID partOfESID = commonTasksManager.getEntitySetId( orgId, CHRONICLE, PART_OF, PART_OF_ES );

        return Map.ofEntries(
                entry( PARTICIPANTS_PREFIX, participantsESID ),
                entry( ANSWERS_ES, answersESID ),
                entry( SUBMISSION_ES, submissionESID ),
                entry( TIMERANGE_ES, timeRangeESID ),
                entry( QUESTIONS_ES, questionESID ),
                entry( QUESTIONNAIRE_ES, questionnaireESID ),
                entry( STUDY_ES, studyESID ),
                entry( RESPONDS_WITH_ES, respondsWithESID ),
                entry( ADDRESSES_ES, addressesESID ),
                entry( PARTICIPATED_IN_ES, participatedInESID ),
                entry( REGISTERED_FOR_ES, registeredForESID ),
                entry( PART_OF_ES, partOfESID )
        );
    }

    @Override
    public void submitTimeUseDiarySurvey(
            UUID orgId,
            UUID studyId,
            String participantId,
            List<Map<FullQualifiedName, Set<Object>>> surveyData ) {
        logger.info( "attempting to submit time use diary survey: studyId = {}, participantId = {}",
                studyId,
                participantId );

        try {
            ApiClient apiClient = apiCacheManager.prodApiClientCache.get( ApiClient.class );
            DataIntegrationApi integrationApi = apiClient.getDataIntegrationApi();
            DataApi dataApi = apiClient.getDataApi();

            // participant must exist
            UUID participantEKID = Preconditions
                    .checkNotNull( commonTasksManager.getParticipantEntityKeyId( orgId, studyId, participantId ),
                            "participant not found" );
            UUID studyEKID = Preconditions
                    .checkNotNull( commonTasksManager.getStudyEntityKeyId( orgId, studyId ), "study not found" );

            // create entitySetId look up map
            Map<String, UUID> entitySetIdMap = createEntitySetIdMap( orgId, studyId );

            ListMultimap<UUID, Map<UUID, Set<Object>>> entities = ArrayListMultimap.create();
            ListMultimap<UUID, DataAssociation> associations = ArrayListMultimap.create();

            Map<EntityKey, Map<UUID, Set<Object>>> entitiesByEK = Maps.newHashMap();
            Set<Triple<EntityKey, EntityKey, EntityKey>> edgesByEntityKey = Sets
                    .newHashSet(); // <src, association, dst>

            OffsetDateTime dateTime = OffsetDateTime.now();

            Pair<Map<EntityKey, Map<UUID, Set<Object>>>, Set<Triple<EntityKey, EntityKey, EntityKey>>> edgesAndEntityKeys = getEdgesAndEntityKeys(
                    entitySetIdMap,
                    surveyData,
                    studyId,
                    participantId,
                    dateTime
            );
            edgesByEntityKey.addAll( edgesAndEntityKeys.getRight() );
            entitiesByEK.putAll( edgesAndEntityKeys.getLeft() );

            Map<EntityKey, UUID> entityKeyIdMap = createEntityKeyIdMap( entitiesByEK.keySet(),
                    edgesByEntityKey,
                    entitySetIdMap,
                    integrationApi,
                    studyId,
                    studyEKID,
                    participantEKID,
                    participantId );

            Map<UUID, Set<Object>> submissionEntityData = getSubmissionEntity( studyId, participantId, dateTime );
            EntityKey submissionEK = getSubmissionEntityKey( submissionEntityData, entitySetIdMap );
            UUID submissionEKID = entityKeyIdMap.get( submissionEK );

            for ( int i = 0; i < surveyData.size(); ++i ) {
                Map<FullQualifiedName, Set<Object>> entity = surveyData.get( i );

                associations.putAll( getDataAssociations( entity,
                        entityKeyIdMap,
                        entitySetIdMap,
                        participantEKID,
                        submissionEKID,
                        dateTime,
                        i ) );

                // entities
                Map<UUID, Set<Object>> answerEntity = getAnswerEntity( entity );
                entities.put( entitySetIdMap.get( ANSWERS_ES ), answerEntity );
            }

            // update entities
            Map<UUID, Map<UUID, Map<UUID, Set<Object>>>> entitiesByEntitySet = getEntitiesByESID( entityKeyIdMap,
                    entitiesByEK );
            entitiesByEntitySet.forEach( ( entitySetId, groupedEntities ) -> {
                dataApi.updateEntitiesInEntitySet( entitySetId, groupedEntities, UpdateType.PartialReplace );
            } );

            DataGraph dataGraph = new DataGraph( entities, associations );
            dataApi.createEntityAndAssociationData( dataGraph );

            // create edges
            Set<DataEdgeKey> dataEdgeKeys = getDataEdgeKeysFromEntityKeys( edgesByEntityKey, entityKeyIdMap );
            dataApi.createEdges( dataEdgeKeys );

            logger.info( "successfully submitted time use diary survey: studyId = {}, participantId = {}",
                    studyId,
                    participantId );

        } catch ( Exception e ) {
            logger.error( "failed to submit time use diary survey: studyId = {}, participantId = {}",
                    studyId,
                    participantId,
                    e );
            throw new RuntimeException( "error submitting time use dairy responses" );
        }
    }
}
