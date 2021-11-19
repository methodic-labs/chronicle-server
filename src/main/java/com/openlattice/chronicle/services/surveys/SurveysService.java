package com.openlattice.chronicle.services.surveys;

import com.dataloom.streams.StreamUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.*;
import com.openlattice.ApiUtil;
import com.openlattice.chronicle.constants.AppComponent;
import com.openlattice.chronicle.data.*;
import com.openlattice.chronicle.services.ApiCacheManager;
import com.openlattice.chronicle.services.edm.EdmCacheManager;
import com.openlattice.chronicle.services.enrollment.EnrollmentManager;
import com.openlattice.chronicle.services.entitysets.EntitySetIdsManager;
import com.openlattice.client.ApiClient;
import com.openlattice.data.*;
import com.openlattice.data.requests.NeighborEntityDetails;
import com.openlattice.search.SearchApi;
import com.openlattice.search.requests.EntityNeighborsFilter;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static com.openlattice.chronicle.constants.AppComponent.CHRONICLE_SURVEYS;
import static com.openlattice.chronicle.constants.EdmConstants.*;
import static com.openlattice.chronicle.util.ChronicleServerUtil.getFirstUUIDOrNull;
import static com.openlattice.chronicle.util.ChronicleServerUtil.setDownloadContentType;
import static com.openlattice.edm.EdmConstants.ID_FQN;

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
public class SurveysService implements SurveysManager {
    protected static final Logger logger = LoggerFactory.getLogger( SurveysService.class );

    private static final String TIME_USE_DIARY_TITLE = "Time Use Diary";

    private final ApiCacheManager     apiCacheManager;
    private final EnrollmentManager   enrollmentManager;
    private final EdmCacheManager     edmCacheManager;
    private final EntitySetIdsManager entitySetIdsManager;

    public SurveysService(
            ApiCacheManager apiCacheManager,
            EdmCacheManager edmCacheManager,
            EntitySetIdsManager entitySetIdsManager,
            EnrollmentManager enrollmentManager ) {

        this.edmCacheManager = edmCacheManager;
        this.apiCacheManager = apiCacheManager;
        this.entitySetIdsManager = entitySetIdsManager;
        this.enrollmentManager = enrollmentManager;
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
            EntitySetsConfig entitySetsConfig = entitySetIdsManager.getEntitySetsConfig( organizationId, studyId, ImmutableSet.of(CHRONICLE_SURVEYS) );

            UUID questionnaireESID = entitySetsConfig.getSurveysEntitySetId();
            UUID studiesESID = entitySetsConfig.getStudiesEntitySetId();
            UUID partOfESID = entitySetsConfig.getPartOfEntitySetId();
            UUID questionESID = entitySetsConfig.getQuestionsEntitySetId();

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
            if ( organizationId != null && !entitySetIdsManager.getEntitySetIdsByOrgId().get( CHRONICLE_SURVEYS )
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
            EntitySetsConfig entitySetsConfig = entitySetIdsManager.getEntitySetsConfig( organizationId, studyId, ImmutableSet.of(
                    CHRONICLE_SURVEYS) );

            UUID questionnaireESID = entitySetsConfig.getSurveysEntitySetId();
            UUID studiesESID = entitySetsConfig.getStudiesEntitySetId();
            UUID partOfESID = entitySetsConfig.getPartOfEntitySetId();

            // filtered search on questionnaires ES to get neighbors of study
            Map<UUID, List<NeighborEntityDetails>> neighbors = searchApi
                    .executeFilteredEntityNeighborSearch(
                            studiesESID,
                            new EntityNeighborsFilter(
                                    ImmutableSet.of( studyEntityKeyId ),
                                    Optional.of( ImmutableSet.of( questionnaireESID ) ),
                                    Optional.of( ImmutableSet.of( studiesESID ) ),
                                    Optional.of( ImmutableSet.of( partOfESID ) )
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
            EntitySetsConfig entitySetsConfig = entitySetIdsManager.getEntitySetsConfig( organizationId, studyId, ImmutableSet.of(
                    CHRONICLE_SURVEYS) );

            UUID participantESID = entitySetsConfig.getParticipantEntitySetId();
            UUID answersESID = entitySetsConfig.getAnswersEntitySetId();
            UUID respondsWithESID = entitySetsConfig.getRespondsWithEntitySetId();
            UUID addressesESID = entitySetsConfig.getAddressesEntitySetId();
            UUID questionsESID = entitySetsConfig.getQuestionsEntitySetId();

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
            EntitySetsConfig entitySetsConfig = entitySetIdsManager.getEntitySetsConfig( organizationId, studyId, ImmutableSet.of(AppComponent.CHRONICLE_DATA_COLLECTION) );
            UUID usedByESID = entitySetsConfig.getUsedByEntitySetId();

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
                    UpdateType.Replace,
                    PropertyUpdateType.Versioned );

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
            EntitySetsConfig entitySetsConfig = entitySetIdsManager.getEntitySetsConfig( organizationId, studyId, ImmutableSet.of(AppComponent.CHRONICLE_DATA_COLLECTION) );

            // search participants to retrieve neighbors in user apps entity set
            Map<UUID, List<NeighborEntityDetails>> participantNeighbors = searchApi.executeFilteredEntityNeighborSearch(
                    entitySetsConfig.getParticipantEntitySetId(),
                    new EntityNeighborsFilter(
                            ImmutableSet.of( participantEKID ),
                            Optional.of( ImmutableSet.of( entitySetsConfig.getUserAppsEntitySetId() ) ),
                            Optional.of( ImmutableSet.of( entitySetsConfig.getParticipantEntitySetId() ) ),
                            Optional.of( ImmutableSet.of( entitySetsConfig.getUsedByEntitySetId() ) )
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
    private EntityKey getSurveyEntityKey( UUID entitySetId, Map<UUID, Set<Object>> data ) {
        return new EntityKey(
                entitySetId,
                ApiUtil.generateDefaultEntityId( List.of( edmCacheManager.getPropertyTypeId( OL_ID_FQN ) ), data )
        );
    }

    // survey -> partof -> study: unique for studyId
    private EntityKey getSurveyPartOfStudyEntityKey( UUID entitySetId, Map<UUID, Set<Object>> data ) {
        return new EntityKey(
                entitySetId,
                ApiUtil.generateDefaultEntityId( List.of( edmCacheManager.getPropertyTypeId( OL_ID_FQN ) ), data )
        );
    }

    private EntityKey getQuestionEntityKey( UUID entitySetId, Map<UUID, Set<Object>> data ) {
        return new EntityKey(
                entitySetId,
                ApiUtil.generateDefaultEntityId(
                        List.of( edmCacheManager.getPropertyTypeId( OL_ID_FQN ) ),
                        data
                )
        );
    }

    // unique for startdatetime + enddatetime
    private EntityKey getTimeRangeEntityKey( UUID entitySetId, Map<UUID, Set<Object>> data ) {
        return new EntityKey(
                entitySetId,
                ApiUtil.generateDefaultEntityId(
                        List.of( END_DATE_TIME_FQN, START_DATE_TIME_FQN ).stream()
                                .map( edmCacheManager::getPropertyTypeId ).collect(
                                        Collectors.toList() ),
                        data
                )
        );
    }

    // unique for datetime + study + participant
    private EntityKey getSubmissionEntityKey( UUID entitySetId, Map<UUID, Set<Object>> data ) {
        return new EntityKey(
                entitySetId,
                ApiUtil.generateDefaultEntityId( List.of( STRING_ID_FQN, OL_ID_FQN, DATE_TIME_FQN ).stream()
                                .map( edmCacheManager::getPropertyTypeId ),
                        data
                )
        );
    }

    // participant -> respondswith -> submission: unique for each studyId + participantId + datetime combination
    private EntityKey getRespondsWithEntityKey( UUID entitySetId, Map<UUID, Set<Object>> data ) {
        return new EntityKey(
                entitySetId,
                ApiUtil.generateDefaultEntityId( List.of( OL_ID_FQN, DATE_TIME_FQN, STRING_ID_FQN ).stream()
                                .map( edmCacheManager::getPropertyTypeId ),
                        data
                )
        );
    }

    // participant -> participated in -> survey: unique for studyId + participantId + datetime
    private EntityKey getParticipatedInEntityKey( UUID entitySetId, Map<UUID, Set<Object>> data ) {
        return new EntityKey(
                entitySetId,
                ApiUtil.generateDefaultEntityId( List.of( STRING_ID_FQN, START_DATE_TIME_FQN, PERSON_ID_FQN )
                                .stream().map( edmCacheManager::getPropertyTypeId ),
                        data
                )
        );
    }

    // question -> partof -> survey: unique for question id (ol.code/ol.id)
    private EntityKey getQuestionPartOfSurveyEntityKey( UUID entitySetId, Map<UUID, Set<Object>> data ) {
        return new EntityKey(
                entitySetId,
                ApiUtil.generateDefaultEntityId( List.of( edmCacheManager.getPropertyTypeId( OL_ID_FQN ) ), data )
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
                edmCacheManager.getPropertyTypeId( OL_ID_FQN ),
                ImmutableSet.of( TIME_USE_DIARY_TITLE )
        );
    }

    private Map<UUID, Set<Object>> getQuestionEntityData( Map<FullQualifiedName, Set<Object>> data ) {
        return ImmutableMap.of(
                edmCacheManager.getPropertyTypeId( OL_ID_FQN ), data.get( OL_ID_FQN ),
                edmCacheManager.getPropertyTypeId( TITLE_FQN ), data.get( TITLE_FQN )
        );
    }

    private Map<UUID, Set<Object>> getAnswerEntity( Map<FullQualifiedName, Set<Object>> data ) {
        return ImmutableMap.of(
                edmCacheManager.getPropertyTypeId( VALUES_FQN ), data.get( VALUES_FQN )
        );
    }

    private Map<UUID, Set<Object>> getRegisteredForEntity( OffsetDateTime dateTime ) {
        return ImmutableMap.of(
                edmCacheManager.getPropertyTypeId( COMPLETED_DATE_TIME_FQN ),
                ImmutableSet.of( dateTime )
        );
    }

    private Map<UUID, Set<Object>> getRespondsWithSubmissionEntity(
            UUID studyId,
            String participantId,
            OffsetDateTime dateTime ) {
        Map<UUID, Set<Object>> entity = Maps.newHashMap();

        entity.put( edmCacheManager.getPropertyTypeId( OL_ID_FQN ), ImmutableSet.of( participantId ) );
        entity.put( edmCacheManager.getPropertyTypeId( DATE_TIME_FQN ), ImmutableSet.of( dateTime ) );
        entity.put( edmCacheManager.getPropertyTypeId( STRING_ID_FQN ), ImmutableSet.of( studyId ) );

        return entity;
    }

    private Map<UUID, Set<Object>> getParticipatedInEntity(
            UUID studyId,
            String participantId,
            OffsetDateTime dateTime ) {
        return ImmutableMap.of(
                edmCacheManager.getPropertyTypeId( START_DATE_TIME_FQN ), ImmutableSet.of( dateTime ),
                edmCacheManager.getPropertyTypeId( STRING_ID_FQN ), ImmutableSet.of( studyId ),
                edmCacheManager.getPropertyTypeId( PERSON_ID_FQN ), ImmutableSet.of( participantId )
        );
    }

    private Map<UUID, Set<Object>> getTimeRangeEntity( Map<FullQualifiedName, Set<Object>> data ) {
        Set<Object> startDatetime = data.getOrDefault( START_DATE_TIME_FQN, ImmutableSet.of() );
        Set<Object> endDatetime = data.getOrDefault( END_DATE_TIME_FQN, ImmutableSet.of() );

        return ImmutableMap.of(
                edmCacheManager.getPropertyTypeId( START_DATE_TIME_FQN ), startDatetime,
                edmCacheManager.getPropertyTypeId( END_DATE_TIME_FQN ), endDatetime
        );
    }

    private Map<UUID, Set<Object>> getSubmissionEntity( UUID studyId, String participantId, OffsetDateTime dateTime ) {
        Map<UUID, Set<Object>> entity = Maps.newHashMap();

        entity.put( edmCacheManager.getPropertyTypeId( DATE_TIME_FQN ), ImmutableSet.of( dateTime ) );
        entity.put( edmCacheManager.getPropertyTypeId( STRING_ID_FQN ), ImmutableSet.of( studyId ) );
        entity.put( edmCacheManager.getPropertyTypeId( OL_ID_FQN ), ImmutableSet.of( participantId ) );

        return entity;
    }

    private Map<UUID, Set<Object>> getAddressesEntity( OffsetDateTime dateTime ) {
        return ImmutableMap.of(
                edmCacheManager.getPropertyTypeId( COMPLETED_DATE_TIME_FQN ),
                ImmutableSet.of( dateTime )
        );
    }

    private Map<UUID, Set<Object>> getQuestionPartOfEntity( Map<FullQualifiedName, Set<Object>> data ) {
        return ImmutableMap.of(
                edmCacheManager.getPropertyTypeId( OL_ID_FQN ), data.get( OL_ID_FQN )
        );
    }

    private Map<UUID, Set<Object>> getSurveyPartOfEntityData( UUID studyId ) {
        return ImmutableMap.of(
                edmCacheManager.getPropertyTypeId( OL_ID_FQN ), ImmutableSet.of( studyId )
        );
    }

    // associate entityKeys with EKIDS
    private Map<EntityKey, UUID> createEntityKeyIdMap(
            Set<EntityKey> entityKeys,
            Set<Triple<EntityKey, EntityKey, EntityKey>> edgesByEntityKey,
            EntitySetsConfig entitySetsConfig,
            DataIntegrationApi integrationApi,
            UUID studyId,
            UUID studyEKID,
            UUID participantEKID,
            String participantId
    ) {
        Map<EntityKey, UUID> entityKeyIdMap = Maps.newHashMap();

        Set<EntityKey> orderedEntityKeys = Sets.newHashSet( entityKeys );

        // entity set ids
        UUID studyEntitySetId = entitySetsConfig.getStudiesEntitySetId();
        UUID participantEntitySetId = entitySetsConfig.getParticipantEntitySetId();

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

        EntityKey studyEK = getStudyEntityKey( studyEntitySetId, studyId );
        entityKeyIdMap.put( studyEK, studyEKID );

        EntityKey participantEK = getParticipantEntityKey( participantEntitySetId, participantId );
        entityKeyIdMap.put( participantEK, participantEKID );

        return entityKeyIdMap;
    }

    private ListMultimap<UUID, DataAssociation> getDataAssociations(
            Map<FullQualifiedName, Set<Object>> entity,
            Map<EntityKey, UUID> entityKeyIdMap,
            EntitySetsConfig entitySetsConfig,
            UUID participantEKID,
            UUID submissionEKID,
            OffsetDateTime dateTime,
            int index
    ) {
        ListMultimap<UUID, DataAssociation> associations = ArrayListMultimap.create();

        Map<UUID, Set<Object>> questionEntityData = getQuestionEntityData( entity );
        EntityKey questionEK = getQuestionEntityKey( entitySetsConfig.getQuestionsEntitySetId(), questionEntityData );
        UUID questionEKID = entityKeyIdMap.get( questionEK );

        Map<UUID, Set<Object>> timeRangeEntityData = getTimeRangeEntity( entity );
        EntityKey timeRangeEK = getTimeRangeEntityKey( entitySetsConfig.getTimeRangeEntitySetId(), timeRangeEntityData );
        UUID timeRangeEKID = entityKeyIdMap.get( timeRangeEK );

        // answer -> registeredfor -> timerange
        Map<UUID, Set<Object>> registeredForEntity = getRegisteredForEntity( dateTime );
        associations.put( entitySetsConfig.getRegisteredForEntitySetId(), new DataAssociation(
                entitySetsConfig.getAnswersEntitySetId(),
                Optional.of( index ),
                Optional.empty(),
                entitySetsConfig.getTimeRangeEntitySetId(),
                Optional.empty(),
                Optional.of( timeRangeEKID ),
                registeredForEntity
        ) );

        // answer -> addresses -> question
        Map<UUID, Set<Object>> addressesEntity = getAddressesEntity( dateTime );
        associations.put( entitySetsConfig.getAddressesEntitySetId(), new DataAssociation(
                entitySetsConfig.getAnswersEntitySetId(),
                Optional.of( index ),
                Optional.empty(),
                entitySetsConfig.getQuestionsEntitySetId(),
                Optional.empty(),
                Optional.of( questionEKID ),
                addressesEntity
        ) );

        // answer -> registeredfor -> submission
        associations.put( entitySetsConfig.getRegisteredForEntitySetId(), new DataAssociation(
                entitySetsConfig.getAnswersEntitySetId(),
                Optional.of( index ),
                Optional.empty(),
                entitySetsConfig.getSubmissionEntitySetId(),
                Optional.empty(),
                Optional.of( submissionEKID ),
                registeredForEntity
        ) );

        // participant -> respondswith -> answer
        Map<UUID, Set<Object>> respondWithEntity = ImmutableMap.of(
                edmCacheManager.getPropertyTypeId( DATE_TIME_FQN ), ImmutableSet.of( dateTime )
        );
        associations.put( entitySetsConfig.getRespondsWithEntitySetId(), new DataAssociation(
                entitySetsConfig.getParticipantEntitySetId(),
                Optional.empty(),
                Optional.of( participantEKID ),
                entitySetsConfig.getAnswersEntitySetId(),
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

    private EntitiesAndEdges getEdgesAndEntityKeys(
            EntitySetsConfig entitySetsConfig,
            List<Map<FullQualifiedName, Set<Object>>> surveyData,
            UUID studyId,
            String participantId,
            OffsetDateTime dateTime
    ) {
        Set<Triple<EntityKey, EntityKey, EntityKey>> edgesByEntityKey = Sets.newHashSet(); // <src, association, dst>
        Map<EntityKey, Map<UUID, Set<Object>>> entitiesByEK = Maps.newHashMap();

        // edge: survey -> partof -> study

        Map<UUID, Set<Object>> surveyEntityData = getSurveyEntityData();
        EntityKey surveyEK = getSurveyEntityKey( entitySetsConfig.getSurveysEntitySetId(), surveyEntityData );
        entitiesByEK.put( surveyEK, surveyEntityData );

        Map<UUID, Set<Object>> surveyPartOfEntityData = getSurveyPartOfEntityData( studyId );
        EntityKey surveyPartOfEK = getSurveyPartOfStudyEntityKey( entitySetsConfig.getPartOfEntitySetId(),
                surveyPartOfEntityData );
        entitiesByEK.put( surveyPartOfEK, surveyPartOfEntityData );

        EntityKey studyEK = getStudyEntityKey( entitySetsConfig.getStudiesEntitySetId(), studyId );
        edgesByEntityKey.add( Triple.of( surveyEK, surveyPartOfEK, studyEK ) );

        // edge: person -> participatedin -> survey

        Map<UUID, Set<Object>> participatedInEntityData = getParticipatedInEntity( studyId, participantId, dateTime );
        EntityKey participatedInEK = getParticipatedInEntityKey( entitySetsConfig.getParticipatedInEntitySetId(),
                participatedInEntityData );
        entitiesByEK.put( participatedInEK, participatedInEntityData );

        EntityKey participantEK = getParticipantEntityKey( entitySetsConfig.getParticipantEntitySetId(), participantId );

        edgesByEntityKey.add( Triple.of( participantEK, participatedInEK, surveyEK ) );

        // edge: person -> respondswith -> submission

        Map<UUID, Set<Object>> submissionEntityData = getSubmissionEntity( studyId, participantId, dateTime );
        EntityKey submissionEK = getSubmissionEntityKey( entitySetsConfig.getSubmissionEntitySetId(), submissionEntityData );
        submissionEntityData.remove( edmCacheManager.getPropertyTypeId( STRING_ID_FQN ) ); // shouldn't be stored
        submissionEntityData.remove( edmCacheManager.getPropertyTypeId( OL_ID_FQN ) ); // shouldn't be stored
        entitiesByEK.put( submissionEK, submissionEntityData );

        Map<UUID, Set<Object>> respondsWithSubmissionEntity = getRespondsWithSubmissionEntity( studyId,
                participantId,
                dateTime );
        EntityKey respondsWithEK = getRespondsWithEntityKey( entitySetsConfig.getRespondsWithEntitySetId(),
                respondsWithSubmissionEntity );
        respondsWithSubmissionEntity
                .remove( edmCacheManager.getPropertyTypeId( STRING_ID_FQN ) ); // shouldn't be stored
        entitiesByEK.put( respondsWithEK, respondsWithSubmissionEntity );

        edgesByEntityKey.add( Triple.of( participantEK, respondsWithEK, submissionEK ) );

        surveyData
                .forEach( entity -> {
                    // edge: question -> partof -> survey
                    Map<UUID, Set<Object>> questionEntityData = getQuestionEntityData( entity );
                    EntityKey questionEK = getQuestionEntityKey( entitySetsConfig.getQuestionsEntitySetId(),
                            questionEntityData );
                    entitiesByEK.put( questionEK, questionEntityData );

                    Map<UUID, Set<Object>> questionPartOfSurveyEntity = getQuestionPartOfEntity( entity );
                    EntityKey partOfEK = getQuestionPartOfSurveyEntityKey( entitySetsConfig.getPartOfEntitySetId(),
                            questionPartOfSurveyEntity );
                    entitiesByEK.put( partOfEK, questionPartOfSurveyEntity );

                    edgesByEntityKey.add( Triple.of( questionEK, partOfEK, surveyEK ) );

                    //edge: answer -> registeredfor -> timerange
                    Map<UUID, Set<Object>> timeRangeEntityData = getTimeRangeEntity( entity );
                    EntityKey timeRangeEK = getTimeRangeEntityKey( entitySetsConfig.getTimeRangeEntitySetId(),
                            timeRangeEntityData );
                    entitiesByEK.put( timeRangeEK, timeRangeEntityData );
                } );

        return new EntitiesAndEdges( entitiesByEK, edgesByEntityKey );
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

    // not available for legacy chronicle; should enforce that organizationId is not null
    @Override
    public void submitTimeUseDiarySurvey(
            UUID orgId,
            UUID studyId,
            String participantId,
            List<Map<FullQualifiedName, Set<Object>>> surveyData ) {

        Preconditions.checkNotNull( orgId, "organizationId is required when submitting Time Use Diary" );

        logger.info( "attempting to submit time use diary survey: orgId = {}, studyId = {}, participantId = {}",
                orgId,
                studyId,
                participantId );

        try {
            ApiClient apiClient = apiCacheManager.prodApiClientCache.get( ApiClient.class );
            DataIntegrationApi integrationApi = apiClient.getDataIntegrationApi();
            DataApi dataApi = apiClient.getDataApi();

            // participant must exist
            UUID participantEKID = Preconditions
                    .checkNotNull( enrollmentManager.getParticipantEntityKeyId( orgId, studyId, participantId ),
                            "participant not found" );
            UUID studyEKID = Preconditions
                    .checkNotNull( enrollmentManager.getStudyEntityKeyId( orgId, studyId ), "study not found" );

            // entity set ids
            EntitySetsConfig entitySetsConfig = entitySetIdsManager.getEntitySetsConfig( orgId, studyId, ImmutableSet.of(CHRONICLE_SURVEYS) );

            ListMultimap<UUID, Map<UUID, Set<Object>>> entities = ArrayListMultimap.create();
            ListMultimap<UUID, DataAssociation> associations = ArrayListMultimap.create();

            Map<EntityKey, Map<UUID, Set<Object>>> entitiesByEntityKey = Maps.newHashMap();
            Set<Triple<EntityKey, EntityKey, EntityKey>> edgesByEntityKey = Sets
                    .newHashSet(); // <src, association, dst>

            OffsetDateTime dateTime = OffsetDateTime.now();

            EntitiesAndEdges edgesAndEntityKeys = getEdgesAndEntityKeys(
                    entitySetsConfig,
                    surveyData,
                    studyId,
                    participantId,
                    dateTime
            );
            edgesByEntityKey.addAll( edgesAndEntityKeys.getSrcEdgeDstEntityKeys() );
            entitiesByEntityKey.putAll( edgesAndEntityKeys.getEntityByEntityKey() );

            Map<EntityKey, UUID> entityKeyIdMap = createEntityKeyIdMap(
                    entitiesByEntityKey.keySet(),
                    edgesByEntityKey,
                    entitySetsConfig,
                    integrationApi,
                    studyId,
                    studyEKID,
                    participantEKID,
                    participantId );

            Map<UUID, Set<Object>> submissionEntityData = getSubmissionEntity( studyId, participantId, dateTime );
            UUID submissionESID = entitySetsConfig.getSubmissionEntitySetId();

            EntityKey submissionEK = getSubmissionEntityKey( submissionESID, submissionEntityData );
            UUID submissionEKID = entityKeyIdMap.get( submissionEK );

            for ( int i = 0; i < surveyData.size(); ++i ) {
                Map<FullQualifiedName, Set<Object>> entity = surveyData.get( i );

                associations.putAll( getDataAssociations(
                        entity,
                        entityKeyIdMap,
                        entitySetsConfig,
                        participantEKID,
                        submissionEKID,
                        dateTime,
                        i ) );

                // entities
                Map<UUID, Set<Object>> answerEntity = getAnswerEntity( entity );
                entities.put( entitySetsConfig.getAnswersEntitySetId(), answerEntity );
            }

            // update entities
            Map<UUID, Map<UUID, Map<UUID, Set<Object>>>> entitiesByEntitySet = getEntitiesByESID( entityKeyIdMap,
                    entitiesByEntityKey );
            entitiesByEntitySet.forEach( ( entitySetId, groupedEntities ) -> {
                dataApi.updateEntitiesInEntitySet( entitySetId,
                        groupedEntities,
                        UpdateType.PartialReplace,
                        PropertyUpdateType.Versioned );
            } );

            DataGraph dataGraph = new DataGraph( entities, associations );
            dataApi.createEntityAndAssociationData( dataGraph );

            // create edges
            Set<DataEdgeKey> dataEdgeKeys = getDataEdgeKeysFromEntityKeys( edgesByEntityKey, entityKeyIdMap );
            dataApi.createEdges( dataEdgeKeys );

            logger.info( "successfully submitted time use diary survey: orgId = {}, studyId = {}, participantId = {}",
                    orgId,
                    studyId,
                    participantId );

        } catch ( Exception e ) {
            logger.error( "failed to submit time use diary survey: orgId = {}, studyId = {}, participantId = {}",
                    orgId,
                    studyId,
                    participantId,
                    e );
            throw new RuntimeException( "error submitting time use dairy responses" );
        }
    }
}
