package com.openlattice.chronicle.constants;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import java.util.Map;

import static com.openlattice.chronicle.constants.CollectionTemplateTypeName.*;
import static java.util.Map.entry;

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
public class EdmConstants {

    private EdmConstants() {
    }

    // entity set names
    public static final String APPS_DICTIONARY_ES   = "chronicle_application_dictionary";
    public static final String ADDRESSES_ES         = "chronicle_addresses";
    public static final String ANSWERS_ES           = "chronicle_answers";
    public static final String USER_APPS_ES         = "chronicle_user_apps";
    public static final String DATA_ES              = "chronicle_app_data";
    public static final String DEVICES_ES           = "chronicle_device";
    public static final String HAS_ES               = "chronicle_has";
    public static final String METADATA_ES          = "chronicle_metadata";
    public static final String NOTIFICATION_ES      = "chronicle_notifications";
    public static final String PARTICIPANTS_PREFIX  = "chronicle_participants_";
    public static final String PARTICIPATED_IN_ES   = "chronicle_participated_in";
    public static final String PART_OF_ES           = "chronicle_partof";
    public static final String PREPROCESSED_DATA_ES = "chronicle_preprocessed_app_data";
    public static final String QUESTIONNAIRE_ES     = "chronicle_questionnaires";
    public static final String QUESTIONS_ES         = "chronicle_questions";
    public static final String RECORDED_BY_ES       = "chronicle_recorded_by";
    public static final String REGISTERED_FOR_ES    = "chronicle_registered_for";
    public static final String RESPONDS_WITH_ES     = "chronicle_respondswith";
    public static final String STUDY_ES             = "chronicle_study";
    public static final String SUBMISSION_ES        = "chronicle_submission";
    public static final String TIMERANGE_ES         = "chronicle_timerange";
    public static final String USED_BY_ES           = "chronicle_used_by";

    public static final Map<String, CollectionTemplateTypeName> LEGACY_DATASET_COLLECTION_TEMPLATE_MAP = Map
            .ofEntries(
                    entry( ADDRESSES_ES, ADDRESSES ),
                    entry( ANSWERS_ES, ANSWER ),
                    entry( APPS_DICTIONARY_ES, APP_DICTIONARY ),
                    entry( USER_APPS_ES, USER_APPS ),
                    entry( DATA_ES, APPDATA ),
                    entry( DEVICES_ES, DEVICE ),
                    entry( HAS_ES, HAS ),
                    entry( METADATA_ES, METADATA ),
                    entry( NOTIFICATION_ES, NOTIFICATION ),
                    entry( PARTICIPATED_IN_ES, PARTICIPATED_IN ),
                    entry( PART_OF_ES, PART_OF ),
                    entry( PREPROCESSED_DATA_ES, PREPROCESSED_DATA ),
                    entry( QUESTIONNAIRE_ES, SURVEY ),
                    entry( QUESTIONS_ES, QUESTION ),
                    entry( RECORDED_BY_ES, RECORDED_BY ),
                    entry( RESPONDS_WITH_ES, RESPONDS_WITH ),
                    entry( REGISTERED_FOR_ES, REGISTERED_FOR ),
                    entry( STUDY_ES, STUDIES ),
                    entry( SUBMISSION_ES, SUBMISSION ),
                    entry( TIMERANGE_ES, TIME_RANGE ),
                    entry( USED_BY_ES, USED_BY )
            );

    // property types
    public static final FullQualifiedName COMPLETED_DATE_TIME_FQN = new FullQualifiedName( "date.completeddatetime" );
    public static final FullQualifiedName DATE_LOGGED_FQN         = new FullQualifiedName( "ol.datelogged" );
    public static final FullQualifiedName DATE_TIME_FQN           = new FullQualifiedName( "ol.datetime" );
    public static final FullQualifiedName END_DATE_TIME_FQN       = new FullQualifiedName( "ol.datetimeend" );
    public static final FullQualifiedName FULL_NAME_FQN           = new FullQualifiedName( "general.fullname" );
    public static final FullQualifiedName MODEL_FQN               = new FullQualifiedName( "vehicle.model" );
    public static final FullQualifiedName OL_ID_FQN               = new FullQualifiedName( "ol.id" );
    public static final FullQualifiedName PERSON_ID_FQN           = new FullQualifiedName( "nc.SubjectIdentification" );
    public static final FullQualifiedName RECORDED_DATE_TIME_FQN  = new FullQualifiedName( "ol.recordeddate" );
    public static final FullQualifiedName RECORD_TYPE_FQN         = new FullQualifiedName( "ol.recordtype" );
    public static final FullQualifiedName START_DATE_TIME_FQN     = new FullQualifiedName( "ol.datetimestart" );
    public static final FullQualifiedName STATUS_FQN              = new FullQualifiedName( "ol.status" );
    public static final FullQualifiedName STRING_ID_FQN           = new FullQualifiedName( "general.stringid" );
    public static final FullQualifiedName TIMEZONE_FQN            = new FullQualifiedName( "ol.timezone" );
    public static final FullQualifiedName TITLE_FQN               = new FullQualifiedName( "ol.title" );
    public static final FullQualifiedName VALUES_FQN              = new FullQualifiedName( "ol.values" );
    public static final FullQualifiedName VERSION_FQN             = new FullQualifiedName( "ol.version" );
}