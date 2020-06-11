package com.openlattice.chronicle.constants;

import com.google.common.collect.ImmutableSet;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
public class EdmConstants {

    private EdmConstants() {
    }

    // entity set names
    public static final String STUDY_ENTITY_SET_NAME             = "chronicle_study";
    public static final String METADATA_ENTITY_SET_NAME          = "chronicle_metadata";
    public static final String DEVICES_ENTITY_SET_NAME           = "chronicle_device";
    public static final String DATA_ENTITY_SET_NAME              = "chronicle_app_data";
    public static final String PREPROCESSED_DATA_ENTITY_SET_NAME = "chronicle_preprocessed_app_data";
    public static final String RECORDED_BY_ENTITY_SET_NAME       = "chronicle_recorded_by";
    public static final String CHRONICLE_USER_APPS               = "chronicle_user_apps";
    public static final String USED_BY_ENTITY_SET_NAME           = "chronicle_used_by";
    public static final String HAS_ENTITY_SET_NAME               = "chronicle_has";
    public static final String PARTICIPATED_IN_AESN              = "chronicle_participated_in";
    public static final String PARTICIPANTS_PREFIX               = "chronicle_participants_";
    public static final String QUESTIONNAIRE_ENTITY_SET_NAME     = "chronicle_questionnaires";
    public static final String QUESTIONS_ENTITY_SET_NAME         = "chronicle_questions";
    public static final String ANSWERS_ENTITY_SET_NAME           = "chronicle_answers";
    public static final String RESPONDS_WITH_ENTITY_SET_NAME     = "chronicle_respondswith";
    public static final String ADDRESSES_ENTITY_SET_NAME         = "chronicle_addresses";
    public static final String NOTIFICATION_ENTITY_SET_NAME      = "chronicle_notifications";
    public static final String PART_OF_ENTITY_SET_NAME           = "chronicle_partof";
    public static final String USER_APPS_DICTIONARY              = "chronicle_application_dictionary";

    public static final ImmutableSet<String> ENTITY_SET_NAMES = ImmutableSet.of(
            ADDRESSES_ENTITY_SET_NAME,
            ANSWERS_ENTITY_SET_NAME,
            CHRONICLE_USER_APPS,
            DATA_ENTITY_SET_NAME,
            DEVICES_ENTITY_SET_NAME,
            NOTIFICATION_ENTITY_SET_NAME,
            PARTICIPATED_IN_AESN,
            PART_OF_ENTITY_SET_NAME,
            QUESTIONNAIRE_ENTITY_SET_NAME,
            QUESTIONS_ENTITY_SET_NAME,
            RECORDED_BY_ENTITY_SET_NAME,
            RESPONDS_WITH_ENTITY_SET_NAME,
            STUDY_ENTITY_SET_NAME,
            USED_BY_ENTITY_SET_NAME,
            USER_APPS_DICTIONARY
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
    public static final FullQualifiedName START_DATE_TIME_FQN     = new FullQualifiedName( "ol.startdatetime" );
    public static final FullQualifiedName STATUS_FQN              = new FullQualifiedName( "ol.status" );
    public static final FullQualifiedName STRING_ID_FQN           = new FullQualifiedName( "general.stringid" );
    public static final FullQualifiedName TIMEZONE_FQN            = new FullQualifiedName( "ol.timezone" );
    public static final FullQualifiedName TITLE_FQN               = new FullQualifiedName( "ol.title" );
    public static final FullQualifiedName VALUES_FQN              = new FullQualifiedName( "ol.values" );
    public static final FullQualifiedName VERSION_FQN             = new FullQualifiedName( "ol.version" );

}