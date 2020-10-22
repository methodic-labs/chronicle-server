package com.openlattice.chronicle.constants;

import com.google.common.collect.ImmutableSet;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

import java.util.UUID;

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
    public static final String RESPONDS_WITH_ES     = "chronicle_respondswith";
    public static final String STUDY_ES             = "chronicle_study";
    public static final String USED_BY_ES           = "chronicle_used_by";

    public static final ImmutableSet<String> ENTITY_SET_NAMES = ImmutableSet.of(
            ADDRESSES_ES,
            ANSWERS_ES,
            APPS_DICTIONARY_ES,
            USER_APPS_ES,
            DATA_ES,
            DEVICES_ES,
            HAS_ES,
            METADATA_ES,
            NOTIFICATION_ES,
            PARTICIPATED_IN_ES,
            PART_OF_ES,
            PREPROCESSED_DATA_ES,
            QUESTIONNAIRE_ES,
            QUESTIONS_ES,
            RECORDED_BY_ES,
            RESPONDS_WITH_ES,
            STUDY_ES,
            USED_BY_ES
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

    // CAFE ORG: to maintain backward compatibility, all studies before apps v2 will be assumed to belong to CAFE ORG
    public static final UUID CAFE_ORG_ID = UUID.fromString( "7349c446-2acc-4d14-b2a9-a13be39cff93" );
}