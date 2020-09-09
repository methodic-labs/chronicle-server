package com.openlattice.chronicle.constants;

import com.google.common.collect.ImmutableSet;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

import java.util.Set;
import java.util.UUID;

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
public class EdmConstants {

    private EdmConstants() {
    }

    // entity set names
    public static final String USER_APPS_DICTIONARY = "chronicle_application_dictionary";

    // property types
    public static final FullQualifiedName COMPLETED_DATE_TIME_FQN = new FullQualifiedName( "date.completeddatetime" );
    public static final FullQualifiedName DATE_LOGGED_FQN         = new FullQualifiedName( "ol.datelogged" );
    public static final FullQualifiedName DATE_TIME_FQN           = new FullQualifiedName( "ol.datetime" );
    public static final FullQualifiedName DURATION_FQN            = new FullQualifiedName( "general.Duration" );
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
    public static final FullQualifiedName USER_FQN                = new FullQualifiedName( "ol.user" );
    public static final FullQualifiedName VALUES_FQN              = new FullQualifiedName( "ol.values" );
    public static final FullQualifiedName VERSION_FQN             = new FullQualifiedName( "ol.version" );

    // entity types
    public static final FullQualifiedName PERSON_FQN = new FullQualifiedName( "general.person" );

    // CAFE ORG: to maintain backward compatibility, all studies before apps v2 will be assumed to belong to CAFE ORG
    public static final UUID CAFE_ORG_ID = UUID.fromString( "7349c446-2acc-4d14-b2a9-a13be39cff93" );

    // app names
    public static final String CHRONICLE_CORE           = "chronicle";
    public static final String DATA_COLLECTION          = "chronicle_data_collection";
    public static final String CHRONICLE_QUESTIONNAIRES = "chronicle_questionnaires";

    public static final Set<String> APP_NAMES = ImmutableSet.of(
            CHRONICLE_CORE,
            CHRONICLE_QUESTIONNAIRES,
            DATA_COLLECTION
    );

    // collection template names
    public static final String STUDIES           = "studies";
    public static final String PARTICIPANTS      = "participants";
    public static final String NOTIFICATION      = "notification";
    public static final String PART_OF           = "partof";
    public static final String METADATA          = "metadata";
    public static final String HAS               = "has";
    public static final String PARTICIPATED_IN   = "participatedin";
    public static final String QUESTION          = "question";
    public static final String USER_APPS         = "userapps";
    public static final String ANSWER            = "answer";
    public static final String ADDRESSES         = "addresses";
    public static final String RESPONDS_WITH     = "respondswith";
    public static final String REGISTERED_FOR    = "registeredfor";
    public static final String SUBMISSION        = "submission";
    public static final String TIME_RANGE        = "timerange";
    public static final String APP_DATA          = "appdata";
    public static final String SURVEY            = "survey";
    public static final String PREPROCESSED_DATA = "preprocesseddata";
    public static final String APPS_DICTIONARY   = "dictionary";
    public static final String DEVICE            = "device";
    public static final String USED_BY           = "usedby";
    public static final String RECORDED_BY       = "recordedby";
}