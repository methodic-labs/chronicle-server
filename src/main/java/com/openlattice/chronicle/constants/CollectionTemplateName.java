package com.openlattice.chronicle.constants;

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
public enum CollectionTemplateName {
    STUDIES( "studies" ),
    PARTICIPANTS( "participants" ),
    NOTIFICATION( "notification" ),
    PART_OF( "partof" ),
    METADATA( "metadata" ),
    HAS( "has" ),
    PARTICIPATED_IN( "participatedin" ),
    QUESTION( "question" ),
    USER_APPS( "userapps" ),
    ANSWER( "answer" ),
    ADDRESSES( "addresses" ),
    RESPONDS_WITH( "respondswith" ),
    REGISTERED_FOR( "registeredfor" ),
    SUBMISSION( "submission" ),
    TIME_RANGE( "timerange" ),
    APP_DATA( "appdata" ),
    SURVEY( "SURVEY" ),
    PREPROCESSED_DATA( "preprocesseddata" ),
    APPS_DICTIONARY( "dictionary" ),
    DEVICE( "device" ),
    USED_BY( "usedby" ),
    RECORDED_BY( "recordedby" );

    private final String template;

    CollectionTemplateName( String template ) {
        this.template = template;
    }

    @Override
    public String toString() {
        return template;
    }
}
