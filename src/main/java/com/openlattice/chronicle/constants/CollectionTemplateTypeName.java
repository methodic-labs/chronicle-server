package com.openlattice.chronicle.constants;

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
public enum CollectionTemplateTypeName {
    ADDRESSES( "addresses" ),
    ANSWER( "answer" ),
    APPDATA( "appdata" ),
    DEVICE( "device" ),
    HAS( "has" ),
    METADATA( "metadata" ),
    NOTIFICATION( "notification" ),
    PARTICIPANTS( "participants" ),
    PARTICIPATED_IN( "participatedin" ),
    PART_OF( "partof" ),
    PREPROCESSED_DATA( "preprocesseddata" ),
    QUESTION( "question" ),
    RECORDED_BY( "recordedby" ),
    REGISTERED_FOR( "registeredfor" ),
    RESPONDS_WITH( "respondswith" ),
    STUDIES( "studies" ),
    SUBMISSION( "submission" ),
    SURVEY( "survey" ),
    TIME_RANGE( "timerange" ),
    USED_BY( "usedby" ),
    USER_APPS( "userapps" );

    private final String template;

    CollectionTemplateTypeName( String template ) {
        this.template = template;
    }

    @Override
    public String toString() {
        return template;
    }
}
