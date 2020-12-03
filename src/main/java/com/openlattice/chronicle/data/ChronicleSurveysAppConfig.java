package com.openlattice.chronicle.data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.UUID;

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 *
 * Represent the surveys component of chronicle and its associated entity set ids
 */
public class ChronicleSurveysAppConfig {
    private final UUID surveyEntitySetId;
    private final UUID timeRangeEntitySetId;
    private final UUID submissionEntitySetId;
    private final UUID registeredForEntitySetId;
    private final UUID respondsWithEntitySetId;
    private final UUID addressesEntitySetId;
    private final UUID questionEntitySetId;
    private final UUID answerEntitySetId;

    @JsonCreator
    public ChronicleSurveysAppConfig(
            @JsonProperty(SerializationConstants.ENTITY_SET_ID)UUID surveyEntitySetId,
            @JsonProperty (SerializationConstants.ENTITY_SET_ID)UUID timeRangeEntitySetId,
            @JsonProperty (SerializationConstants.ENTITY_SET_ID)UUID submissionEntitySetId,
            @JsonProperty (SerializationConstants.ENTITY_SET_ID)UUID registeredForEntitySetId,
            @JsonProperty (SerializationConstants.ENTITY_SET_ID)UUID respondsWithEntitySetId,
            @JsonProperty (SerializationConstants.ENTITY_SET_ID)UUID addressesEntitySetId,
            @JsonProperty (SerializationConstants.ENTITY_SET_ID)UUID answerEntitySetId,
            @JsonProperty (SerializationConstants.ENTITY_SET_ID)UUID questionEntitySetId
    ) {
        this.surveyEntitySetId = surveyEntitySetId;
        this.timeRangeEntitySetId = timeRangeEntitySetId;
        this.submissionEntitySetId = submissionEntitySetId;
        this.registeredForEntitySetId = registeredForEntitySetId;
        this.respondsWithEntitySetId = respondsWithEntitySetId;
        this.addressesEntitySetId = addressesEntitySetId;
        this.questionEntitySetId = questionEntitySetId;
        this.answerEntitySetId = answerEntitySetId;
    }

    @JsonProperty (SerializationConstants.ENTITY_SET_ID)
    public UUID getSurveyEntitySetId() {
        return surveyEntitySetId;
    }

    @JsonProperty (SerializationConstants.ENTITY_SET_ID)
    public UUID getTimeRangeEntitySetId() {
        return timeRangeEntitySetId;
    }

    @JsonProperty (SerializationConstants.ENTITY_SET_ID)
    public UUID getSubmissionEntitySetId() {
        return submissionEntitySetId;
    }

    @JsonProperty (SerializationConstants.ENTITY_SET_ID)
    public UUID getRegisteredForEntitySetId() {
        return registeredForEntitySetId;
    }

    @JsonProperty (SerializationConstants.ENTITY_SET_ID)
    public UUID getRespondsWithEntitySetId() {
        return respondsWithEntitySetId;
    }

    @JsonProperty (SerializationConstants.ENTITY_SET_ID)
    public UUID getAddressesEntitySetId() {
        return addressesEntitySetId;
    }

    @JsonProperty (SerializationConstants.ENTITY_SET_ID)
    public UUID getQuestionEntitySetId() {
        return questionEntitySetId;
    }

    @JsonProperty (SerializationConstants.ENTITY_SET_ID)
    public UUID getAnswerEntitySetId() {
        return answerEntitySetId;
    }
}
