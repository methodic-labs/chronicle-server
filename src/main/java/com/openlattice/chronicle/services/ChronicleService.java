package com.openlattice.chronicle.services;
import com.hazelcast.core.HazelcastInstance;

import java.util.UUID;

public class ChronicleService {

    public ChronicleService(HazelcastInstance hazelcast) {

    }

    public void logData( studyId, participantId ) {

    }

    public void enrollDevice( studyId, participantId, deviceId ) {

    }

    public UUID createStudy( study ) {

    }

    public void deleteStudy( studyId ) {

    }

    public Iterable<Study> getAllStudies() {

    }

    public Study getStudyById( studyId ) {

    }

    public UUID createParticipant( participant ) {

    }

    public void deleteParticipant( participantId ) {

    }

    public Iterable<Person> getAllParticipants() {

    }

    public Iterable<Person> getParticipantsFromStudy( studyId ) {

    }

    public Person getParticipantById( participantId ) {

    }

    public void updateParticipantMetadata( participantId, metadataupdate ) {

    }

    public void updateStudyMetadata( studyId, metadataupdate ) {

    }

    public void addParticipantToStudy( studyId, participantId ) {

    }

    public void removeParticipantFromStudy( studyId, participantId ) {

    }

    public void addParticipantsToStudy( studyId, participantId ) {

    }

    public void removeParticipantsFromStudy( studyId, participantId ) {

    }

}