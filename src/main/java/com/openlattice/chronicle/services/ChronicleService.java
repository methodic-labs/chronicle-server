package com.openlattice.chronicle.services;

import com.google.common.eventbus.EventBus;
import com.hazelcast.core.HazelcastInstance;
import com.openlattice.hazelcast.HazelcastMap;
import com.openlattice.authorization.HazelcastAclKeyReservationService;

import javax.inject.Inject;
import java.util.UUID;

public class ChronicleService {
    private final IMap<UUID, Study>                    studies;
    private final IMap<UUID, Person>                   participants;

    private final HazelcastAclKeyReservationService    reservations;

    @Inject
    private EventBus eventBus;

    public ChronicleService(
            HazelcastInstance hazelcast,
            HazelcastAclKeyReservationService reservations) {
        this.studies = hazelcast.getMap( HazelcastMap.STUDIES.name() );
        this.participants = hazelcast.getMap( HazelcastMap.PARTICIPANTS.name() );
        this.reservations = reservations;
    }

    public void logData( studyId, participantId ) {
//        TODO
    }

    public void enrollDevice( studyId, participantId, deviceId ) {
//        TODO
    }

    public UUID createStudy( study ) {
        reservations.reserveIdAndValidateType( study, study::getName );
        studies.putIfAbsent( study.getId(), study );
//        TODO: edm.events.StudyCreatedEvent
        eventBus.post( new StudyCreatedEvent( study ) );
        return study.getId();

    }

    public void deleteStudy( studyId ) {
        studies.delete( studyId );
//        Don't release the studyId because it might conflict with external records
//        TODO: edm.events.StudyDeletedEvent
        eventBus.post( new StudyDeletedEvent( studyId ) );

    }

    public Iterable<Study> getAllStudies() {
        return studies.values();
    }

    public Study getStudyById( studyId ) {
        return studies.get( studyId );
    }

    public UUID createParticipant( participant ) {
        reservations.reserveIdAndValidateType( participant, participant::getName );
        studies.putIfAbsent( participant.getId(), participant );
//        TODO: edm.events.PersonCreatedEvent
        eventBus.post( new PersonCreatedEvent( participant ) );
        return participant.getId();
    }

    public void deleteParticipant( participantId ) {
        participants.delete( participantId );
//        Don't release the participantId because it might conflict with external records
//        TODO: edm.events.PersonDeletedEvent
        eventBus.post( new PersonDeletedEvent( studyId ) );
    }

    public Iterable<Person> getAllParticipants() {
//        Here I only want people who are created from the chronicle web, not all people.
//        We will be displaying them by ID not name, as they will not have names
//        TODO
    }

    public Iterable<Person> getParticipantsFromStudy( studyId ) {
//        TODO: This may be a front end task. Or can I do studies.getAll(participants)?
    }

    public Person getParticipantById( participantId ) {
        return participants.get( participantId );
    }

    public void updateParticipantMetadata( participantId, metadataUpdate ) {
//        TODO: UpdatePersonMetadataProcessor
        participants.executeOnKey( participantId, new UpdatePersonMetadataProcessor( metadataUpdate ) );
        eventBus.post( new PersonCreatedEvent( participants.get( participantId ) ) );
    }

    public void updateStudyMetadata( studyId, metadataUpdate ) {
//        TODO: UpdateStudyMetadataProcessor
        studies.executeOnKey( studyId, new UpdateStudyMetadataProcessor( metadataUpdate ) );
        eventBus.post( new StudyCreatedEvent( studies.get( studyId ) ) );
    }

    public void addParticipantToStudy( studyId, participantId ) {
//        TODO
    }

    public void removeParticipantFromStudy( studyId, participantId ) {
//        TODO
    }

    public void addParticipantsToStudy( studyId, participantId ) {
//        TODO
    }

    public void removeParticipantsFromStudy( studyId, participantId ) {
//        TODO
    }

}