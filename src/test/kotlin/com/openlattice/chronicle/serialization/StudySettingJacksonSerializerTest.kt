package com.openlattice.chronicle.serialization

import com.fasterxml.jackson.module.kotlin.readValue
import com.geekbeast.mappers.mappers.ObjectMappers
import com.geekbeast.serializer.serializer.AbstractJacksonSerializationTest
import com.openlattice.chronicle.study.StudySettings

class StudySettingsJacksonSerializerTest : AbstractJacksonSerializationTest<StudySettings>() {
    override fun getSampleData(): StudySettings {
        return ObjectMappers.getJsonMapper()
            .readValue(content = "{\"Sensor\": [\"com.openlattice.chronicle.sensorkit.SensorSetting\", [\"messagesUsage\", \"deviceUsage\"]], \"Survey\": {\"@class\": \"com.openlattice.chronicle.survey.SurveySettings\", \"appUsageThresholdInSeconds\": 180, \"deviceUsageThresholdInSeconds\": 180}, \"TimeUseDiary\": {\"@class\": \"com.openlattice.chronicle.timeusediary.TimeUseDiarySettings\", \"language\": \"en\", \"enableChangesForSherbrookeUniversity\": false}, \"Notifications\": {\"@class\": \"com.openlattice.chronicle.notifications.StudyNotificationSettings\", \"noDataUploaded\": {\"days\": 1, \"years\": 0, \"months\": 0}, \"noTudSubmitted\": {\"days\": 1, \"years\": 0, \"months\": 0}, \"labFriendlyName\": \"\", \"notifyResearchers\": false, \"studyFriendlyName\": \"App Store Review Study\", \"notifyOnEnrollment\": false, \"researcherPhoneNumbers\": \"\", \"noAppUsageSurveySubmitted\": {\"days\": 1, \"years\": 0, \"months\": 0}}}")
    }

    override fun getClazz(): Class<StudySettings>? {
        return StudySettings::class.java
    }
}


