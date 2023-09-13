package com.openlattice.chronicle.util.tests

import com.google.common.collect.HashMultimap
import com.google.common.collect.ImmutableList
import com.google.common.collect.LinkedHashMultimap
import com.google.common.collect.SetMultimap
import com.openlattice.chronicle.android.ChronicleData
import com.openlattice.chronicle.android.ChronicleUsageEvent
import com.openlattice.chronicle.android.LegacyChronicleData
import com.openlattice.chronicle.authorization.*
import com.openlattice.chronicle.candidates.Candidate
import com.openlattice.chronicle.constants.EdmConstants
import com.openlattice.chronicle.data.ParticipationStatus
import com.openlattice.chronicle.notifications.StudyNotificationSettings
import com.openlattice.chronicle.organizations.ChronicleDataCollectionSettings
import com.openlattice.chronicle.organizations.OrganizationPrincipal
import com.openlattice.chronicle.participants.Participant
import com.openlattice.chronicle.participants.ParticipantStats
import com.openlattice.chronicle.sensorkit.SensorSetting
import com.openlattice.chronicle.sensorkit.SensorType
import com.openlattice.chronicle.services.legacy.LegacyEdmResolver
import com.openlattice.chronicle.settings.AppUsageFrequency
import com.openlattice.chronicle.sources.AndroidDevice
import com.openlattice.chronicle.study.*
import com.openlattice.chronicle.survey.SurveySettings
import com.openlattice.chronicle.timeusediary.TimeUseDiarySettings
import org.apache.commons.lang3.RandomStringUtils
import org.apache.commons.lang3.RandomUtils
import org.apache.commons.text.CharacterPredicates
import org.apache.commons.text.RandomStringGenerator
import java.time.LocalDate
import java.time.OffsetDateTime
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class TestDataFactory {
    companion object {
        private val actions = Action.values()
        private val r = Random()
        private val permissions = Permission.values()
        private val securableObjectTypes = SecurableObjectType.values()
        private val studyFeatures = StudyFeature.values()
        private val studySettings = StudySettingType.values()
        private val sensorTypes = SensorType.values()
        private val allowedDigitsAndLetters = arrayOf(
            charArrayOf('a', 'z'), charArrayOf('A', 'Z'), charArrayOf('0', '9')
        )
        private val randomAlphaNumeric: RandomStringGenerator = org.apache.commons.text.RandomStringGenerator.Builder()
            .withinRange(*allowedDigitsAndLetters)
            .filteredBy(CharacterPredicates.LETTERS, CharacterPredicates.DIGITS)
            .build()

        fun <T> randomSubset(values: Array<T>): Set<T> {
            return values.filter { r.nextBoolean() }.toSet()
        }

        fun randomFeatures(): Map<StudyFeature, Any> {
            val numFeatures = 1 + r.nextInt(studyFeatures.size)
            return (0 until numFeatures).associate { studyFeatures[it] to mapOf<String, Any>() }
        }

        fun randomSettings(): StudySettings {
            val numFeatures = 1 + r.nextInt(studySettings.size)
            return StudySettings((0 until numFeatures).associate {
                studySettings[it] to when (studySettings[it]) {
                    StudySettingType.DataCollection -> ChronicleDataCollectionSettings(if (r.nextBoolean()) AppUsageFrequency.DAILY else AppUsageFrequency.HOURLY)
                    StudySettingType.Sensor -> SensorSetting(randomSubset(sensorTypes))
                    StudySettingType.Notifications -> StudyNotificationSettings(
                        randomAlphanumeric(5),
                        randomAlphanumeric(5),
                        r.nextBoolean()
                    )
                    StudySettingType.TimeUseDiary -> TimeUseDiarySettings()
                    StudySettingType.Survey -> SurveySettings()
                }
            })

        }


        fun randomAlphanumeric(length: Int): String {
            return randomAlphaNumeric.generate(length)
        }

        fun aclKey(): AclKey {
            return AclKey(UUID.randomUUID(), UUID.randomUUID())
        }

        fun role(): Role {
            return Role(
                Optional.of(UUID.randomUUID()),
                UUID.randomUUID(),
                rolePrincipal(),
                randomAlphanumeric(5),
                Optional.of<String>(randomAlphanumeric(5))
            )
        }

        fun role(organizationId: UUID): Role {
            return Role(
                Optional.of(UUID.randomUUID()),
                organizationId,
                rolePrincipal(),
                randomAlphanumeric(5),
                Optional.of<String>(randomAlphanumeric(5))
            )
        }

        fun rolePrincipal(): Principal {
            return Principal(
                PrincipalType.ROLE,
                randomAlphanumeric(5)
            )
        }

        fun userPrincipal(): Principal {
            return Principal(PrincipalType.USER, randomAlphanumeric(10))
        }

        fun ace(): Ace {
            return Ace(
                userPrincipal(),
                permissions()
            )
        }

        fun aceValue(): AceValue {
            return AceValue(
                permissions(),
                securableObjectType()
            )
        }

        fun acl(): Acl {
            return Acl(
                AclKey(UUID.randomUUID(), UUID.randomUUID()),
                ImmutableList.of(ace(), ace(), ace(), ace())
            )
        }

        fun aclData(): AclData {
            return AclData(acl(), actions[r.nextInt(actions.size)])
        }

        @JvmStatic
        fun permissions(): EnumSet<Permission> {
            return permissions
                .filter { r.nextBoolean() }
                .toCollection(EnumSet.noneOf(Permission::class.java))
        }

        fun nonEmptyPermissions(): EnumSet<Permission> {
            var ps: EnumSet<Permission> = permissions()
            while (ps.isEmpty()) {
                ps = permissions()
            }
            return ps
        }

        fun securableObjectType(): SecurableObjectType {
            return securableObjectTypes[r.nextInt(securableObjectTypes.size)]

        }

        fun securablePrincipal(type: PrincipalType): SecurablePrincipal {
            val principal: Principal = when (type) {
                PrincipalType.ROLE -> rolePrincipal()
                PrincipalType.ORGANIZATION -> organizationPrincipal()
                PrincipalType.USER -> userPrincipal()
                else -> userPrincipal()
            }
            return SecurablePrincipal(
                AclKey(UUID.randomUUID()),
                principal,
                randomAlphanumeric(10),
                Optional.of<String>(randomAlphanumeric(10))
            )
        }

        fun organizationPrincipal(): Principal {
            return Principal(
                PrincipalType.ORGANIZATION,
                randomAlphanumeric(
                    10
                )
            )
        }

        fun securableOrganizationPrincipal(): OrganizationPrincipal {
            return OrganizationPrincipal(
                Optional.of(UUID.randomUUID()),
                organizationPrincipal(),
                randomAlphanumeric(5),
                Optional.of<String>(randomAlphanumeric(10))
            )
        }

        fun candidate(): Candidate {
            return Candidate(
                firstName = RandomStringUtils.randomAlphabetic(10),
                lastName = RandomStringUtils.randomAlphabetic(10),
                dateOfBirth = LocalDate.of(1920 + r.nextInt(100), 1 + r.nextInt(11), 1 + r.nextInt(27)),
                email = "${RandomStringUtils.randomAlphanumeric(10)}@openlattice.com"
            )
        }

        fun participant(participationStatus: ParticipationStatus = ParticipationStatus.ENROLLED): Participant {
            return Participant(
                RandomStringUtils.randomAlphanumeric(8),
                candidate(),
                participationStatus
            )
        }


        fun study(): Study {
            return Study(
                title = "This is a test study.",
                contact = "${RandomStringUtils.randomAlphabetic(5)}@openlattice.com",
                settings = randomSettings(),
                modules = randomFeatures()
            )
        }

        fun androidDevice() : AndroidDevice {
            return AndroidDevice(
                randomAlphanumeric(5),
                randomAlphanumeric(5),
                randomAlphanumeric(5),
                randomAlphanumeric(5),
                randomAlphanumeric(5),
                randomAlphanumeric(5),
                randomAlphanumeric(5),
                randomAlphanumeric(5)
            )
        }

        fun legacyChronicleUsageEvents(count: Int = 10): LegacyChronicleData {
            val usageEvents = (0 until count).map {
                val mm : SetMultimap<UUID, Any> = LinkedHashMultimap.create()
                mm.put(LegacyEdmResolver.getPropertyTypeId(EdmConstants.FULL_NAME_FQN), randomAlphanumeric(5))
                mm.put(LegacyEdmResolver.getPropertyTypeId(EdmConstants.RECORD_TYPE_FQN), randomAlphanumeric(5))
                mm.put(LegacyEdmResolver.getPropertyTypeId(EdmConstants.DATE_LOGGED_FQN), OffsetDateTime.now())
                mm.put(LegacyEdmResolver.getPropertyTypeId(EdmConstants.TIMEZONE_FQN), TimeZone.getDefault().id)
                mm.put(LegacyEdmResolver.getPropertyTypeId(EdmConstants.USER_FQN), randomAlphanumeric(5))
                mm.put(LegacyEdmResolver.getPropertyTypeId(EdmConstants.TITLE_FQN), randomAlphanumeric(5))
                return@map mm
            }

            return LegacyChronicleData(usageEvents)
        }


        fun chronicleUsageEvents(studyId: UUID, participantId: String, count: Int = 10): ChronicleData {
            val usageEvents = (0 until count).map {
                ChronicleUsageEvent(
                    studyId,
                    participantId,
                    RandomStringUtils.randomAlphanumeric(5),
                    RandomStringUtils.randomAlphanumeric(5),
                    RandomUtils.nextInt(0,100),
                    OffsetDateTime.now(),
                    TimeZone.getDefault().id,
                    RandomStringUtils.randomAlphanumeric(5),
                    RandomStringUtils.randomAlphanumeric(5)
                )
            }

            return ChronicleData( usageEvents )
        }

        fun participantStats(): ParticipantStats {
            return ParticipantStats(
                UUID.randomUUID(),
                RandomStringUtils.randomAlphanumeric(8),
                OffsetDateTime.now(),
                OffsetDateTime.now(),
                OffsetDateTime.now(),
                setOf(LocalDate.now()),
                OffsetDateTime.now(),
                OffsetDateTime.now(),
                OffsetDateTime.now(),
                setOf(LocalDate.now()),
                OffsetDateTime.now(),
                null,
                setOf(LocalDate.now()),
            )
        }
    }
}
