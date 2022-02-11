package com.openlattice.chronicle.storage

import com.geekbeast.postgres.PostgresColumnsIndexDefinition
import com.geekbeast.postgres.PostgresTableDefinition
import com.openlattice.chronicle.storage.PostgresColumns.Companion.ACL_KEY
import com.openlattice.chronicle.storage.PostgresColumns.Companion.APP_USERS
import com.openlattice.chronicle.storage.PostgresColumns.Companion.BASE
import com.openlattice.chronicle.storage.PostgresColumns.Companion.CANDIDATE_ID
import com.openlattice.chronicle.storage.PostgresColumns.Companion.CONTACT
import com.openlattice.chronicle.storage.PostgresColumns.Companion.CREATED_AT
import com.openlattice.chronicle.storage.PostgresColumns.Companion.DATE_OF_BIRTH
import com.openlattice.chronicle.storage.PostgresColumns.Companion.DESCRIPTION
import com.openlattice.chronicle.storage.PostgresColumns.Companion.DEVICE_ID
import com.openlattice.chronicle.storage.PostgresColumns.Companion.EMAIL
import com.openlattice.chronicle.storage.PostgresColumns.Companion.ENDED_AT
import com.openlattice.chronicle.storage.PostgresColumns.Companion.EXPIRATION
import com.openlattice.chronicle.storage.PostgresColumns.Companion.EXPIRATION_DATE
import com.openlattice.chronicle.storage.PostgresColumns.Companion.FIRST_NAME
import com.openlattice.chronicle.storage.PostgresColumns.Companion.LAST_NAME
import com.openlattice.chronicle.storage.PostgresColumns.Companion.LAT
import com.openlattice.chronicle.storage.PostgresColumns.Companion.LON
import com.openlattice.chronicle.storage.PostgresColumns.Companion.LSB
import com.openlattice.chronicle.storage.PostgresColumns.Companion.MSB
import com.openlattice.chronicle.storage.PostgresColumns.Companion.NAME
import com.openlattice.chronicle.storage.PostgresColumns.Companion.NOTIFICATIONS_ENABLED
import com.openlattice.chronicle.storage.PostgresColumns.Companion.ORGANIZATION_ID
import com.openlattice.chronicle.storage.PostgresColumns.Companion.PARTICIPANT_ID
import com.openlattice.chronicle.storage.PostgresColumns.Companion.PARTICIPATION_STATUS
import com.openlattice.chronicle.storage.PostgresColumns.Companion.PARTITION_INDEX
import com.openlattice.chronicle.storage.PostgresColumns.Companion.PHONE_NUMBER
import com.openlattice.chronicle.storage.PostgresColumns.Companion.PRINCIPAL_ID
import com.openlattice.chronicle.storage.PostgresColumns.Companion.PRINCIPAL_OF_ACL_KEY
import com.openlattice.chronicle.storage.PostgresColumns.Companion.PRINCIPAL_TYPE
import com.openlattice.chronicle.storage.PostgresColumns.Companion.SCOPE
import com.openlattice.chronicle.storage.PostgresColumns.Companion.SECURABLE_OBJECT_ID
import com.openlattice.chronicle.storage.PostgresColumns.Companion.SECURABLE_OBJECT_NAME
import com.openlattice.chronicle.storage.PostgresColumns.Companion.SECURABLE_OBJECT_TYPE
import com.openlattice.chronicle.storage.PostgresColumns.Companion.SETTINGS
import com.openlattice.chronicle.storage.PostgresColumns.Companion.SOURCE_DEVICE
import com.openlattice.chronicle.storage.PostgresColumns.Companion.SOURCE_DEVICE_ID
import com.openlattice.chronicle.storage.PostgresColumns.Companion.STARTED_AT
import com.openlattice.chronicle.storage.PostgresColumns.Companion.STORAGE
import com.openlattice.chronicle.storage.PostgresColumns.Companion.STUDY_GROUP
import com.openlattice.chronicle.storage.PostgresColumns.Companion.STUDY_ID
import com.openlattice.chronicle.storage.PostgresColumns.Companion.STUDY_VERSION
import com.openlattice.chronicle.storage.PostgresColumns.Companion.TITLE
import com.openlattice.chronicle.storage.PostgresColumns.Companion.SUBMISSION_ID
import com.openlattice.chronicle.storage.PostgresColumns.Companion.UPDATED_AT
import com.openlattice.chronicle.storage.PostgresColumns.Companion.USER_DATA
import com.openlattice.chronicle.storage.PostgresColumns.Companion.USER_ID
import com.openlattice.chronicle.storage.PostgresColumns.Companion.SUBMISSION
import com.openlattice.chronicle.storage.PostgresColumns.Companion.SUBMISSION_DATE

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class ChroniclePostgresTables {
    companion object {

        @JvmField
        val ORGANIZATIONS = PostgresTableDefinition("organizations")
            .addColumns(
                ORGANIZATION_ID,
                TITLE,
                DESCRIPTION,
                SETTINGS
            )
            .primaryKey(ORGANIZATION_ID)
            .overwriteOnConflict()

        @JvmField
        val STUDIES = PostgresTableDefinition("studies")
            .addColumns(
                STUDY_ID,
                TITLE,
                DESCRIPTION,
                CREATED_AT,
                UPDATED_AT,
                STARTED_AT,
                ENDED_AT,
                LAT,
                LON,
                STUDY_GROUP,
                STUDY_VERSION,
                CONTACT,
                NOTIFICATIONS_ENABLED,
                STORAGE,
                SETTINGS,
            )
            .primaryKey(STUDY_ID)
            .overwriteOnConflict()

        @JvmField
        val LEGACY_STUDY_SETTINGS = PostgresTableDefinition("legacy_study_settings")
            .addColumns(ORGANIZATION_ID, SETTINGS)
            .primaryKey(ORGANIZATION_ID)

        @JvmField
        val ORGANIZATION_STUDIES = PostgresTableDefinition("organization_studies")
            .addColumns(
                ORGANIZATION_ID,
                STUDY_ID,
                USER_ID,
                CREATED_AT,
                SETTINGS
            )
            .primaryKey(ORGANIZATION_ID, STUDY_ID)

        @JvmField
        val STUDY_PARTICIPANTS = PostgresTableDefinition("study_participants")
            .addColumns(
                STUDY_ID,
                PARTICIPANT_ID,
                CANDIDATE_ID,
                PARTICIPATION_STATUS
            )
            .primaryKey(STUDY_ID, PARTICIPANT_ID)

        @JvmField
        val CANDIDATES = PostgresTableDefinition("candidates")
            .addColumns(
                CANDIDATE_ID,
                FIRST_NAME,
                LAST_NAME,
                NAME,
                DATE_OF_BIRTH,
                EMAIL,
                PHONE_NUMBER,
                EXPIRATION_DATE
            )
            .primaryKey(CANDIDATE_ID)

        @JvmField
        val DEVICES = PostgresTableDefinition("DEVICES")
            .addColumns(
                STUDY_ID,
                DEVICE_ID,
                PARTICIPANT_ID, //Make sure this is indexed.
                SOURCE_DEVICE_ID,
                SOURCE_DEVICE
            )
            .primaryKey(STUDY_ID, DEVICE_ID) //Just in case device is used across multiple studies

        @JvmField
        val TIME_USE_DIARY_SUBMISSIONS = PostgresTableDefinition("time_use_diary_submissions")
            .addColumns(
                SUBMISSION_ID,
                ORGANIZATION_ID,
                STUDY_ID,
                PARTICIPANT_ID,
                SUBMISSION_DATE,
                SUBMISSION
            )
            .primaryKey(SUBMISSION_ID)

        @JvmField
        val BASE_LONG_IDS: PostgresTableDefinition = PostgresTableDefinition("base_long_ids")
            .addColumns(SCOPE, BASE)
            .primaryKey(SCOPE)

        @JvmField
        val APP_USAGE_SURVEY: PostgresTableDefinition = PostgresTableDefinition("app_usage_survey")
            .addColumns(
                STUDY_ID,
                PARTICIPANT_ID,
                SUBMISSION_DATE, // date when survey was submitted
                RedshiftColumns.APPLICATION_LABEL,
                RedshiftColumns.APP_PACKAGE_NAME,
                RedshiftColumns.TIMESTAMP, // usage event
                RedshiftColumns.TIMEZONE, // usage event timezone
                APP_USERS
            )
            .primaryKey(RedshiftColumns.APP_PACKAGE_NAME, RedshiftColumns.APP_PACKAGE_NAME, RedshiftColumns.TIMESTAMP)

        /**
         * Authorization tables
         *
         */

        /**
         * Table containing all securable principals
         */
        @JvmField
        val PRINCIPALS = PostgresTableDefinition("principals")
            .addColumns(ACL_KEY, PRINCIPAL_TYPE, PRINCIPAL_ID, TITLE, DESCRIPTION)
            .primaryKey(ACL_KEY)
            .setUnique(PRINCIPAL_TYPE, PRINCIPAL_ID)

        @JvmField
        val PRINCIPAL_TREES = PostgresTableDefinition("principal_trees")
            .addColumns(ACL_KEY, PRINCIPAL_OF_ACL_KEY)
            .primaryKey(ACL_KEY, PRINCIPAL_OF_ACL_KEY)

        @JvmField
        val ID_GENERATION = PostgresTableDefinition("id_gen")
            .primaryKey(PARTITION_INDEX)
            .addColumns(PARTITION_INDEX, MSB, LSB)

        @JvmField
        val PERMISSIONS = PostgresTableDefinition("permissions")
            .addColumns(
                ACL_KEY,
                PRINCIPAL_TYPE,
                PRINCIPAL_ID,
                PostgresColumns.PERMISSIONS,
                PostgresColumns.EXPIRATION_DATE
            )
            .primaryKey(ACL_KEY, PRINCIPAL_TYPE, PRINCIPAL_ID)

        @JvmField
        val SECURABLE_OBJECTS = PostgresTableDefinition("securable_objects")
            .addColumns(ACL_KEY, SECURABLE_OBJECT_TYPE, SECURABLE_OBJECT_ID, SECURABLE_OBJECT_NAME)
            .primaryKey(ACL_KEY)

        @JvmField
        val USERS = PostgresTableDefinition("users")
            .addColumns(USER_ID, USER_DATA, EXPIRATION)
            .primaryKey(USER_ID)
            .overwriteOnConflict()

        init {
            ORGANIZATION_STUDIES
                .addIndexes(PostgresColumnsIndexDefinition(ORGANIZATION_STUDIES, ORGANIZATION_ID).ifNotExists())
            DEVICES
                .addIndexes(
                    //(study id, participant id, datasource id) is bijective with device id
                    PostgresColumnsIndexDefinition(
                        DEVICES,
                        STUDY_ID,
                        PARTICIPANT_ID,
                        SOURCE_DEVICE_ID
                    ).ifNotExists().unique()
                )
        }
    }
}