package com.openlattice.chronicle.storage

import com.openlattice.chronicle.storage.PostgresColumns.Companion.DATE_OF_BIRTH
import com.openlattice.chronicle.storage.PostgresColumns.Companion.DESCRIPTION
import com.openlattice.chronicle.storage.PostgresColumns.Companion.FIRST_NAME
import com.openlattice.chronicle.storage.PostgresColumns.Companion.LAST_NAME
import com.openlattice.chronicle.storage.PostgresColumns.Companion.NAME
import com.openlattice.chronicle.storage.PostgresColumns.Companion.ORGANIZATION_ID
import com.openlattice.chronicle.storage.PostgresColumns.Companion.PARTICIPANT_ID
import com.openlattice.chronicle.storage.PostgresColumns.Companion.SETTINGS
import com.openlattice.chronicle.storage.PostgresColumns.Companion.STUDY_ID
import com.openlattice.chronicle.storage.PostgresColumns.Companion.TITLE
import com.openlattice.postgres.PostgresTableDefinition

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
                        ORGANIZATION_ID,
                        STUDY_ID,
                        TITLE,
                        DESCRIPTION,
                        SETTINGS
                )
                .primaryKey(STUDY_ID)

        @JvmField
        val STUDY_PARTICIPATION = PostgresTableDefinition("study_participation")
                .addColumns(
                        ORGANIZATION_ID,
                        STUDY_ID,
                        PARTICIPANT_ID,
                )
                .primaryKey(STUDY_ID, PARTICIPANT_ID)

        @JvmField
        val PARTICIPANTS = PostgresTableDefinition("participants")
                .addColumns(
                        PARTICIPANT_ID,
                        TITLE,
                        NAME,
                        FIRST_NAME,
                        LAST_NAME,
                        DATE_OF_BIRTH,
                        DESCRIPTION,
                        SETTINGS
                )
                .primaryKey(PARTICIPANT_ID)
    }
}