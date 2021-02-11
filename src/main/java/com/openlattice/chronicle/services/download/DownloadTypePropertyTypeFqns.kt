package com.openlattice.chronicle.services.download

import com.openlattice.chronicle.constants.EdmConstants.*
import com.openlattice.chronicle.constants.ParticipantDataType

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
class DownloadTypePropertyTypeFqns {
    companion object {
        val SRC = mapOf(
                ParticipantDataType.USAGE_DATA to linkedSetOf(TITLE_FQN, FULL_NAME_FQN),
                ParticipantDataType.RAW_DATA to linkedSetOf(
                        DATE_LOGGED_FQN,
                        TIMEZONE_FQN,
                        TITLE_FQN,
                        FULL_NAME_FQN,
                        RECORD_TYPE_FQN
                ),
                ParticipantDataType.PREPROCESSED to linkedSetOf(
                        NEW_APP_FQN,
                        TIMEZONE_FQN,
                        START_DATE_TIME_FQN,
                        GENERAL_END_TIME_FQN,
                        RECORD_TYPE_FQN,
                        TITLE_FQN,
                        FULL_NAME_FQN,
                        NEW_PERIOD_FQN,
                        DURATION_FQN,
                        WARNING_FQN
                )
        )

        val EDGE = mapOf(
                ParticipantDataType.USAGE_DATA to linkedSetOf(USER_FQN, DATE_TIME_FQN),
                ParticipantDataType.RAW_DATA to linkedSetOf(),
                ParticipantDataType.PREPROCESSED to linkedSetOf()
        )
    }
}