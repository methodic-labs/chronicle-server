/*
 * Copyright (C) 2019. OpenLattice, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@openlattice.com
 *
 *
 */

package com.openlattice.chronicle.auditing

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
enum class AuditEventType {
    ACCESS_DENIED,
    ASSOCIATE_STUDY,
    BACKGROUND_USAGE_DATA_DELETION,
    CREATE_JOB,
    CREATE_ORGANIZATION,
    CREATE_STUDY,
    DELETE_STUDY,
    DOWNLOAD_TIME_USE_DIARY_SUBMISSIONS,
    ENROLL_DEVICE,
    GET_ORGANIZATION,
    GET_STUDY,
    GET_TIME_USE_DIARY_SUBMISSION,
    REGISTER_CANDIDATE,
    STUDY_NOT_FOUND,
    SUBMIT_TIME_USE_DIARY,
    UPDATE_STUDY,
    UPDATE_STUDY_SETTINGS,
}
