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
    ALLOW_APPS,
    ADD_PERMISSION,
    ASSOCIATE_STUDY,
    BACKGROUND_USAGE_DATA_DELETION,
    BACKGROUND_TUD_DATA_DELETION,
    BACKGROUND_APP_USAGE_SURVEY_DATA_DELETION,
    CREATE_JOB,
    CREATE_ORGANIZATION,
    CREATE_QUESTIONNAIRE,
    CREATE_STUDY,
    DELETE_QUESTIONNAIRE,
    DELETE_STUDY,
    DELETE_PARTICIPANTS,
    DOWNLOAD_TIME_USE_DIARY_DATA,
    DOWNLOAD_PARTICIPANTS_TIME_USE_DIARY_DATA,
    DOWNLOAD_PARTICIPANTS_DATA,
    ENROLL_DEVICE,
    FILTER_APPS,
    GET_FILTERED_APPS,
    GET_ORGANIZATION,
    GET_STUDY,
    GET_TIME_USE_DIARY_SUBMISSION,
    REGISTER_CANDIDATE,
    REMOVE_PERMISSION,
    SET_PERMISSION,
    STUDY_NOT_FOUND,
    SUBMIT_TIME_USE_DIARY,
    UPDATE_PARTICIPATION_STATUS,
    UPDATE_QUESTIONNAIRE,
    UPDATE_STUDY,
    UPDATE_STUDY_SETTINGS,
    QUEUE_NOTIFICATIONS,
    GET_ALL_STUDIES,
    NOTIFICATION_SENT,
    SET_STUDY_LIMITS,
    TRIGGER_STUDY_COMPLIANCE_NOTIFICATIONS,
}
