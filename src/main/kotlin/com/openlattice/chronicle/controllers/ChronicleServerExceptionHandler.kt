/*
 * Copyright (C) 2018. OpenLattice, Inc.
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
 */
package com.openlattice.chronicle.controllers

import com.fasterxml.jackson.databind.JsonMappingException
import com.geekbeast.controllers.exceptions.wrappers.ErrorsDTO
import com.geekbeast.controllers.util.ApiExceptions
import com.openlattice.chronicle.auditing.AuditEventType
import com.openlattice.chronicle.auditing.AuditableEvent
import com.openlattice.chronicle.auditing.AuditingComponent
import com.openlattice.chronicle.auditing.AuditingManager
import com.openlattice.chronicle.authorization.AclKey
import com.openlattice.chronicle.authorization.principals.Principals
import com.openlattice.chronicle.ids.IdConstants
import org.apache.commons.io.IOUtils
import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.http.converter.HttpMessageNotReadableException
import org.springframework.security.access.AccessDeniedException
import org.springframework.web.bind.annotation.ExceptionHandler
import org.springframework.web.bind.annotation.RestControllerAdvice
import java.util.*
import javax.inject.Inject
import javax.servlet.http.HttpServletRequest

@RestControllerAdvice
class ChronicleServerExceptionHandler @Inject constructor(override val auditingManager: AuditingManager) :
    AuditingComponent {
    @ExceptionHandler(
        NullPointerException::class,
        StudyRegistrationNotFoundException::class,
        StudyNotFoundException::class
    )
    fun handleNotFoundException(req: HttpServletRequest, e: Exception): ResponseEntity<ErrorsDTO> {
        logException(req, e)
        val principal = Principals.getCurrentSecurablePrincipal()
        val principals = Principals.getCurrentPrincipals()
        val event = when (e) {
            is StudyNotFoundException -> {
                AuditableEvent(
                    AclKey(e.studyId),
                    principal.id,
                    principal.principal,
                    AuditEventType.STUDY_NOT_FOUND,
                    "Unable to find study ${e.studyId}",
                    e.studyId,
                    data = mapOf("principals" to principals)
                )
            }
            else -> {
                AuditableEvent(
                    AclKey(IdConstants.METHODIC.id),
                    principal.id,
                    principal.principal,
                    AuditEventType.STUDY_NOT_FOUND,
                    e.message ?: "Exception did not include message",
                    IdConstants.UNINITIALIZED.id,
                    data = mapOf("principals" to principals)
                )
            }
        }
        recordEvent(event)

        return if (e.message != null) {
            ResponseEntity(
                ErrorsDTO(ApiExceptions.RESOURCE_NOT_FOUND_EXCEPTION, e.message!!),
                HttpStatus.NOT_FOUND
            )
        } else ResponseEntity(HttpStatus.NOT_FOUND)
    }

    @ExceptionHandler(IllegalArgumentException::class, HttpMessageNotReadableException::class)
    fun handleIllegalArgumentException(req: HttpServletRequest, e: Exception): ResponseEntity<ErrorsDTO> {
        when (e) {
            is HttpMessageNotReadableException -> logger.error(
                "Body that caused error if available: " + IOUtils.toString(
                    e.httpInputMessage.body
                )
            )
            else -> logger.error("Body is not available.")
        }
        logException(req, e)
        return ResponseEntity(
            ErrorsDTO(ApiExceptions.ILLEGAL_ARGUMENT_EXCEPTION, e.message!!),
            HttpStatus.BAD_REQUEST
        )
    }

    @ExceptionHandler(IllegalStateException::class)
    fun handleIllegalStateException(req: HttpServletRequest, e: Exception): ResponseEntity<ErrorsDTO> {
        logException(req, e)
        return ResponseEntity(
            ErrorsDTO(ApiExceptions.ILLEGAL_STATE_EXCEPTION, e.message!!),
            HttpStatus.INTERNAL_SERVER_ERROR
        )
    }

    @ExceptionHandler(AccessDeniedException::class)
    fun handleUnauthorizedExceptions(req: HttpServletRequest, e: AccessDeniedException): ResponseEntity<ErrorsDTO> {
        logException(req, e)
        return ResponseEntity(
            ErrorsDTO(ApiExceptions.FORBIDDEN_EXCEPTION, e.message!!),
            HttpStatus.UNAUTHORIZED
        )
    }
    
    @ExceptionHandler(JsonMappingException::class)
    fun handleJsonExceptions(req: HttpServletRequest, e: JsonMappingException) {
        logger.error("Body that caused error if available: " + e.originalMessage)
        logException(req, e)
    }

    @ExceptionHandler(Exception::class)
    fun handleOtherExceptions(req: HttpServletRequest, e: Exception): ResponseEntity<ErrorsDTO> {
        logException(req, e)
        return ResponseEntity(
            ErrorsDTO(ApiExceptions.OTHER_EXCEPTION, e.javaClass.simpleName + ": " + e.message),
            HttpStatus.INTERNAL_SERVER_ERROR
        )
    }

    private fun logException(req: HttpServletRequest, e: Exception) {
        logger.error("Encountered exception handling request of type ${req.method} to URL ${req.requestURL}", e)
    }

    companion object {
        private val logger = LoggerFactory.getLogger(ChronicleServerExceptionHandler::class.java)
    }
}

class StudyRegistrationNotFoundException : RuntimeException {
    constructor(message: String) : super(message)
    constructor(message: String, cause: Throwable) : super(message, cause)
}

class CandidateNotFoundException(candidateId: UUID, message: String? = "$candidateId") : RuntimeException(message)
class StudyNotFoundException(val studyId: UUID, message: String) : RuntimeException(message)
class OrganizationNotFoundException(val organization: UUID, message: String) : RuntimeException(message)
class TimeUseDiaryDownloadExcpetion(val studyId: UUID, message: String) : RuntimeException(message)