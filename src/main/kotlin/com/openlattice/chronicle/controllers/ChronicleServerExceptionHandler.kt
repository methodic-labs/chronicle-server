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

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.JsonMappingException
import com.fasterxml.jackson.databind.ObjectMapper
import com.geekbeast.controllers.exceptions.wrappers.ErrorsDTO
import com.geekbeast.controllers.util.ApiExceptions
import com.geekbeast.mappers.mappers.ObjectMappers
import com.openlattice.chronicle.auditing.AuditEventType
import com.openlattice.chronicle.auditing.AuditableEvent
import com.openlattice.chronicle.auditing.AuditingComponent
import com.openlattice.chronicle.auditing.AuditingManager
import com.openlattice.chronicle.authorization.AclKey
import com.openlattice.chronicle.authorization.principals.Principals
import com.openlattice.chronicle.ids.IdConstants
import org.eclipse.jetty.io.EofException
import org.slf4j.LoggerFactory
import org.springframework.core.Ordered
import org.springframework.core.annotation.Order
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.http.converter.HttpMessageNotReadableException
import org.springframework.security.access.AccessDeniedException
import org.springframework.web.bind.annotation.ExceptionHandler
import org.springframework.web.bind.annotation.RestControllerAdvice
import org.springframework.web.util.ContentCachingRequestWrapper
import org.springframework.web.util.WebUtils
import java.nio.charset.StandardCharsets
import java.util.*
import javax.inject.Inject
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse

@RestControllerAdvice
@Order(Ordered.HIGHEST_PRECEDENCE)
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

    @ExceptionHandler(IllegalArgumentException::class)
    fun handleIllegalArgumentException(req: HttpServletRequest, e: Exception): ResponseEntity<ErrorsDTO> {
        logException(req, e)
        return ResponseEntity(
            ErrorsDTO(ApiExceptions.ILLEGAL_ARGUMENT_EXCEPTION, e.message ?: e.javaClass.simpleName),
            HttpStatus.BAD_REQUEST
        )
    }

    /**
     * Handles failures to read/parse the request body. This is intentionally void-returning so that we own the
     * response entirely: when the client has disconnected (EofException) or the response is already committed,
     * attempting to serialize an error body throws while writing, which cascades into Spring's
     * DefaultHandlerExceptionResolver calling sendError() on a committed response — the noisy
     * "IllegalStateException: COMMITTED" we used to see. By writing nothing in that case we end the request cleanly.
     */
    @ExceptionHandler(HttpMessageNotReadableException::class)
    fun handleHttpMessageNotReadable(
        req: HttpServletRequest,
        res: HttpServletResponse,
        e: HttpMessageNotReadableException,
    ) {
        val clientDisconnected = findCause(e, EofException::class.java) != null
        logHttpMessageNotReadable(req, e, clientDisconnected)

        if (clientDisconnected) {
            // The socket is gone; there is no one to send a response to. Anything we write here would fail and
            // trigger a secondary "COMMITTED" failure, so we stop quietly.
            return
        }

        if (res.isCommitted) {
            logger.warn("Response already committed while handling unreadable request body; not writing an error body.")
            return
        }

        writeErrorResponse(res, e.message ?: e.javaClass.simpleName)
    }

    private fun logHttpMessageNotReadable(
        req: HttpServletRequest,
        e: HttpMessageNotReadableException,
        clientDisconnected: Boolean,
    ) {
        val jsonMappingException = findCause(e, JsonMappingException::class.java)
        val jsonProcessingException = findCause(e, JsonProcessingException::class.java)

        val cachedBody = cachedRequestBody(req)

        // Client disconnects mid-upload are an expected condition on flaky mobile networks rather than a server
        // bug, so log them at WARN to keep the error stream actionable; genuine malformed payloads stay at ERROR.
        val message =
            "HttpMessageNotReadable: method={} url={} contentLength={} contentType={} remoteAddr={} jacksonPath={} location={} clientDisconnected={} capturedBody={}"
        val args = arrayOf<Any?>(
            req.method,
            req.requestURL,
            req.contentLengthLong,
            req.contentType,
            req.remoteAddr,
            jsonMappingException?.pathReference,
            jsonProcessingException?.location,
            clientDisconnected,
            cachedBody ?: "<unavailable>"
        )
        if (clientDisconnected) {
            logger.warn(message, *args)
        } else {
            logger.error(message, *args)
        }

        if (cachedBody == null) {
            logger.warn(
                "No cached request body available for ${req.method} ${req.requestURL}; the body may have exceeded the cache limit, been empty, or the ContentCachingRequestFilter did not wrap this request."
            )
        }
    }

    /**
     * Recovers the bytes that were actually received for this request from the [ContentCachingRequestWrapper]
     * installed by ContentCachingRequestFilter. For a truncated upload this is the partial body up to the point the
     * stream died. Returns null when no cached content is available.
     */
    private fun cachedRequestBody(req: HttpServletRequest): String? {
        val wrapper = WebUtils.getNativeRequest(req, ContentCachingRequestWrapper::class.java) ?: return null
        val bytes = wrapper.contentAsByteArray
        if (bytes.isEmpty()) {
            return null
        }
        val capped = if (bytes.size > MAX_LOGGED_BODY_BYTES) bytes.copyOf(MAX_LOGGED_BODY_BYTES) else bytes
        val body = String(capped, StandardCharsets.UTF_8)
        return if (bytes.size > MAX_LOGGED_BODY_BYTES || bytes.size.toLong() < req.contentLengthLong) {
            "$body …[captured ${bytes.size} bytes of contentLength=${req.contentLengthLong}; truncated]"
        } else {
            body
        }
    }

    private fun writeErrorResponse(res: HttpServletResponse, message: String) {
        try {
            res.status = HttpStatus.BAD_REQUEST.value()
            res.contentType = MediaType.APPLICATION_JSON_VALUE
            res.characterEncoding = StandardCharsets.UTF_8.name()
            mapper.writeValue(res.outputStream, ErrorsDTO(ApiExceptions.ILLEGAL_ARGUMENT_EXCEPTION, message))
        } catch (ex: Exception) {
            // Writing failed (e.g. the connection dropped between our committed check and the write). Nothing more
            // we can do for the client; log and move on rather than letting this escalate.
            logger.warn("Failed to write error response for unreadable request body: {}", ex.toString())
        }
    }

    private fun <T : Throwable> findCause(e: Throwable, clazz: Class<T>): T? {
        var cur: Throwable? = e
        while (cur != null) {
            if (clazz.isInstance(cur)) return clazz.cast(cur)
            if (cur.cause === cur) return null
            cur = cur.cause
        }
        return null
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
        logger.error(
            "JsonMapping error: path={} originalMessage={} capturedBody={}",
            e.pathReference,
            e.originalMessage,
            cachedRequestBody(req) ?: "<unavailable>"
        )
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
        private val mapper: ObjectMapper = ObjectMappers.newJsonMapper()

        // Cap how much of a captured body we emit to the logs to keep individual log lines bounded even though the
        // request cache itself is already limited by ContentCachingRequestFilter.
        private const val MAX_LOGGED_BODY_BYTES = 16 * 1024
    }
}

class StudyRegistrationNotFoundException : RuntimeException {
    constructor(message: String) : super(message)
    constructor(message: String, cause: Throwable) : super(message, cause)
}

class CandidateNotFoundException(candidateId: UUID, message: String? = "$candidateId") : RuntimeException(message)
class StudyNotFoundException(val studyId: UUID, message: String) : RuntimeException(message)
class OrganizationNotFoundException(val organization: UUID, message: String) : RuntimeException(message)
class TimeUseDiaryDownloadException(val studyId: UUID, message: String) : RuntimeException(message)