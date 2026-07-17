package com.openlattice.chronicle.filters

import org.springframework.web.filter.OncePerRequestFilter
import org.springframework.web.util.ContentCachingRequestWrapper
import javax.servlet.FilterChain
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse

/**
 * Wraps requests that carry a body in a [ContentCachingRequestWrapper] so that, if downstream parsing fails (e.g. a
 * malformed payload or a client that disconnects mid-upload), the exception handler can recover the bytes that were
 * actually received and log them for investigation.
 *
 * The wrapper only retains bytes that are actually consumed by the downstream message converter, capped at
 * [cacheLimit] to bound both heap usage and the size of anything we later write to the logs. For a truncated upload
 * this yields the partial body up to the point the stream died, which is exactly what we want for debugging.
 */
class ContentCachingRequestFilter(private val cacheLimit: Int = DEFAULT_CACHE_LIMIT) : OncePerRequestFilter() {

    override fun doFilterInternal(
        request: HttpServletRequest,
        response: HttpServletResponse,
        filterChain: FilterChain,
    ) {
        // Only requests with a body can produce a parse failure worth capturing, and don't double-wrap.
        if (hasBody(request) && request !is ContentCachingRequestWrapper) {
            filterChain.doFilter(ContentCachingRequestWrapper(request, cacheLimit), response)
        } else {
            filterChain.doFilter(request, response)
        }
    }

    private fun hasBody(request: HttpServletRequest): Boolean {
        return when (request.method?.uppercase()) {
            "POST", "PUT", "PATCH" -> true
            else -> false
        }
    }

    companion object {
        // 256 KiB is enough to capture the leading portion of a sensor upload (hundreds of samples) while bounding
        // heap per in-flight request and keeping log lines manageable.
        const val DEFAULT_CACHE_LIMIT = 256 * 1024
    }
}
