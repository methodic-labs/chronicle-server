package com.openlattice.chronicle.services.download

import com.dataloom.streams.StreamUtil
import com.google.common.collect.*
import com.openlattice.chronicle.constants.EdmConstants
import com.openlattice.chronicle.constants.OutputConstants
import com.openlattice.chronicle.data.EntitySetIdGraph
import com.openlattice.chronicle.services.edm.EdmCacheManager
import com.openlattice.data.requests.NeighborEntityDetails
import com.openlattice.edm.set.EntitySetPropertyMetadata
import com.openlattice.graph.GraphApi
import com.openlattice.graph.NeighborPage
import com.openlattice.graph.PagedNeighborRequest
import com.openlattice.search.requests.EntityNeighborsFilter
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.LoggerFactory
import java.lang.Exception
import java.time.OffsetDateTime
import java.time.ZoneId
import java.util.*
import java.util.concurrent.locks.ReentrantLock
import java.util.function.Consumer
import java.util.function.Supplier
import java.util.stream.Stream

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
class ParticipantDataIterable(private val supplier: NeighborPageSupplier) : Iterable<Map<String, Set<Any>>> {
    override fun iterator(): Iterator<Map<String, Set<Any>>> {
        return ParticipantDataIterator(supplier)
    }

    fun stream(): Stream<Map<String, Set<Any>>>? {
        return StreamUtil.stream(this)
    }

    class NeighborPageSupplier(
            val edmCacheManager: EdmCacheManager,
            private val graphApi: GraphApi,
            private val entitySetIdGraph: EntitySetIdGraph,
            val srcPropertiesToExclude: Set<FullQualifiedName>,
            val edgePropertiesToExclude: Set<FullQualifiedName>,
            val srcMetadata: Map<UUID, EntitySetPropertyMetadata>,
            val edgeMetadata: Map<UUID, EntitySetPropertyMetadata>,
            val participantEKID: UUID
    ) : Supplier<NeighborPage?> {
        private var page: NeighborPage?

        companion object {
            private val logger = LoggerFactory.getLogger(NeighborPageSupplier::class.java)
            private const val MAX_PAGE_SIZE = 10000
        }

        init {
            page = NeighborPage(ImmutableMap.of(), null)
        }

        override fun get(): NeighborPage? {
            page = try {
                val searchFilter = EntityNeighborsFilter(
                        ImmutableSet.of(participantEKID),
                        Optional.of(ImmutableSet.of(entitySetIdGraph.srcEntitySetId)),
                        Optional.of(ImmutableSet.of(entitySetIdGraph.dstEntitySetId)),
                        Optional.of(ImmutableSet.of(entitySetIdGraph.edgeEntitySetId))
                )
                graphApi.getPageOfNeighbors(entitySetIdGraph.dstEntitySetId,
                        PagedNeighborRequest(searchFilter, page!!.bookmark, MAX_PAGE_SIZE))
            } catch (e: Exception) {
                logger.error("error retrieving neighbor page", e)
                null
            }
            return page
        }
    }

    private class ParticipantDataIterator(private val neighborPageSupplier: NeighborPageSupplier) : Iterator<Map<String, Set<Any>>> {
        private val lock = ReentrantLock()
        private var hasMorePages = true
        private var finishedCurrentPage = false
        private var currentIndex = 0
        private var currentPage: NeighborPage? = null

        companion object {
            private val logger = LoggerFactory.getLogger(ParticipantDataIterable::class.java)
        }

        override fun hasNext(): Boolean {
            return hasMorePages
        }

        private fun parseDateTimeValues(values: Set<Any?>, zoneId: ZoneId): Set<Any> {
            val result: MutableSet<Any> = Sets.newHashSet()
            values.forEach(Consumer { `val`: Any? ->
                try {
                    val parsed = OffsetDateTime.parse(`val`.toString())
                            .toInstant()
                            .atZone(zoneId)
                            .toOffsetDateTime()
                            .toString()
                    result.add(parsed)
                } catch (e: Exception) {
                    result.add("")
                }
            })
            return result
        }

        override fun next(): Map<String, Set<Any>> {
            lock.lock()

            val nextElement: MutableMap<String, Set<Any>> = Maps.newHashMap()
            try {
                // if we have finished processing all data in current page, retrieve next
                if (currentPage == null || finishedCurrentPage) {
                    currentPage = neighborPageSupplier.get()

                    // if an exception occurs while getting page, stop iterating
                    if (currentPage == null) {
                        hasMorePages = false
                        throw RuntimeException("download failed")
                    }
                    currentIndex = 0 // reset index
                }
                val neighbors: Map<UUID, List<NeighborEntityDetails>> = currentPage!!.neighbors
                if (neighbors.isEmpty()) {
                    hasMorePages = false
                }
                val neighborEntityDetails = neighbors[neighborPageSupplier.participantEKID] ?: ImmutableList.of()
                if (neighborEntityDetails.isNotEmpty()) {

                    // 1: process entity data
                    val entities = neighborEntityDetails[currentIndex]
                            .neighborDetails.get()
                    val zoneId = ZoneId.of(entities
                            .getOrDefault(EdmConstants.TIMEZONE_FQN, ImmutableSet.of<Any>(OutputConstants.DEFAULT_TIMEZONE))
                            .iterator()
                            .next()
                            .toString()
                    )
                    entities.forEach { (key: FullQualifiedName?, `val`: Set<Any?>) ->
                        if (!neighborPageSupplier.srcPropertiesToExclude.contains(key)) {
                            val propertyType = neighborPageSupplier.edmCacheManager
                                    .getPropertyType(key)
                            val title = OutputConstants.APP_PREFIX + neighborPageSupplier.srcMetadata[propertyType.id]
                                    ?.title
                            if (propertyType.datatype == EdmPrimitiveTypeKind.DateTimeOffset) {
                                nextElement[title] = parseDateTimeValues(`val`, zoneId)
                            } else {
                                nextElement[title] = `val`
                            }
                        }
                    }

                    // 2: process association data
                    val associations = neighborEntityDetails[currentIndex]
                            .associationDetails
                    associations.forEach { (key: FullQualifiedName?, `val`: Set<Any?>) ->
                        if (!neighborPageSupplier.edgePropertiesToExclude.contains(key)) {
                            val propertyTypeId = neighborPageSupplier.edmCacheManager
                                    .getPropertyTypeId(key)
                            val title = OutputConstants.USER_PREFIX + neighborPageSupplier.edgeMetadata[propertyTypeId]
                                    ?.title
                            nextElement[title] = `val`
                        }
                    }
                    currentIndex++
                    finishedCurrentPage = currentIndex == neighborEntityDetails.size
                }
            } catch (e: Exception) {
                logger.error("unable to retrieve next element", e)
            } finally {
                lock.unlock()
            }
            return nextElement.toMap()
        }
    }
}