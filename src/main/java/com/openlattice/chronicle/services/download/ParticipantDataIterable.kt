package com.openlattice.chronicle.services.download

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
import java.time.OffsetDateTime
import java.time.ZoneId
import java.util.*
import java.util.function.Consumer
import java.util.function.Supplier

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
class ParticipantDataIterable(private val columnTitles :List<String>, private val supplier: NeighborPageSupplier) : Iterable<Map<String, Set<Any>>> {
    override fun iterator(): Iterator<Map<String, Set<Any>>> {
        return ParticipantDataIterator(supplier)
    }

    fun getColumnTitles() :List<String> {
        return columnTitles
    }

    class NeighborPageSupplier(
            val edmCacheManager: EdmCacheManager,
            private val graphApi: GraphApi,
            private val entitySetIdGraph: EntitySetIdGraph,
            val srcPropertiesToInclude: Set<FullQualifiedName>,
            val edgePropertiesToInclude: Set<FullQualifiedName>,
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
            page = NeighborPage(mapOf(), null)
        }

        override fun get(): NeighborPage? {
            page = try {
                val searchFilter = EntityNeighborsFilter(
                        setOf(participantEKID),
                        Optional.of(setOf(entitySetIdGraph.srcEntitySetId)),
                        Optional.of(setOf(entitySetIdGraph.dstEntitySetId)),
                        Optional.of(setOf(entitySetIdGraph.edgeEntitySetId))
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
        private var hasMorePages = true
        private var finishedCurrentPage = false
        private var currentIndex = 0
        private var currentPage: NeighborPage? = null
        private var isFirstPage = true

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

        private fun getZoneIdFromEntity(entity: Map<FullQualifiedName, Set<Any>>): ZoneId {
            return ZoneId.of(entity
                    .getOrDefault(EdmConstants.TIMEZONE_FQN, ImmutableSet.of<Any>(OutputConstants.DEFAULT_TIMEZONE))
                    .iterator()
                    .next()
                    .toString())

        }

        private fun getDownloadValuesForEntity(
                entity: Map<FullQualifiedName, Set<Any>>,
                metadata: Map<UUID, EntitySetPropertyMetadata>,
                propertiesToInclude: Set<FullQualifiedName>,
                prefix: String
        ): Map<String, Set<Any>> {
            val zoneId = getZoneIdFromEntity(entity)

            return entity.entries.filter { (fqn, _) ->
                propertiesToInclude.contains(fqn)
            }.associate { (fqn, values) ->
                val propertyType = neighborPageSupplier.edmCacheManager.getPropertyType(fqn)
                val title = prefix + metadata.getValue(propertyType.id).title

                if (propertyType.datatype == EdmPrimitiveTypeKind.DateTimeOffset) {
                    title to parseDateTimeValues(values, zoneId)
                } else {
                    title to values
                }
            }
        }

        @Synchronized
        override fun next(): Map<String, Set<Any>> {

            try {
                // if we haven't retrieved any page yet, send an empty map to the output writer
                if (isFirstPage) {
                    isFirstPage = false
                    return mapOf()
                }

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
                val neighborEntityDetails = neighbors[neighborPageSupplier.participantEKID] ?: listOf()
                if (neighborEntityDetails.isNotEmpty()) {

                    val neighborValues = getDownloadValuesForEntity(
                            neighborEntityDetails[currentIndex].neighborDetails.get(),
                            neighborPageSupplier.srcMetadata,
                            neighborPageSupplier.srcPropertiesToInclude,
                            OutputConstants.APP_PREFIX
                    )

                    val associationValues = getDownloadValuesForEntity(
                            neighborEntityDetails[currentIndex].associationDetails,
                            neighborPageSupplier.edgeMetadata,
                            neighborPageSupplier.edgePropertiesToInclude,
                            OutputConstants.USER_PREFIX
                    )

                    currentIndex++
                    finishedCurrentPage = currentIndex == neighborEntityDetails.size

                    return neighborValues + associationValues
                }
            } catch (e: Exception) {
                logger.error("unable to retrieve next element", e)
                return mapOf()
            }
            return mapOf()
        }
    }
}