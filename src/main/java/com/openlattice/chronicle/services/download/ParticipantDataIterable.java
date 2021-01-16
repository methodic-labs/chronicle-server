package com.openlattice.chronicle.services.download;

import com.google.common.collect.*;
import com.openlattice.chronicle.data.EntitySetIdGraph;
import com.openlattice.chronicle.services.edm.EdmCacheManager;
import com.openlattice.data.requests.NeighborEntityDetails;
import com.openlattice.edm.set.EntitySetPropertyMetadata;
import com.openlattice.edm.type.PropertyType;
import com.openlattice.graph.GraphApi;
import com.openlattice.graph.NeighborPage;
import com.openlattice.graph.PagedNeighborRequest;
import com.openlattice.search.requests.EntityNeighborsFilter;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import static com.openlattice.chronicle.constants.EdmConstants.TIMEZONE_FQN;
import static com.openlattice.chronicle.constants.OutputConstants.APP_PREFIX;
import static com.openlattice.chronicle.constants.OutputConstants.DEFAULT_TIMEZONE;
import static com.openlattice.chronicle.constants.OutputConstants.USER_PREFIX;

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
public class ParticipantDataIterable implements Iterable<Map<String, Set<Object>>> {

    private static final Logger logger = LoggerFactory.getLogger( ParticipantDataIterable.class );

    private final NeighborPageSupplier supplier;

    public ParticipantDataIterable( NeighborPageSupplier supplier ) {
        this.supplier = supplier;
    }

    @NotNull @Override
    public Iterator<Map<String, Set<Object>>> iterator() {
        return new ParticipantDataIterator( supplier );
    }

    public static class NeighborPageSupplier implements Supplier<NeighborPage> {
        private static final int MAX_PAGE_SIZE = 10000;

        private final EntitySetIdGraph                     entitySetIdGraph;
        private final Set<FullQualifiedName>               srcPropertiesToExclude;
        private final Set<FullQualifiedName>               edgePropertiesToExclude;
        private final Map<UUID, EntitySetPropertyMetadata> srcMetadata;
        private final Map<UUID, EntitySetPropertyMetadata> edgeMetadata;
        private final EdmCacheManager                      edmCacheManager;

        private final UUID     participantEKID;
        private final GraphApi graphApi;

        private NeighborPage page;

        public NeighborPageSupplier(
                EdmCacheManager edmCacheManager,
                GraphApi graphApi,
                EntitySetIdGraph entitySetIdGraph,
                Set<FullQualifiedName> srcPropertiesToExclude,
                Set<FullQualifiedName> edgePropertiesToExclude,
                Map<UUID, EntitySetPropertyMetadata> srcMetadata,
                Map<UUID, EntitySetPropertyMetadata> edgeMetadata,
                UUID participantEKID
        ) {
            this.graphApi = graphApi;
            this.entitySetIdGraph = entitySetIdGraph;
            this.srcPropertiesToExclude = srcPropertiesToExclude;
            this.edgePropertiesToExclude = edgePropertiesToExclude;
            this.srcMetadata = srcMetadata;
            this.edgeMetadata = edgeMetadata;
            this.participantEKID = participantEKID;
            this.edmCacheManager = edmCacheManager;

            this.page = new NeighborPage( ImmutableMap.of(), null );
        }

        @Override public NeighborPage get() {
            EntityNeighborsFilter searchFilter = new EntityNeighborsFilter(
                    ImmutableSet.of( participantEKID ),
                    Optional.of( ImmutableSet.of( entitySetIdGraph.getSrcEntitySetId() ) ),
                    Optional.of( ImmutableSet.of( entitySetIdGraph.getDstEntitySetId() ) ),
                    Optional.of( ImmutableSet.of( entitySetIdGraph.getEdgeEntitySetId() ) )
            );
            page = graphApi.getPageOfNeighbors( entitySetIdGraph.getDstEntitySetId(),
                    new PagedNeighborRequest( searchFilter, page.getBookmark(), MAX_PAGE_SIZE ) );

            return page;
        }

        public Set<FullQualifiedName> getSrcPropertiesToExclude() {
            return srcPropertiesToExclude;
        }

        public Set<FullQualifiedName> getEdgePropertiesToExclude() {
            return edgePropertiesToExclude;
        }

        public UUID getParticipantEKID() {
            return participantEKID;
        }

        public Map<UUID, EntitySetPropertyMetadata> getSrcMetadata() {
            return srcMetadata;
        }

        public Map<UUID, EntitySetPropertyMetadata> getEdgeMetadata() {
            return edgeMetadata;
        }

        public EdmCacheManager getEdmCacheManager() {
            return edmCacheManager;
        }
    }

    private static class ParticipantDataIterator implements Iterator<Map<String, Set<Object>>> {

        private final NeighborPageSupplier neighborPageSupplier;
        private final ReentrantLock        lock = new ReentrantLock();

        private boolean hasMorePages;
        private boolean finishedCurrentPage;
        private int     currentIndex;

        private NeighborPage currentPage;

        public ParticipantDataIterator( NeighborPageSupplier neighborPageSupplier ) {
            this.neighborPageSupplier = neighborPageSupplier;
            this.hasMorePages = true;
            this.finishedCurrentPage = false;
            this.currentPage = null;
            this.currentIndex = 0;
        }

        @Override
        public boolean hasNext() {
            return hasMorePages;
        }

        private Set<Object> parseDateTimeValues( Set<Object> values, ZoneId zoneId ) {
            Set<Object> result = Sets.newHashSet();
            values.forEach( val -> {
                try {
                    String parsed = OffsetDateTime.parse( val.toString() )
                            .toInstant()
                            .atZone( zoneId )
                            .toOffsetDateTime()
                            .toString();

                    result.add( parsed );
                } catch ( Exception e ) {
                    result.add( null );
                }
            } );

            return result;
        }

        @Override
        public Map<String, Set<Object>> next() {
            lock.lock();

            Map<String, Set<Object>> nextElement = Maps.newHashMap();
            try {
                // if we have finished processing all data in current page, retrieve next
                if ( currentPage == null || finishedCurrentPage ) {
                    currentPage = neighborPageSupplier.get();
                    currentIndex = 0; // reset index
                }

                Map<UUID, List<NeighborEntityDetails>> neighbors = currentPage.getNeighbors();

                if ( neighbors.isEmpty() ) {
                    hasMorePages = false;
                }

                List<NeighborEntityDetails> neighborEntityDetails = neighbors
                        .getOrDefault( neighborPageSupplier.getParticipantEKID(), ImmutableList.of() );
                // logger.info( "size of neighbors: {}", neighbors.size() );
                if ( !neighborEntityDetails.isEmpty() ) {

                    // 1: process entity data
                    Map<FullQualifiedName, Set<Object>> entities = neighborEntityDetails.get( currentIndex )
                            .getNeighborDetails().get();

                    ZoneId zoneId = ZoneId.of( entities
                            .getOrDefault( TIMEZONE_FQN, ImmutableSet.of( DEFAULT_TIMEZONE ) )
                            .iterator()
                            .next()
                            .toString()
                    );

                    entities.forEach( ( key, val ) -> {
                        if ( !neighborPageSupplier.getSrcPropertiesToExclude().contains( key ) ) {
                            PropertyType propertyType = neighborPageSupplier.getEdmCacheManager()
                                    .getPropertyType( key );
                            String title =
                                    APP_PREFIX + neighborPageSupplier.getSrcMetadata().get( propertyType.getId() )
                                            .getTitle();

                            if ( propertyType.getDatatype() == EdmPrimitiveTypeKind.DateTimeOffset ) {
                                nextElement.put( title, parseDateTimeValues( val, zoneId ) );
                            } else {
                                nextElement.put( title, val );
                            }
                        }
                    } );

                    // 2: process association data
                    Map<FullQualifiedName, Set<Object>> associations = neighborEntityDetails.get( currentIndex )
                            .getAssociationDetails();
                    associations.forEach( ( key, val ) -> {
                        if ( !neighborPageSupplier.getEdgePropertiesToExclude().contains( key ) ) {
                            UUID propertyTypeId = neighborPageSupplier.getEdmCacheManager()
                                    .getPropertyTypeId( key );
                            String title =
                                    USER_PREFIX + neighborPageSupplier.getEdgeMetadata().get( propertyTypeId )
                                            .getTitle();

                            nextElement.put( title, val );
                        }
                    } );

                    currentIndex++;

                    finishedCurrentPage = currentIndex == neighborEntityDetails.size();
                }

            } catch ( Exception e ) {
                logger.error( "unable to retrieve next element", e );
            } finally {
                lock.unlock();
            }

            return nextElement;
        }
    }
}
