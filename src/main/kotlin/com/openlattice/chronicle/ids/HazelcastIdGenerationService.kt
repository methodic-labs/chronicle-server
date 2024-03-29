package com.openlattice.chronicle.ids

import com.geekbeast.hazelcast.IHazelcastClientProvider
import com.google.common.collect.Queues
import com.openlattice.chronicle.hazelcast.HazelcastClient
import com.openlattice.chronicle.hazelcast.HazelcastMap
import com.openlattice.chronicle.hazelcast.HazelcastQueue
import com.openlattice.chronicle.mapstores.ids.IdsGeneratingEntryProcessor
import com.openlattice.chronicle.mapstores.ids.Range
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.BlockingQueue
import java.util.concurrent.Executors

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class HazelcastIdGenerationService(clients: IHazelcastClientProvider) {
    var random = false

    /**
     * For testing only
     */
    internal constructor(clients: IHazelcastClientProvider, random: Boolean) : this(clients) {
        this.random = random
    }

    /*
     * This should be good enough until we scale past 65536 Hazelcast nodes.
     */
    companion object {
        private const val PARTITION_SCROLL_SIZE = 5
        private const val MASK_LENGTH = 16
        const val NUM_PARTITIONS = 1 shl MASK_LENGTH //65536
        private val logger = LoggerFactory.getLogger(HazelcastIdGenerationService::class.java)
        private val executor = Executors.newSingleThreadExecutor()
    }

    /*
     * Each range owns a portion of the keyspace.
     */
    private val hazelcastInstance = clients.getClient(HazelcastClient.IDS.name)
    private val scrolls = HazelcastMap.ID_GENERATION.getMap(hazelcastInstance)
    private val idsQueue = HazelcastQueue.ID_GENERATION.getQueue(hazelcastInstance)
    private val localQueue = Queues.newArrayBlockingQueue<UUID>(NUM_PARTITIONS) as BlockingQueue<UUID>

    init {
        if (scrolls.isEmpty) {
            //Initialize the ranges
            scrolls.putAll((0L until NUM_PARTITIONS).associateWith { Range(it shl 48) })
        }
    }

    private val enqueueJob = executor.execute {
        if (random) {
            generateSequence { UUID.randomUUID() }.forEach { idsQueue.put(it) }
        }
        while (true) {
            val ids = try {
                //Use the 0 key a fence around the entire map.
                scrolls.lock(0L)
                //TODO: Handle exhaustion of partition.
                scrolls.executeOnEntries(IdsGeneratingEntryProcessor(PARTITION_SCROLL_SIZE)) as Map<Long, List<UUID>>
            } finally {
                scrolls.unlock(0L)
            }

            ids.values.asSequence().flatten().forEach { idsQueue.put(it) }

            logger.debug("Added $NUM_PARTITIONS ids to queue")
        }
    }

    /**
     * Returns an id to the local id for later use.
     * @param id to return to the pool
     */
    fun returnId(id: UUID) {
        localQueue.offer(id)
    }

    fun returnIds(ids: Collection<UUID>) {
        ids.forEach(::returnId)
    }

    fun getNextIds(count: Int): Set<UUID> {
        return generateSequence { getNextId() }.take(count).toSet()
    }

    fun getNextId(): UUID {
        var id = localQueue.poll() ?: idsQueue.take()
        //This will make sure that reserved ids are skipped
        while ((id.mostSignificantBits == 0L) && (id.leastSignificantBits > 0L) && (id.leastSignificantBits < IdConstants.RESERVED_IDS_BASE)) {
            id = localQueue.poll() ?: idsQueue.take()
        }
        return id
    }
}