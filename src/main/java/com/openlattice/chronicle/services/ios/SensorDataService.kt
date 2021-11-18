package com.openlattice.chronicle.services.ios

import com.openlattice.chronicle.services.ApiCacheManager
import com.openlattice.chronicle.services.ScheduledTasksManager
import com.openlattice.chronicle.services.edm.EdmCacheManager
import com.openlattice.chronicle.services.enrollment.EnrollmentManager
import com.openlattice.chronicle.services.entitysets.EntitySetIdsManager
import org.slf4j.LoggerFactory
import java.util.*

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
class SensorDataService(
        val apiCacheManager: ApiCacheManager,
        val edmCacheManager: EdmCacheManager,
        val entitySetIdsManager: EntitySetIdsManager,
        val scheduledTasksManager: ScheduledTasksManager,
        val enrollmentManager: EnrollmentManager) : SensorDataManager {

    private val logger = LoggerFactory.getLogger(SensorDataService::class.java)

    override fun uploadData(organizationId: UUID, studyId: UUID, participantId: String, deviceId: String, data: List<Map<UUID, Any>>) {

        logger.info("server called with data size ${data.size}")
    }
}