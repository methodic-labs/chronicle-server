package com.openlattice.chronicle.services.studies.tasks

import com.geekbeast.tasks.HazelcastFixedRateTask
import com.geekbeast.tasks.HazelcastTaskDependencies
import com.openlattice.chronicle.services.studies.StudyComplianceManager
import java.util.concurrent.TimeUnit

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@getmethodic.com&gt;
 */
class StudyComplianceHazelcastTask : HazelcastFixedRateTask<StudyComplianceHazelcastTaskDependencies> {
    override fun getInitialDelay(): Long = 0

    override fun getPeriod(): Long = 15

    override fun getTimeUnit(): TimeUnit = TimeUnit.MINUTES

    override fun runTask() {
        getDependency().studyComplianceManager
    }

    override fun getName(): String = "study_compliance_task"

    override fun getDependenciesClass(): Class<out StudyComplianceHazelcastTaskDependencies> =
        StudyComplianceHazelcastTaskDependencies::class.java
}

data class StudyComplianceHazelcastTaskDependencies(
    val studyComplianceManager: StudyComplianceManager,
) : HazelcastTaskDependencies
