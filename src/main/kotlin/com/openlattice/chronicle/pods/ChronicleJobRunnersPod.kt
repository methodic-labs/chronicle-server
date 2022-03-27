package com.openlattice.chronicle.pods

import com.geekbeast.auth0.Auth0Pod
import com.geekbeast.authentication.Auth0Configuration
import com.geekbeast.hazelcast.HazelcastClientProvider
import com.geekbeast.jdbc.DataSourceManager
import com.geekbeast.mail.MailService
import com.google.common.eventbus.EventBus
import com.google.common.util.concurrent.ListeningExecutorService
import com.hazelcast.core.HazelcastInstance
import com.openlattice.chronicle.configuration.ChronicleConfiguration
import com.openlattice.chronicle.deletion.*
import com.openlattice.chronicle.services.notifications.NotificationJobRunner
import com.openlattice.chronicle.services.twilio.TwilioService
import com.openlattice.chronicle.storage.StorageResolver
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Import
import javax.inject.Inject

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Configuration
@Import(Auth0Pod::class)
class ChronicleJobRunnersPod {
        @Inject
        private lateinit var auth0Configuration: Auth0Configuration

        @Inject
        private lateinit var dataSourceManager: DataSourceManager

        @Inject
        private lateinit var hazelcastClientProvider: HazelcastClientProvider

        @Inject
        private lateinit var executor: ListeningExecutorService

        @Inject
        private lateinit var hazelcast: HazelcastInstance

        @Inject
        private lateinit var eventBus: EventBus

        @Inject
        private lateinit var chronicleConfiguration: ChronicleConfiguration

        @Inject
        private lateinit var storageResolver: StorageResolver

        @Inject
        private lateinit var twilioService: TwilioService

        @Inject
        private lateinit var mailService: MailService

        @Bean
        fun deleteStudyUsageDataRunner() : DeleteStudyUsageDataRunner {
            return DeleteStudyUsageDataRunner(storageResolver)
        }

        @Bean
        fun deleteStudyTUDSubmissionDataRunner() : DeleteStudyTUDSubmissionDataRunner {
                return DeleteStudyTUDSubmissionDataRunner()
        }

        @Bean
        fun deleteAppUsageSurveyDataRunner() : DeleteStudyAppUsageSurveyDataRunner {
                return DeleteStudyAppUsageSurveyDataRunner()
        }

        @Bean
        fun deleteParticipantUsageDataRunner() : DeleteParticipantUsageDataRunner {
                return DeleteParticipantUsageDataRunner(storageResolver)
        }

        @Bean
        fun deleteParticipantTUDSubmissionDataRunner() : DeleteParticipantTUDSubmissionDataRunner {
                return DeleteParticipantTUDSubmissionDataRunner()
        }

        @Bean
        fun deleteParticipantAppUsageSurveyDataRunner() : DeleteParticipantAppUsageSurveyDataRunner {
                return DeleteParticipantAppUsageSurveyDataRunner()
        }

        @Bean
        fun notificationJobRunner() : NotificationJobRunner {
                return NotificationJobRunner(twilioService, mailService)
        }

}
