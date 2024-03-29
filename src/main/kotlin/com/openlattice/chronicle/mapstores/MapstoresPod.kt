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
package com.openlattice.chronicle.mapstores

import com.auth0.json.mgmt.users.User
import com.geekbeast.postgres.PostgresPod
import com.geekbeast.auth0.Auth0Pod
import com.geekbeast.postgres.PostgresTableManager
import com.geekbeast.authentication.Auth0Configuration
import org.jdbi.v3.core.Jdbi
import com.geekbeast.rhizome.mapstores.SelfRegisteringMapStore
import java.util.UUID
import com.geekbeast.rhizome.jobs.DistributableJob
import com.geekbeast.rhizome.jobs.PostgresJobsMapStore
import com.google.common.eventbus.EventBus
import com.geekbeast.auth0.Auth0TokenProvider
import com.geekbeast.auth0.RefreshingAuth0TokenProvider
import com.geekbeast.rhizome.KotlinDelegatedStringSet
import com.openlattice.chronicle.authorization.*
import com.openlattice.chronicle.authorization.mapstores.SecurableObjectTypeMapstore
import com.openlattice.chronicle.authorization.mapstores.UserMapstore
import com.openlattice.chronicle.authorization.principals.PrincipalMapstore
import com.openlattice.chronicle.ids.mapstores.IdGenerationMapstore
import com.openlattice.chronicle.ids.mapstores.LongIdsMapstore
import com.openlattice.chronicle.mapstores.apps.FilteredAppsMapstore
import com.openlattice.chronicle.mapstores.authorization.PermissionMapstore
import com.openlattice.chronicle.mapstores.authorization.PrincipalTreesMapstore
import com.openlattice.chronicle.mapstores.ids.Range
import com.openlattice.chronicle.mapstores.stats.ParticipantStatsMapstore
import com.openlattice.chronicle.mapstores.storage.StudyLimitsMapstore
import com.openlattice.chronicle.mapstores.storage.StudyMapstore
import com.openlattice.chronicle.participants.ParticipantStats
import com.openlattice.chronicle.storage.StorageResolver
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Import
import javax.inject.Inject

@Configuration
@Import(PostgresPod::class, Auth0Pod::class)
class MapstoresPod {
    @Inject
    private lateinit var ptMgr: PostgresTableManager

    @Inject
    private lateinit var auth0Configuration: Auth0Configuration

    @Inject
    private lateinit var eventBus: EventBus

    @Inject
    private lateinit var storageResolver: StorageResolver

    @Inject
    private lateinit var jdbi: Jdbi

    @Bean
    fun studyLimitsMapstore(): StudyLimitsMapstore {
        return StudyLimitsMapstore(storageResolver.getPlatformStorage())
    }

    @Bean
    fun jobsMapstore(): SelfRegisteringMapStore<UUID, DistributableJob<*>> {
        return PostgresJobsMapStore(storageResolver.getPlatformStorage())
    }

    @Bean
    fun studyMapstore(): StudyMapstore {
        return StudyMapstore(storageResolver.getPlatformStorage())
    }

    @Bean
    fun permissionMapstore(): SelfRegisteringMapStore<AceKey, AceValue> {
        return PermissionMapstore(storageResolver.getPlatformStorage(), eventBus)
    }

    @Bean
    fun filteredAppsMapstore(): SelfRegisteringMapStore<UUID, KotlinDelegatedStringSet> {
        return FilteredAppsMapstore(storageResolver.getPlatformStorage())
    }

    @Bean
    fun securableObjectTypeMapstore(): SelfRegisteringMapStore<AclKey, SecurableObjectType> {
        return SecurableObjectTypeMapstore(storageResolver.getPlatformStorage())
    }

    //    @Bean
    //    public SelfRegisteringMapStore<String, UUID> aclKeysMapstore() {
    //        return new AclKeysMapstore( storageResolver.getPlatformStorage() );
    //    }
    @Bean
    fun principalsMapstore(): SelfRegisteringMapStore<AclKey, SecurablePrincipal> {
        return PrincipalMapstore(storageResolver.getPlatformStorage())
    }

    @Bean
    fun longIdsMapstore(): SelfRegisteringMapStore<String, Long> {
        return LongIdsMapstore(storageResolver.getPlatformStorage())
    }

    @Bean
    fun userMapstore(): SelfRegisteringMapStore<String, User> {
        return UserMapstore(storageResolver.getPlatformStorage())
    }

    //
    //    @Bean
    //    public SelfRegisteringMapStore<UUID, Organization> organizationsMapstore() {
    //        return new OrganizationsMapstore( storageResolver.getPlatformStorage() );
    //    }
    @Bean
    fun idGenerationMapstore(): SelfRegisteringMapStore<Long, Range> {
        return IdGenerationMapstore(storageResolver.getPlatformStorage())
    }

    @Bean
    fun auth0TokenProvider(): Auth0TokenProvider {
        return RefreshingAuth0TokenProvider(auth0Configuration)
    }

    @Bean
    fun principalTreesMapstore(): PrincipalTreesMapstore {
        return PrincipalTreesMapstore(storageResolver.getPlatformStorage())
    }

    @Bean
    fun participantStatsMapstore(): ParticipantStatsMapstore {
        return ParticipantStatsMapstore(storageResolver.getPlatformStorage())
    }

    //    @Bean
    //    public SecurablePrincipalsMapLoader securablePrincipalsMapLoader() {
    //        return new SecurablePrincipalsMapLoader();
    //    }
    //
    //    @Bean
    //    public ResolvedPrincipalTreesMapLoader resolvedPrincipalTreesMapLoader() {
    //        return new ResolvedPrincipalTreesMapLoader();
    //    }
    companion object {
        private val logger = LoggerFactory.getLogger(MapstoresPod::class.java)
    }
}