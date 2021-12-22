

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

package com.openlattice.chronicle.mapstores;

import com.auth0.json.mgmt.users.User;
import com.geekbeast.rhizome.jobs.DistributableJob;
import com.geekbeast.rhizome.jobs.PostgresJobsMapStore;
import com.google.common.eventbus.EventBus;
import com.kryptnostic.rhizome.mapstores.SelfRegisteringMapStore;
import com.openlattice.apps.App;
import com.openlattice.apps.AppConfigKey;
import com.openlattice.apps.AppTypeSetting;
import com.openlattice.assembler.EntitySetAssemblyKey;
import com.openlattice.assembler.MaterializedEntitySet;
import com.openlattice.assembler.OrganizationAssembly;
import com.openlattice.auditing.AuditRecordEntitySetConfiguration;
import com.openlattice.auth0.Auth0Pod;
import com.openlattice.auth0.Auth0TokenProvider;
import com.openlattice.auth0.AwsAuth0TokenProvider;
import com.openlattice.authentication.Auth0Configuration;
import com.openlattice.authorization.AceKey;
import com.openlattice.authorization.AceValue;
import com.openlattice.authorization.AclKey;
import com.openlattice.authorization.SecurablePrincipal;
import com.openlattice.authorization.securable.SecurableObjectType;
import com.openlattice.chronicle.authorization.AceKey;
import com.openlattice.chronicle.authorization.AceValue;
import com.openlattice.chronicle.authorization.AclKey;
import com.openlattice.chronicle.authorization.SecurableObjectType;
import com.openlattice.chronicle.authorization.SecurablePrincipal;
import com.openlattice.chronicle.authorization.principals.PrincipalMapstore;
import com.openlattice.chronicle.mapstores.authorization.PermissionMapstore;
import com.openlattice.chronicle.mapstores.ids.IdGenerationMapstore;
import com.openlattice.chronicle.mapstores.ids.Range;
import com.openlattice.chronicle.mapstores.principals.PrincipalTreesMapstore;
import com.openlattice.collaborations.mapstores.CollaborationMapstore;
import com.openlattice.collaborations.mapstores.ProjectedTablesMapstore;
import com.openlattice.collections.CollectionTemplateKey;
import com.openlattice.collections.EntitySetCollection;
import com.openlattice.collections.EntityTypeCollection;
import com.openlattice.collections.mapstores.EntitySetCollectionConfigMapstore;
import com.openlattice.collections.mapstores.EntitySetCollectionMapstore;
import com.openlattice.collections.mapstores.EntityTypeCollectionMapstore;
import com.openlattice.datasets.ObjectMetadataMapstore;
import com.openlattice.directory.MaterializedViewAccount;
import com.openlattice.edm.EntitySet;
import com.openlattice.edm.type.AssociationType;
import com.openlattice.edm.type.EntityType;
import com.openlattice.edm.type.PropertyType;
import com.openlattice.entitysets.DeletedEntitySetMapstore;
import com.openlattice.hazelcast.mapstores.shuttle.IntegrationJobsMapstore;
import com.openlattice.hazelcast.mapstores.shuttle.IntegrationsMapstore;
import com.openlattice.ids.IdGenerationMapstore;
import com.openlattice.ids.Range;
import com.openlattice.ids.mapstores.LongIdsMapstore;
import com.openlattice.linking.mapstores.LinkingFeedbackMapstore;
import com.openlattice.notifications.sms.SmsInformationMapstore;
import com.openlattice.organization.ExternalColumn;
import com.openlattice.organization.ExternalTable;
import com.openlattice.organizations.Organization;
import com.openlattice.organizations.mapstores.ExternalColumnsMapstore;
import com.openlattice.organizations.mapstores.ExternalTablesMapstore;
import com.openlattice.organizations.mapstores.OrganizationDatabasesMapstore;
import com.openlattice.organizations.mapstores.OrganizationsMapstore;
import com.openlattice.organizations.mapstores.WarehousesMapstore;
import com.openlattice.postgres.PostgresPod;
import com.openlattice.postgres.PostgresTableManager;
import com.openlattice.requests.Status;
import com.openlattice.scheduling.mapstores.ScheduledTasksMapstore;
import com.openlattice.shuttle.Integration;
import com.openlattice.shuttle.IntegrationJob;
import com.zaxxer.hikari.HikariDataSource;
import java.util.UUID;
import javax.inject.Inject;
import org.jdbi.v3.core.Jdbi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import( { PostgresPod.class, Auth0Pod.class } )
public class MapstoresPod {
    private static final Logger logger = LoggerFactory.getLogger( MapstoresPod.class );

    @Inject
    private HikariDataSource hikariDataSource;

    @Inject
    private PostgresTableManager ptMgr;

    @Inject
    private Auth0Configuration auth0Configuration;

    @Inject
    private EventBus eventBus;

    @Inject
    private Jdbi jdbi;

    @Bean
    public SelfRegisteringMapStore<UUID, DistributableJob<?>> jobsMapstore() {
        return new PostgresJobsMapStore( hikariDataSource );
    }

    @Bean
    public SelfRegisteringMapStore<AceKey, AceValue> permissionMapstore() {
        return new PermissionMapstore( hikariDataSource, eventBus );
    }

    @Bean
    public SelfRegisteringMapStore<AclKey, SecurableObjectType> securableObjectTypeMapstore() {
        return new SecurableObjectTypeMapstore( hikariDataSource );
    }

    @Bean
    public SelfRegisteringMapStore<String, UUID> aclKeysMapstore() {
        return new AclKeysMapstore( hikariDataSource );
    }

    @Bean
    public SelfRegisteringMapStore<UUID, String> namesMapstore() {
        return new NamesMapstore( hikariDataSource );
    }


    @Bean
    public SelfRegisteringMapStore<AclKey, SecurablePrincipal> principalsMapstore() {
        return new PrincipalMapstore( hikariDataSource );
    }

    @Bean
    public SelfRegisteringMapStore<String, Long> longIdsMapstore() {
        return new LongIdsMapstore( hikariDataSource );
    }

    @Bean
    public SelfRegisteringMapStore<String, User> userMapstore() {
        return new UserMapstore( hikariDataSource );
    }

    @Bean
    public SelfRegisteringMapStore<UUID, Organization> organizationsMapstore() {
        return new OrganizationsMapstore( hikariDataSource );
    }


    @Bean
    public SelfRegisteringMapStore<Long, Range> idGenerationMapstore() {
        return new IdGenerationMapstore( hikariDataSource );
    }



    @Bean
    public Auth0TokenProvider auth0TokenProvider() {
        return new AwsAuth0TokenProvider( auth0Configuration );
    }

    @Bean
    public PrincipalTreesMapstore principalTreesMapstore() {
        return new PrincipalTreesMapstore( hikariDataSource );
    }


    @Bean
    public SecurablePrincipalsMapLoader securablePrincipalsMapLoader() {
        return new SecurablePrincipalsMapLoader();
    }

    @Bean
    public ResolvedPrincipalTreesMapLoader resolvedPrincipalTreesMapLoader() {
        return new ResolvedPrincipalTreesMapLoader();
    }

}