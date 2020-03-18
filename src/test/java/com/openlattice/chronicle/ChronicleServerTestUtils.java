package com.openlattice.chronicle;

import com.google.common.collect.ImmutableSet;
import com.openlattice.authorization.Principal;
import com.openlattice.authorization.PrincipalType;
import com.openlattice.edm.EdmApi;
import com.openlattice.edm.EntitySet;
import com.openlattice.entitysets.EntitySetsApi;
import com.openlattice.organization.OrganizationsApi;
import com.openlattice.organizations.Organization;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

import java.util.*;

import static com.openlattice.chronicle.constants.EdmConstants.*;

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */

public class ChronicleServerTestUtils {

    private static final String CAFE_ORG_ID = "7349c446-2acc-4d14-b2a9-a13be39cff93";

    public static String getRandomElement( List<String> userTypes ) {
        Random random = new Random(  );
        return userTypes.get( random.nextInt(userTypes.size()) );
    }

    public static Set<Object> getRandomElements( List<String> userTypes ) {
        Set<Object> result = new HashSet<>(  );
        Random random = new Random(  );
        int numItems = random.nextInt(userTypes.size() + 1);


        for (int i = 0; i < numItems; i++) {
            result.add( getRandomElement( userTypes ) );
        }
        return result;
    }

    private static Set<EntitySet> buildEntitySets ( EdmApi edmApi ) {
        Set<EntitySet> entitySets = new HashSet<>(  );

        Map<String, FullQualifiedName> entitySetTypeFQNMap = new HashMap<>(  );
        entitySetTypeFQNMap.put( STUDY_ENTITY_SET_NAME, STUDY_FQN );
        entitySetTypeFQNMap.put( CHRONICLE_USER_APPS, APP_DATA_FQN );
        entitySetTypeFQNMap.put( DATA_ENTITY_SET_NAME, APP_DATA_FQN );
        entitySetTypeFQNMap.put( RECORDED_BY_ENTITY_SET_NAME, RECORDED_BY_FQN );
        entitySetTypeFQNMap.put( USED_BY_ENTITY_SET_NAME, USED_BY_FQN );
        entitySetTypeFQNMap.put( DEVICES_ENTITY_SET_NAME, DEVICE_FQN );

        for ( Map.Entry<String, FullQualifiedName> entity : entitySetTypeFQNMap.entrySet() ) {
            UUID entityTypeId = edmApi.getEntityTypeId( entity.getValue() );
            EntitySet entitySet = new EntitySet(
                    entityTypeId,
                    entity.getKey(),
                    "Test entity set",
                    Optional.of( "Test entity set description" ),
                    ImmutableSet.of(),
                    Optional.empty(),
                    UUID.fromString( CAFE_ORG_ID ),
                    Optional.empty(),
                    Optional.empty()
            );
            entitySets.add( entitySet );
        }

        return entitySets;
    }

    // mapping from entitySetName -> EntitySetId;
    public static Map<String, UUID> createEntitySets( EntitySetsApi entitySetsApi, EdmApi edmApi ) {
        Set<EntitySet> entitySets = buildEntitySets(edmApi);
        return entitySetsApi.createEntitySets( entitySets );
    }

    // required before creating
    public static void createOrganization( OrganizationsApi organizationsApi ) {

        Principal principal = new Principal( PrincipalType.ORGANIZATION, RandomStringUtils.randomAlphanumeric( 10 ) );
        Organization organization = new Organization(
                Optional.of( UUID.fromString( CAFE_ORG_ID ) ),
                principal,
                "Cafe organization",
                Optional.of( "cafe organization description" ),
                ImmutableSet.of(),
                ImmutableSet.of(),
                ImmutableSet.of()
        );

        organizationsApi.createOrganizationIfNotExists( organization );
    }
}
