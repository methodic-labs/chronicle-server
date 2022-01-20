package com.openlattice.chronicle.hazelcast.serializers

import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest
import com.openlattice.chronicle.authorization.PrincipalType
import com.openlattice.chronicle.authorization.SecurablePrincipal
import com.openlattice.chronicle.serializers.SecurablePrincipalStreamSerializer
import com.openlattice.chronicle.util.TestDataFactory

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class SecurablePrincipalStreamSerializerTest:
    AbstractStreamSerializerTest<SecurablePrincipalStreamSerializer, SecurablePrincipal>() {
    override fun createSerializer(): SecurablePrincipalStreamSerializer {
        return SecurablePrincipalStreamSerializer()
    }

    override fun createInput(): SecurablePrincipal {
        return TestDataFactory.securablePrincipal(PrincipalType.USER)
    }
}