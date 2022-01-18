package com.openlattice.chronicle.util

import com.hazelcast.map.IMap
import org.apache.olingo.commons.api.edm.FullQualifiedName

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
fun <K, V> getSafely(m: IMap<K, V>, key: K): V? {
    return m[key]
}

fun fqnToString(fqn: FullQualifiedName): String? {
    return fqn.fullQualifiedNameAsString
}