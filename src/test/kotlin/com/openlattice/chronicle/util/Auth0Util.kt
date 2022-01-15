package com.openlattice.chronicle.util

import com.kryptnostic.rhizome.pods.ConfigurationLoader
import com.openlattice.ResourceConfigurationLoader
import com.openlattice.auth0.Auth0Delegate
import com.openlattice.authentication.Auth0Configuration

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class Auth0Util {
    companion object {
        @JvmStatic
        fun main(args: Array<String>) {
            val auth0 =
                Auth0Delegate.fromConfig(ResourceConfigurationLoader.loadConfiguration(Auth0Configuration::class.java))
//                auth0.getIdToken()

        }
    }
}