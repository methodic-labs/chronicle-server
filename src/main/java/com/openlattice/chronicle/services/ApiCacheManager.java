package com.openlattice.chronicle.services;

import com.geekbeast.auth0.Auth0Delegate;
import com.geekbeast.authentication.Auth0Configuration;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.openlattice.chronicle.configuration.ChronicleConfiguration;
import com.openlattice.client.ApiClient;
import com.openlattice.client.RetrofitFactory;

import java.util.concurrent.TimeUnit;

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
public class ApiCacheManager {

    private final String username;
    private final String password;

    public final transient LoadingCache<Class<?>, ApiClient> prodApiClientCache;
    public final transient LoadingCache<Class<?>, ApiClient> intApiClientCache;
    private final          Auth0Delegate                     auth0Client;

    public ApiCacheManager(
            ChronicleConfiguration chronicleConfiguration,
            Auth0Configuration auth0Configuration ) {

        this.auth0Client = Auth0Delegate.fromConfig( auth0Configuration );
        this.username = chronicleConfiguration.getUser();
        this.password = chronicleConfiguration.getPassword();

        prodApiClientCache = CacheBuilder
                .newBuilder()
                .expireAfterWrite( 9, TimeUnit.HOURS )
                .build( new CacheLoader<Class<?>, ApiClient>() {
                    @Override
                    public ApiClient load( Class<?> key ) throws Exception {

                        String jwtToken = auth0Client.getIdToken( username, password );
                        return new ApiClient( RetrofitFactory.Environment.PRODUCTION, () -> jwtToken );
                    }
                } );

        intApiClientCache = CacheBuilder
                .newBuilder()
                .expireAfterWrite( 9, TimeUnit.HOURS )
                .build( new CacheLoader<Class<?>, ApiClient>() {
                    @Override
                    public ApiClient load( Class<?> key ) throws Exception {

                        String jwtToken = auth0Client.getIdToken( username, password );
                        return new ApiClient( RetrofitFactory.Environment.PROD_INTEGRATION, () -> jwtToken );
                    }
                } );
    }
}
