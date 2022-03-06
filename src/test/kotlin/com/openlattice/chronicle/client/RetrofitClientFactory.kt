package com.openlattice.chronicle.client

import com.fasterxml.jackson.databind.ObjectMapper
import com.geekbeast.mappers.mappers.ObjectMappers
import com.geekbeast.retrofit.RhizomeByteConverterFactory
import com.geekbeast.retrofit.RhizomeCallAdapterFactory
import com.geekbeast.retrofit.RhizomeJacksonConverterFactory
import okhttp3.Interceptor
import okhttp3.OkHttpClient
import retrofit2.CallAdapter
import retrofit2.Retrofit
import java.util.concurrent.TimeUnit
import java.util.function.Consumer
import java.util.function.Supplier

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class RetrofitClientFactory{
    companion object {
        const val BASE_URL = "https://api.openlattice.com/"
        const val LOCAL_BASE_URL = "http://localhost:8080/"
        const val STAGING_BASE_URL = "https://api.staging.openlattice.com/"
        const val TESTING_BASE_URL = "http://localhost:8080/"
        const val TESTING_CHRONICLE_URL = "http://localhost:8080/"

        private val jsonMapper = ObjectMappers.getJsonMapper()
        /**
         * Create a new client with no authentication
         */
        fun newClient(environment: Environment): Retrofit {
            return decorateWithOpenLatticeFactories(
                createBaseRhizomeRetrofitBuilder(environment, OkHttpClient.Builder())
            ).build()
        }

        fun newClient(jwtToken: () -> String): Retrofit {
            return newClient(Environment.PRODUCTION, jwtToken)
        }

        fun newClient(
            environment: Environment,
            jwtToken: Supplier<String>,
            callFactory: CallAdapter.Factory
        ): Retrofit {
            val httpBuilder = okHttpClientWithOpenLatticeAuth(jwtToken)
            return decorateWithFactories(createBaseRhizomeRetrofitBuilder(environment, httpBuilder), callFactory)
                .build()
        }

        fun newClient(
            environment: Environment,
            jwtToken:() -> String
        ): Retrofit {
            val httpBuilder = okHttpClientWithOpenLatticeAuth(jwtToken)
            return decorateWithOpenLatticeFactories(createBaseRhizomeRetrofitBuilder(environment, httpBuilder)).build()
        }

        fun newClient(baseUrl: String, jwtToken: Supplier<String>): Retrofit {
            val httpBuilder = okHttpClientWithOpenLatticeAuth(jwtToken)
            return decorateWithOpenLatticeFactories(createBaseRhizomeRetrofitBuilder(baseUrl, httpBuilder.build()))
                .build()
        }

        fun createBaseRhizomeRetrofitBuilder(
            environment: Environment,
            httpBuilder: OkHttpClient.Builder
        ): Retrofit.Builder {
            return createBaseRhizomeRetrofitBuilder(environment.baseUrl, httpBuilder.build())
        }

        fun createBaseRhizomeRetrofitBuilder(baseUrl: String, httpClient: OkHttpClient?): Retrofit.Builder {
            return Retrofit.Builder().baseUrl(baseUrl).client(httpClient)
        }

        fun decorateWithOpenLatticeFactories(builder: Retrofit.Builder): Retrofit.Builder {
            return decorateWithFactories(builder, RhizomeCallAdapterFactory())
        }

        fun decorateWithFactories(builder: Retrofit.Builder, callFactory: CallAdapter.Factory?): Retrofit.Builder {
            return builder.addConverterFactory(RhizomeByteConverterFactory())
                .addConverterFactory(RhizomeJacksonConverterFactory(RetrofitClientFactory.jsonMapper))
                .addCallAdapterFactory(callFactory)
        }

        fun okHttpClient(): OkHttpClient.Builder {
            return OkHttpClient.Builder()
                .readTimeout(0, TimeUnit.MILLISECONDS)
                .writeTimeout(0, TimeUnit.MILLISECONDS)
                .connectTimeout(0, TimeUnit.MILLISECONDS)
        }

        fun okHttpClientWithOpenLatticeAuth(jwtToken: Supplier<String>): OkHttpClient.Builder {
            return okHttpClient()
                .addInterceptor { chain: Interceptor.Chain ->
                    chain.proceed(
                        chain.request().newBuilder()
                            .addHeader("Authorization", "Bearer " + jwtToken.get())
                            .build()
                    )
                }
        }

        fun configureObjectMapper(c: Consumer<ObjectMapper>) {
            c.accept(jsonMapper)
        }
    }
    
    
}

enum class Environment(val baseUrl: String) {
    PRODUCTION(RetrofitClientFactory.BASE_URL),
    STAGING(RetrofitClientFactory.STAGING_BASE_URL),
    LOCAL(RetrofitClientFactory.LOCAL_BASE_URL),
    TESTING(RetrofitClientFactory.TESTING_BASE_URL),
    TESTING_CHRONICLE(RetrofitClientFactory.TESTING_CHRONICLE_URL);
}
