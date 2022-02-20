/*
 * Copyright (C) 2018. OpenLattice, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * You can contact the owner of the copyright at support@openlattice.com
 */
package com.openlattice.chronicle.directory

import com.fasterxml.jackson.databind.ObjectMapper
import com.geekbeast.mappers.mappers.ObjectMappers
import retrofit2.Retrofit
import com.geekbeast.retrofit.RhizomeCallAdapterFactory
import retrofit2.CallAdapter
import com.geekbeast.retrofit.RhizomeByteConverterFactory
import com.geekbeast.retrofit.RhizomeJacksonConverterFactory
import okhttp3.Interceptor
import okhttp3.OkHttpClient
import java.util.concurrent.TimeUnit
import java.util.function.Consumer
import java.util.function.Supplier

object RetrofitFactory {

    private val jsonMapper = ObjectMappers.getJsonMapper()

    fun newClient(baseUrl: String, jwtToken: Supplier<String>): Retrofit {
        val httpBuilder = okHttpClientWithAuth(jwtToken)
        return decorateWithOpenLatticeFactories(createBaseRhizomeRetrofitBuilder(baseUrl, httpBuilder.build()))
            .build()
    }

    fun createBaseRhizomeRetrofitBuilder(baseUrl: String, httpClient: OkHttpClient): Retrofit.Builder {
        return Retrofit.Builder().baseUrl(baseUrl).client(httpClient)
    }

    fun decorateWithOpenLatticeFactories(builder: Retrofit.Builder): Retrofit.Builder {
        return decorateWithFactories(builder, RhizomeCallAdapterFactory())
    }

    fun decorateWithFactories(builder: Retrofit.Builder, callFactory: CallAdapter.Factory): Retrofit.Builder {
        return builder.addConverterFactory(RhizomeByteConverterFactory())
            .addConverterFactory(RhizomeJacksonConverterFactory(jsonMapper))
            .addCallAdapterFactory(callFactory)
    }

    fun okHttpClient(): OkHttpClient.Builder {
        return OkHttpClient.Builder()
            .readTimeout(0, TimeUnit.MILLISECONDS)
            .writeTimeout(0, TimeUnit.MILLISECONDS)
            .connectTimeout(0, TimeUnit.MILLISECONDS)
    }

    fun okHttpClientWithAuth(jwtToken: Supplier<String>): OkHttpClient.Builder {
        return okHttpClient()
            .addInterceptor { chain: Interceptor.Chain ->
                chain.proceed(
                    chain.request().newBuilder()
                        .addHeader("Authorization", "Bearer " + jwtToken.get())
                        .build()
                )
            }
    }

    fun configureObjectMapper(c: Consumer<ObjectMapper?>) {
        c.accept(jsonMapper)
    }
}
