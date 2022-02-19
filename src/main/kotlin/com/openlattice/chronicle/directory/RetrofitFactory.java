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

package com.openlattice.chronicle.directory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.geekbeast.mappers.mappers.ObjectMappers;
import com.geekbeast.retrofit.RhizomeByteConverterFactory;
import com.geekbeast.retrofit.RhizomeCallAdapterFactory;
import com.geekbeast.retrofit.RhizomeJacksonConverterFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;
import okhttp3.OkHttpClient;
import retrofit2.CallAdapter;
import retrofit2.Retrofit;

public final class RetrofitFactory {
    private static final ObjectMapper jsonMapper = ObjectMappers.getJsonMapper();

    private RetrofitFactory() {
    }

    public static Retrofit newClient( String baseUrl, Supplier<String> jwtToken ) {
        OkHttpClient.Builder httpBuilder = okHttpClientWithOpenLatticeAuth( jwtToken );
        return decorateWithOpenLatticeFactories( createBaseRhizomeRetrofitBuilder( baseUrl, httpBuilder.build() ) )
                .build();
    }

    public static Retrofit.Builder createBaseRhizomeRetrofitBuilder( String baseUrl, OkHttpClient httpClient ) {
        return new Retrofit.Builder().baseUrl( baseUrl ).client( httpClient );
    }

    public static Retrofit.Builder decorateWithOpenLatticeFactories( Retrofit.Builder builder ) {
        return decorateWithFactories( builder, new RhizomeCallAdapterFactory() );
    }

    public static Retrofit.Builder decorateWithFactories( Retrofit.Builder builder, CallAdapter.Factory callFactory ) {
        return builder.addConverterFactory( new RhizomeByteConverterFactory() )
                .addConverterFactory( new RhizomeJacksonConverterFactory( jsonMapper ) )
                .addCallAdapterFactory( callFactory );
    }

    public static OkHttpClient.Builder okHttpClient() {
        return new OkHttpClient.Builder()
                .readTimeout( 0, TimeUnit.MILLISECONDS )
                .writeTimeout( 0, TimeUnit.MILLISECONDS )
                .connectTimeout( 0, TimeUnit.MILLISECONDS );
    }

    public static OkHttpClient.Builder okHttpClientWithOpenLatticeAuth( Supplier<String> jwtToken ) {
        return okHttpClient()
                .addInterceptor( chain -> chain.proceed(
                        chain.request().newBuilder()
                                .addHeader( "Authorization", "Bearer " + jwtToken.get() )
                                .build()
                        )
                );
    }

    public static void configureObjectMapper( Consumer<ObjectMapper> c ) {
        c.accept( jsonMapper );
    }
}
