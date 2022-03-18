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
package com.openlattice.chronicle.pods.servlet

import org.springframework.security.config.annotation.method.configuration.EnableGlobalMethodSecurity
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity
import com.ryantenney.metrics.spring.config.annotation.EnableMetrics
import com.geekbeast.auth0.Auth0SecurityPod
import org.springframework.context.annotation.Configuration
import org.springframework.http.HttpMethod
import kotlin.Throws
import org.springframework.security.config.annotation.web.builders.HttpSecurity
import org.springframework.web.filter.CharacterEncodingFilter
import org.springframework.security.web.csrf.CsrfFilter
import java.lang.Exception
import java.nio.charset.StandardCharsets

@Configuration
@EnableGlobalMethodSecurity(prePostEnabled = true)
@EnableWebSecurity(debug = false)
@EnableMetrics
class ChronicleServerSecurityPod : Auth0SecurityPod() {
    @Throws(Exception::class)
    override fun authorizeRequests(http: HttpSecurity) {
        //TODO: Lock these down
        http.authorizeRequests()
            .antMatchers(HttpMethod.OPTIONS).permitAll()
            .antMatchers(HttpMethod.POST, "/chronicle/study/**").permitAll()
            .antMatchers(HttpMethod.POST, "/chronicle/v2/**").permitAll()
            .antMatchers(HttpMethod.POST, "/chronicle/data/study/participant").permitAll()
            .antMatchers(HttpMethod.POST, "/chronicle/study/**").permitAll()
            .antMatchers(HttpMethod.POST, "/chronicle/v2/**").permitAll()
            .antMatchers(HttpMethod.POST, "/chronicle/v3/study/*/participant/*/ios/*").permitAll()
            .antMatchers(HttpMethod.POST, "/chronicle/v3/study/*/participant/*/android/*").permitAll()
            .antMatchers(HttpMethod.POST, "/chronicle/v3/time-user-diary/**").permitAll()
            .antMatchers(HttpMethod.GET, "/chronicle/study/**").permitAll()
            .antMatchers(HttpMethod.GET, "/chronicle/v2/**").permitAll()
            .antMatchers(HttpMethod.GET, "/chronicle/v3/study/*/settings/sensors").permitAll()
            .antMatchers(HttpMethod.GET, "/chronicle/v3/study/*/settings").permitAll()
            .antMatchers(HttpMethod.GET, "/chronicle/v3/study/*/participant/*/verify").permitAll()
            .antMatchers( "/chronicle/**").authenticated()

        val filter = CharacterEncodingFilter()
        filter.encoding = StandardCharsets.UTF_8.toString()
        filter.setForceEncoding(true)
        http.addFilterBefore(filter, CsrfFilter::class.java)
    }
}