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
package com.openlattice.chronicle.pods

import com.geekbeast.rhizome.configuration.servlets.DispatcherServletConfiguration
import com.google.common.collect.Lists
import com.openlattice.chronicle.pods.servlet.ChronicleServerMvcPod
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class ChronicleServerServletsPod {

    @Bean
    fun chronicleServlet(): DispatcherServletConfiguration {
        return DispatcherServletConfiguration(
            "chronicle", arrayOf("/chronicle/*", "/datastore/*"),
            1,
            Lists.newArrayList<Class<*>>(ChronicleServerMvcPod::class.java)
        )
    }
}
