package com.openlattice.chronicle.jobs

import com.fasterxml.jackson.annotation.JsonTypeInfo

/**
 * @author Solomon Tang <solomon@openlattice.com>
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "@type")
interface ChronicleJobDefinition {

}