package com.openlattice.chronicle.configuration

import com.fasterxml.jackson.annotation.JsonProperty
import com.geekbeast.rhizome.configuration.Configuration
import com.geekbeast.rhizome.configuration.ConfigurationKey
import com.geekbeast.rhizome.configuration.SimpleConfigurationKey
import com.geekbeast.rhizome.configuration.configuration.annotation.ReloadableConfiguration

const val BUCKET_NAME = "bucketName"
const val REGION_NAME = "regionName"
const val TIME_TO_LIVE = "timeToLive"
const val ACCESS_KEY_ID = "accessKeyId"
const val SECRET_ACCESS_KEY = "secretAccessKey"
const val SEARCH_CONFIGURATION = "searchConfiguration"

@ReloadableConfiguration(uri = "chronicle.yaml")
data class ChronicleConfiguration(
    @JsonProperty(BUCKET_NAME) val bucketName: String,
    @JsonProperty(REGION_NAME) val regionName: String,
    @JsonProperty(TIME_TO_LIVE) val timeToLive: Long,
    @JsonProperty(ACCESS_KEY_ID) val accessKeyId: String,
    @JsonProperty(SECRET_ACCESS_KEY) val secretAccessKey: String,
    @JsonProperty(SEARCH_CONFIGURATION) val searchConfiguration: SearchConfiguration,
    @JsonProperty("storageConfiguration") val storageConfiguration: ChronicleStorageConfiguration = ChronicleStorageConfiguration()
) : Configuration {

    companion object {
        @JvmField
        val key = SimpleConfigurationKey("chronicle.yaml")
    }

    override fun getKey(): ConfigurationKey {
        return key
    }
}