package com.openlattice;

import static com.openlattice.chronicle.ChronicleFields.ADDITIONAL_INFO;
import static com.openlattice.chronicle.ChronicleFields.BRAND;
import static com.openlattice.chronicle.ChronicleFields.CODENAME;
import static com.openlattice.chronicle.ChronicleFields.DEVICE;
import static com.openlattice.chronicle.ChronicleFields.DEVICE_ID;
import static com.openlattice.chronicle.ChronicleFields.MODEL;
import static com.openlattice.chronicle.ChronicleFields.OS_VERSION;
import static com.openlattice.chronicle.ChronicleFields.PRODUCT;
import static com.openlattice.chronicle.ChronicleFields.SDK_VERSION;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import com.openlattice.chronicle.sources.AndroidDevice;
import com.openlattice.chronicle.sources.SourceDevice;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Ignore
public class SourceDeviceSerializationTest {
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final Logger       logger = LoggerFactory.getLogger( SourceDeviceSerializationTest.class );

    @JsonTypeInfo(use=JsonTypeInfo.Id.CLASS, include= JsonTypeInfo.As.PROPERTY, property="@class")
    public static  class AndroidDeviceTest implements SourceDevice {
        private final String              device;
        private final String              model;
        private final String              codename;
        private final String              brand;
        private final String              osVersion;
        private final String              sdkVersion;
        private final String              product;
        private final String              deviceId;
        private final Map<String, Object> additionalInfo;

        @JsonCreator
        public AndroidDeviceTest(
                @JsonProperty( DEVICE ) String device,
                @JsonProperty( MODEL ) String model,
                @JsonProperty( CODENAME ) String codename,
                @JsonProperty( BRAND ) String brand,
                @JsonProperty( OS_VERSION ) String osVersion,
                @JsonProperty( SDK_VERSION ) String sdkVersion,
                @JsonProperty( PRODUCT ) String product,
                @JsonProperty( DEVICE_ID ) String deviceId,
                @JsonProperty( ADDITIONAL_INFO ) Map<String, Object> additionalInfo ) {
            this.device = device;
            this.model = model;
            this.codename = codename;
            this.brand = brand;
            this.osVersion=osVersion;
            this.sdkVersion = sdkVersion;
            this.product = product;
            this.deviceId = deviceId;
            this.additionalInfo = new HashMap<>();
        }

        @Override public boolean equals( Object o ) {
            if ( this == o ) { return true; }
            if ( !( o instanceof AndroidDeviceTest ) ) { return false; }
            AndroidDeviceTest that = (AndroidDeviceTest) o;
            return Objects.equals( device, that.device ) &&
                    Objects.equals( model, that.model ) &&
                    Objects.equals( codename, that.codename ) &&
                    Objects.equals( brand, that.brand ) &&
                    Objects.equals( osVersion, that.osVersion ) &&
                    Objects.equals( sdkVersion, that.sdkVersion ) &&
                    Objects.equals( product, that.product ) &&
                    Objects.equals( deviceId, that.deviceId ) &&
                    Objects.equals( additionalInfo, that.additionalInfo );
        }

        @Override public int hashCode() {
            return Objects.hash( device, model, codename, brand, osVersion, sdkVersion, product, deviceId, additionalInfo );
        }

        @Override public String toString() {
            return "AndroidDevice{" +
                    "device='" + device + '\'' +
                    ", model='" + model + '\'' +
                    ", codename='" + codename + '\'' +
                    ", brand='" + brand + '\'' +
                    ", osVersion='" + osVersion + '\'' +
                    ", sdkVersion='" + sdkVersion + '\'' +
                    ", product='" + product + '\'' +
                    ", deviceId='" + deviceId + '\'' +
                    ", additionalInfo=" + additionalInfo +
                    '}';
        }

        @JsonProperty( DEVICE )
        public String getDevice() {
            return device;
        }

        @JsonProperty( MODEL )
        public String getModel() {
            return model;
        }

        @JsonProperty( CODENAME )
        public String getCodename() {
            return codename;
        }

        @JsonProperty( BRAND )
        public String getBrand() {
            return brand;
        }

        @JsonProperty( SDK_VERSION )
        public String getSdkVersion() {
            return sdkVersion;
        }

        @JsonProperty( OS_VERSION )
        public String getOsVersion() {
            return sdkVersion;
        }

        @JsonProperty( PRODUCT )
        public String getProduct() {
            return product;
        }

        @JsonProperty( DEVICE_ID )
        public String getDeviceId() {
            return deviceId;
        }

        @JsonProperty( ADDITIONAL_INFO )
        public Map<String, Object> getAdditionalInfo() {
            return additionalInfo;
        }
    }


    @Test
    public void testSerdes() throws JsonProcessingException {
        final var adt = new AtomicReference<>(new AndroidDeviceTest(
                "Samsung",
                "P",
                "Chocholate Chip",
                "Samsung",
                "21",
                "21",
                "",
                "",
                new HashMap<>()
        ));

        final var json = mapper.writeValueAsString( adt );
        logger.info("JSON: {}", json );
        final var attempt = mapper.readValue( json , new TypeReference<AtomicReference<AndroidDeviceTest>>(){} );
    }

}
