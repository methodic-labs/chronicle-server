package com.openlattice.chronicle.configuration;

import com.kryptnostic.rhizome.configuration.annotation.ReloadableConfiguration;
import java.util.Objects;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@ReloadableConfiguration(uri="chronicle.yaml")
public class ChronicleConfiguration {
    private final String user;

    private final String password;

    public ChronicleConfiguration( String user, String password ) {
        this.user = user;
        this.password = password;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }

    @Override public boolean equals( Object o ) {
        if ( this == o ) { return true; }
        if ( !( o instanceof ChronicleConfiguration ) ) { return false; }
        ChronicleConfiguration that = (ChronicleConfiguration) o;
        return Objects.equals( user, that.user ) &&
                Objects.equals( password, that.password );
    }

    @Override public int hashCode() {
        return Objects.hash( user, password );
    }

    @Override public String toString() {
        return "ChronicleConfiguration{" +
                "user='" + user + '\'' +
                ", password='" + password + '\'' +
                '}';
    }
}
