package com.openlattice.chronicle.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.kryptnostic.rhizome.configuration.annotation.ReloadableConfiguration;
import java.util.Objects;

@ReloadableConfiguration(uri="chronicle.yaml")
public class ChronicleConfiguration {
    private final String USER_KEY = "user";
    private final String PASSWORD_KEY = "password";

    private final String user;
    private final String password;

    public ChronicleConfiguration(
            @JsonProperty( USER_KEY ) String user,
            @JsonProperty( PASSWORD_KEY ) String password ) {
        this.user = user;
        this.password = password;
    }

    @JsonProperty( USER_KEY )
    public String getUser() {
        return user;
    }

    @JsonProperty( PASSWORD_KEY )
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
