package com.openlattice.chronicle.constants;

import java.time.OffsetDateTime;

public class OutputConstants {

    private OutputConstants() {
    }

    // prefix for column names
    public static final String APP_PREFIX = "app_";
    public static final String USER_PREFIX = "user_";

    // minimum datetime
    public static final OffsetDateTime MINIMUM_DATE = OffsetDateTime
            .parse("2020-01-01T00:00:00Z");

}
