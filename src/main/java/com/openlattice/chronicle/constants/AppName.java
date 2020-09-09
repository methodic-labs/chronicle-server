package com.openlattice.chronicle.constants;

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
public enum AppName {
    CHRONICLE_CORE("chronicle"),
    DATA_COLLECTION("chronicle_data_collection"),
    CHRONICLE_QUESTIONNAIRES("chronicle_questionnaires");

    private final String appName;

    AppName( String appName ) {
        this.appName = appName;
    }

    @Override
    public String toString() {
        return appName;
    }
}
