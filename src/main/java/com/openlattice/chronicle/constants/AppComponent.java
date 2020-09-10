package com.openlattice.chronicle.constants;

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
public enum AppComponent {
    CHRONICLE_CORE("chronicle"),
    DATA_COLLECTION("chronicle_data_collection"),
    CHRONICLE_QUESTIONNAIRES("chronicle_questionnaires");

    private final String component;

    AppComponent( String appName ) {
        this.component = appName;
    }

    @Override
    public String toString() {
        return component;
    }
}
