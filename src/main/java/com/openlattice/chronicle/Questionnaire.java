package com.openlattice.chronicle;

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
public class Questionnaire {
    private String name;
    private String cronExpression;
    private String description;
    private Boolean active;

    public Questionnaire(String name, String cronExpression, String description, Boolean active) {
        this.name = name;
        this.cronExpression = cronExpression;
        this.description = description;
        this.active = active;
    }

    public String getName() {
        return name;
    }

    public void setName( String name ) {
        this.name = name;
    }

    public String getCronExpression() {
        return cronExpression;
    }

    public void setCronExpression( String cronExpression ) {
        this.cronExpression = cronExpression;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription( String description ) {
        this.description = description;
    }

    public Boolean getActive() {
        return active;
    }

    public void setActive( Boolean active ) {
        this.active = active;
    }
}
