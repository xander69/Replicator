package ru.xander.replicator.schema;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * @author Alexander Shakhov
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class CheckConstraint extends Constraint {

    private String condition;

    public String getCondition() {
        return condition;
    }

    public void setCondition(String condition) {
        this.condition = condition;
    }
}
