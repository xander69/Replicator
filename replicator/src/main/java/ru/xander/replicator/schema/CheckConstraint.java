package ru.xander.replicator.schema;

/**
 * @author Alexander Shakhov
 */
public class CheckConstraint extends Constraint {

    private String condition;

    public String getCondition() {
        return condition;
    }

    public void setCondition(String condition) {
        this.condition = condition;
    }
}
