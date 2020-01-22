package ru.xander.replicator.schema;

public class CheckConstraint extends Constraint {

    private String condition;

    public String getCondition() {
        return condition;
    }

    public void setCondition(String condition) {
        this.condition = condition;
    }
}
