package ru.xander.replicator.schema;

public class PrimaryKey extends Constraint {

    private String columnName;

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }
}
