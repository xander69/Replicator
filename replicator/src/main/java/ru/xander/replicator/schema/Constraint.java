package ru.xander.replicator.schema;

import com.fasterxml.jackson.annotation.JsonBackReference;

/**
 * @author Alexander Shakhov
 */
public abstract class Constraint {

    @JsonBackReference
    private Table table;
    private String name;
    private String columnName;
    private Boolean enabled;

    public Table getTable() {
        return table;
    }

    public void setTable(Table table) {
        this.table = table;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public Boolean getEnabled() {
        return enabled;
    }

    public void setEnabled(Boolean enabled) {
        this.enabled = enabled;
    }
}
