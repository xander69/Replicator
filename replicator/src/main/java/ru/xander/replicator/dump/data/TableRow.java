package ru.xander.replicator.dump.data;

import ru.xander.replicator.schema.Table;

/**
 * @author Alexander Shakhov
 */
public class TableRow {

    private Table table;
    private TableField[] fields;

    public TableRow() {
    }

    public TableRow(Table table, TableField[] fields) {
        this.table = table;
        this.fields = fields;
    }

    public Table getTable() {
        return table;
    }

    public void setTable(Table table) {
        this.table = table;
    }

    public TableField[] getFields() {
        return fields;
    }

    public void setFields(TableField[] fields) {
        this.fields = fields;
    }
}
