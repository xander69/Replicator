package ru.xander.replicator.dump.data;

import ru.xander.replicator.schema.Column;

/**
 * @author Alexander Shakhov
 */
public class TableField {

    private final Column column;
    private Object value;

    public TableField(Column column) {
        this.column = column;
    }

    public Column getColumn() {
        return column;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }
}
