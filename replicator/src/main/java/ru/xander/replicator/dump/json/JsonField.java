package ru.xander.replicator.dump.json;

import ru.xander.replicator.schema.ColumnType;

/**
 * @author Alexander Shakhov
 */
public class JsonField {
    private ColumnType type;
    private String value;

    public ColumnType getType() {
        return type;
    }

    public void setType(ColumnType type) {
        this.type = type;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
