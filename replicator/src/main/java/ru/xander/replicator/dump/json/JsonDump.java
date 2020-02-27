package ru.xander.replicator.dump.json;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import ru.xander.replicator.schema.Table;
import ru.xander.replicator.schema.TableRowCursor;

/**
 * @author Alexander Shakhov
 */
public class JsonDump {

    @JsonProperty("table")
    @JsonSerialize(using = TableSerializer.class)
    private Table table;
    @JsonProperty("rows")
    @JsonSerialize(using = RowsSerializer.class)
    private TableRowCursor cursor;

    public Table getTable() {
        return table;
    }

    public void setTable(Table table) {
        this.table = table;
    }

    public TableRowCursor getCursor() {
        return cursor;
    }

    public void setCursor(TableRowCursor cursor) {
        this.cursor = cursor;
    }
}
