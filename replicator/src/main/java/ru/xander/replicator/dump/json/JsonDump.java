package ru.xander.replicator.dump.json;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import ru.xander.replicator.dump.data.TableRowExtractor;
import ru.xander.replicator.schema.Table;

/**
 * @author Alexander Shakhov
 */
public class JsonDump {

    @JsonProperty("table")
    @JsonSerialize(using = TableSerializer.class)
    private Table table;
    @JsonProperty("rows")
    @JsonSerialize(using = RowsSerializer.class)
    private TableRowExtractor rowExtractor;

    public Table getTable() {
        return table;
    }

    public void setTable(Table table) {
        this.table = table;
    }

    public TableRowExtractor getRowExtractor() {
        return rowExtractor;
    }

    public void setRowExtractor(TableRowExtractor rowExtractor) {
        this.rowExtractor = rowExtractor;
    }
}
