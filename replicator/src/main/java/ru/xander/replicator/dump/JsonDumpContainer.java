package ru.xander.replicator.dump;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import ru.xander.replicator.schema.Table;
import ru.xander.replicator.schema.TableRowExtractor;

/**
 * @author Alexander Shakhov
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class JsonDumpContainer {

    private Table table;
    @JsonProperty("rows")
    @JsonSerialize(using = JsonRowsSerializer.class)
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
