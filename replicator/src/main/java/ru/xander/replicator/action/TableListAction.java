package ru.xander.replicator.action;

import ru.xander.replicator.filter.Filter;
import ru.xander.replicator.schema.SchemaConfig;
import ru.xander.replicator.schema.SchemaConnection;

import java.util.List;
import java.util.Objects;

/**
 * @author Alexander Shakhov
 */
public class TableListAction implements Action {

    private final SchemaConfig schemaConfig;
    private final List<Filter> filterList;

    public TableListAction(SchemaConfig schemaConfig, List<Filter> filterList) {
        Objects.requireNonNull(schemaConfig, "Configure schema");
        this.schemaConfig = schemaConfig;
        this.filterList = filterList;
    }

    public List<String> execute() {
        try (SchemaConnection schemaConnection = new SchemaConnection(schemaConfig)) {
            return schemaConnection.getSchema().getTables(filterList);
        }
    }
}
