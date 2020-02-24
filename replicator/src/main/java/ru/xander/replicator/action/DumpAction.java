package ru.xander.replicator.action;

import ru.xander.replicator.dump.DumpOptions;
import ru.xander.replicator.dump.DumpType;
import ru.xander.replicator.dump.SqlTableSerializer;
import ru.xander.replicator.dump.TableSerializer;
import ru.xander.replicator.exception.ReplicatorException;
import ru.xander.replicator.schema.Schema;
import ru.xander.replicator.schema.SchemaConfig;
import ru.xander.replicator.schema.SchemaConnection;
import ru.xander.replicator.schema.Table;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;

/**
 * @author Alexander Shakhov
 */
public class DumpAction implements Action {

    private final SchemaConfig schemaConfig;
    private final DumpType dumpType;
    private final OutputStream output;
    private final DumpOptions options;
    private final String tableName;

    public DumpAction(SchemaConfig schemaConfig, DumpType dumpType, OutputStream output, DumpOptions options, String tableName) {
        Objects.requireNonNull(schemaConfig, "Configure schema");
        Objects.requireNonNull(dumpType, "Choose dump type");
        Objects.requireNonNull(output, "Output stream cannot be null");
        Objects.requireNonNull(options, "Options cannot be null");
        Objects.requireNonNull(tableName, "Table name for dump");
        this.schemaConfig = schemaConfig;
        this.dumpType = dumpType;
        this.output = output;
        this.options = options;
        this.tableName = tableName;
    }

    public void execute() {
        try (SchemaConnection schemaConnection = new SchemaConnection(schemaConfig)) {
            dumpTable(schemaConnection.getSchema());
        }
    }

    private void dumpTable(Schema schema) {
        Table table = schema.getTable(tableName);
        if (table == null) {
            throw new ReplicatorException("Table " + tableName + " not found");
        }
        TableSerializer tableSerializer;
        switch (dumpType) {
            case SQL:
                tableSerializer = new SqlTableSerializer(schema, options);
                break;
            case JSON:
            case XML:
            default:
                throw new ReplicatorException("Unsupported dump type <" + dumpType + ">");
        }
        try {
            serializeTable(table, tableSerializer);
        } catch (Exception e) {
            String errorMessage = "Failed to dump table " + tableName + ": " + e.getMessage();
            throw new ReplicatorException(errorMessage, e);
        }
    }

    private void serializeTable(Table table, TableSerializer tableSerializer) throws IOException {
        if (options.isDumpDdl()) {
            tableSerializer.serializeTable(table, output);
            if (options.isDumpDml()) {
                output.write('\n');
                tableSerializer.serializeRows(table, output);
            }
            tableSerializer.serializeTableObjects(table, output);
            tableSerializer.serializeAnalyze(table, output);
        } else if (options.isDumpDml()) {
            tableSerializer.serializeRows(table, output);
            tableSerializer.serializeAnalyze(table, output);
        }
    }
}
