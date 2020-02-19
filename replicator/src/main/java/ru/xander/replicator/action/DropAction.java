package ru.xander.replicator.action;

import ru.xander.replicator.exception.ReplicatorException;
import ru.xander.replicator.schema.PrimaryKey;
import ru.xander.replicator.schema.Schema;
import ru.xander.replicator.schema.SchemaConnection;
import ru.xander.replicator.schema.Sequence;
import ru.xander.replicator.schema.Table;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * @author Alexander Shakhov
 */
public class DropAction {

    public void execute(String tableName, DropConfig config) {
        Objects.requireNonNull(tableName, "tableName");
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(config.getSchemaConfig(), "Configure schema");
        try (SchemaConnection schema = new SchemaConnection(config.getSchemaConfig())) {
            Set<String> droppedTables = new HashSet<>();
            dropTable(tableName, schema.getSchema(), droppedTables, config);
        }
    }

    private void dropTable(String tableName, Schema schema, Set<String> droppedTables, DropConfig config) {
        if (droppedTables.contains(tableName)) {
            return;
        }
        droppedTables.add(tableName);

        Table table = schema.getTable(tableName);
        if (table == null) {
            throw new ReplicatorException("Table " + tableName + " not found");
        }

        table.getExportedKeys().forEach(exportedKey -> {
            if (config.isDropExported()) {
                dropTable(exportedKey.getFkTableName(), schema, droppedTables, config);
            } else {
                schema.dropConstraint(exportedKey);
            }
        });

        dropTable(table, schema);
    }

    private void dropTable(Table table, Schema schema) {
        PrimaryKey primaryKey = table.getPrimaryKey();
        if (primaryKey != null) {
            schema.dropPrimaryKey(primaryKey);
        }
        table.getImportedKeys().forEach(schema::dropConstraint);
        table.getIndices().forEach(schema::dropIndex);
        table.getTriggers().forEach(schema::dropTrigger);
        Sequence sequence = table.getSequence();
        if (sequence != null) {
            schema.dropSequence(sequence);
        }
        schema.dropTable(table);
    }
}
