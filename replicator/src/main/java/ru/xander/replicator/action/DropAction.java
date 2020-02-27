package ru.xander.replicator.action;

import ru.xander.replicator.exception.ReplicatorException;
import ru.xander.replicator.schema.PrimaryKey;
import ru.xander.replicator.schema.Schema;
import ru.xander.replicator.schema.SchemaConfig;
import ru.xander.replicator.schema.Sequence;
import ru.xander.replicator.schema.Table;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * @author Alexander Shakhov
 */
public class DropAction implements Action {

    private final SchemaConfig schemaConfig;
    private final boolean dropExported;
    private final String[] tables;

    public DropAction(SchemaConfig schemaConfig, boolean dropExported, String[] tables) {
        Objects.requireNonNull(schemaConfig, "Configure schema");
        Objects.requireNonNull(tables, "Tables for drop");
        if (tables.length == 0) {
            throw new IllegalArgumentException("At least one table must be specified for drop");
        }
        this.schemaConfig = schemaConfig;
        this.dropExported = dropExported;
        this.tables = tables;
    }

    public void execute() {
        withSchema(schemaConfig, schema -> {
            for (String tableName : tables) {
                Set<String> droppedTables = new HashSet<>();
                dropTable(tableName, schema, droppedTables);
            }
        });
    }

    private void dropTable(String tableName, Schema schema, Set<String> droppedTables) {
        if (droppedTables.contains(tableName)) {
            return;
        }
        droppedTables.add(tableName);

        Table table = schema.getTable(tableName);
        if (table == null) {
            throw new ReplicatorException("Table " + tableName + " not found");
        }

        table.getExportedKeys().forEach(exportedKey -> {
            if (dropExported) {
                dropTable(exportedKey.getFkTableName(), schema, droppedTables);
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
        table.getCheckConstraints().forEach(schema::dropConstraint);
        table.getIndices().forEach(schema::dropIndex);
        table.getTriggers().forEach(schema::dropTrigger);
        Sequence sequence = table.getSequence();
        if (sequence != null) {
            schema.dropSequence(sequence);
        }
        schema.dropTable(table);
    }
}
