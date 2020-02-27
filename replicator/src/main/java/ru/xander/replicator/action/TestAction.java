package ru.xander.replicator.action;

import ru.xander.replicator.schema.Schema;
import ru.xander.replicator.schema.SchemaConfig;
import ru.xander.replicator.schema.SchemaConnectionTest;

import java.util.Objects;

/**
 * @author Alexander Shakhov
 */
public class TestAction implements Action {

    private final SchemaConfig schemaConfig;

    public TestAction(SchemaConfig schemaConfig) {
        Objects.requireNonNull(schemaConfig, "Configure schema");
        this.schemaConfig = schemaConfig;
    }

    public SchemaConnectionTest execute() {
        return withSchemaAndReturn(schemaConfig, Schema::testConnection);
    }
}
