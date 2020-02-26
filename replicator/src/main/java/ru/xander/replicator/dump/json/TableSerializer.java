package ru.xander.replicator.dump.json;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import ru.xander.replicator.schema.CheckConstraint;
import ru.xander.replicator.schema.Column;
import ru.xander.replicator.schema.ImportedKey;
import ru.xander.replicator.schema.Index;
import ru.xander.replicator.schema.PrimaryKey;
import ru.xander.replicator.schema.Sequence;
import ru.xander.replicator.schema.Table;
import ru.xander.replicator.schema.Trigger;
import ru.xander.replicator.util.StringUtils;

import java.io.IOException;
import java.util.Collection;

/**
 * @author Alexander Shakhov
 */
public class TableSerializer extends JsonSerializer<Table> {
    @Override
    public void serialize(Table table, JsonGenerator gen, SerializerProvider serializers) throws IOException {
        gen.writeStartObject();

        gen.writeStringField("schema", table.getSchema());
        gen.writeStringField("name", table.getName());
        gen.writeStringField("comment", table.getComment());

        writeColumns(gen, table.getColumns());
        writePrimaryKey(gen, table.getPrimaryKey());
        writeImportedKeys(gen, table.getImportedKeys());
        writeCheckConstraints(gen, table.getCheckConstraints());
        writeIndices(gen, table.getIndices());
        writeSequence(gen, table.getSequence());
        writeTriggers(gen, table.getTriggers());

        gen.writeEndObject();
    }

    private void writeColumns(JsonGenerator gen, Collection<Column> columns) throws IOException {
        gen.writeArrayFieldStart("columns");
        for (Column column : columns) {
            gen.writeStartObject();
            gen.writeNumberField("number", column.getNumber());
            gen.writeStringField("name", column.getName());
            gen.writeObjectField("type", column.getColumnType());
            gen.writeNumberField("size", column.getSize());
            gen.writeNumberField("scale", column.getScale());
            gen.writeBooleanField("nullable", column.isNullable());
            gen.writeStringField("default", column.getDefaultValue());
            gen.writeStringField("comment", column.getComment());
            gen.writeEndObject();
        }
        gen.writeEndArray();
    }

    private void writePrimaryKey(JsonGenerator gen, PrimaryKey primaryKey) throws IOException {
        if (primaryKey == null) {
            return;
        }
        gen.writeObjectFieldStart("primaryKey");
        gen.writeStringField("name", primaryKey.getName());
        gen.writeStringField("columns", StringUtils.joinColumns(primaryKey.getColumns()));
        gen.writeBooleanField("enabled", primaryKey.getEnabled());
        gen.writeEndObject();
    }

    private void writeImportedKeys(JsonGenerator gen, Collection<ImportedKey> importedKeys) throws IOException {
        if (importedKeys.isEmpty()) {
            return;
        }
        gen.writeArrayFieldStart("importedKeys");
        for (ImportedKey importedKey : importedKeys) {
            gen.writeStartObject();
            gen.writeStringField("name", importedKey.getName());
            gen.writeStringField("columns", StringUtils.joinColumns(importedKey.getColumns()));
            gen.writeBooleanField("enabled", importedKey.getEnabled());
            gen.writeStringField("pkName", importedKey.getPkName());
            gen.writeStringField("pkTableSchema", importedKey.getPkTableSchema());
            gen.writeStringField("pkTableName", importedKey.getPkTableName());
            gen.writeStringField("pkColumns", StringUtils.joinColumns(importedKey.getPkColumns()));
            gen.writeEndObject();
        }
        gen.writeEndArray();
    }

    private void writeCheckConstraints(JsonGenerator gen, Collection<CheckConstraint> checkConstraints) throws IOException {
        if (checkConstraints.isEmpty()) {
            return;
        }
        gen.writeArrayFieldStart("checkConstraints");
        for (CheckConstraint checkConstraint : checkConstraints) {
            gen.writeStartObject();
            gen.writeStringField("name", checkConstraint.getName());
            gen.writeStringField("columns", StringUtils.joinColumns(checkConstraint.getColumns()));
            gen.writeBooleanField("enabled", checkConstraint.getEnabled());
            gen.writeStringField("condition", checkConstraint.getCondition());
            gen.writeEndObject();
        }
        gen.writeEndArray();
    }

    private void writeIndices(JsonGenerator gen, Collection<Index> indices) throws IOException {
        if (indices.isEmpty()) {
            return;
        }
        gen.writeArrayFieldStart("indices");
        for (Index index : indices) {
            gen.writeStartObject();
            gen.writeStringField("name", index.getName());
            gen.writeObjectField("type", index.getType());
            gen.writeStringField("columns", StringUtils.joinColumns(index.getColumns()));
            gen.writeBooleanField("enabled", index.getEnabled());
            gen.writeEndObject();
        }
        gen.writeEndArray();
    }

    private void writeSequence(JsonGenerator gen, Sequence sequence) throws IOException {
        if (sequence == null) {
            return;
        }
        gen.writeObjectFieldStart("sequence");
        gen.writeStringField("schema", sequence.getSchema());
        gen.writeStringField("name", sequence.getName());
        gen.writeStringField("startWith", String.valueOf(sequence.getStartWith()));
        gen.writeStringField("incrementBy", String.valueOf(sequence.getIncrementBy()));
        gen.writeStringField("minValue", String.valueOf(sequence.getMinValue()));
        gen.writeStringField("maxValue", String.valueOf(sequence.getMaxValue()));
        gen.writeStringField("cacheSize", String.valueOf(sequence.getCacheSize()));
        gen.writeBooleanField("cycle", sequence.getCycle());
        gen.writeEndObject();
    }

    private void writeTriggers(JsonGenerator gen, Collection<Trigger> triggers) throws IOException {
        if (triggers.isEmpty()) {
            return;
        }
        gen.writeArrayFieldStart("triggers");
        for (Trigger trigger : triggers) {
            gen.writeStartObject();
            gen.writeStringField("name", trigger.getName());
            gen.writeObjectField("vendorType", trigger.getVendorType());
            gen.writeBooleanField("enabled", trigger.getEnabled());
            gen.writeStringField("body", trigger.getBody());
            gen.writeEndObject();
        }
        gen.writeEndArray();
    }
}
