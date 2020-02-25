package ru.xander.replicator.schema;

public abstract class AbstractDialect implements Dialect {

    protected final String workSchema;

    protected AbstractDialect(String workSchema) {
        this.workSchema = workSchema;
    }

    protected String getQualifiedName(Table table) {
        return workSchema + '.' + table.getName();
    }

    protected String getQualifiedName(Index index) {
        return workSchema + '.' + index.getName();
    }

    protected String getQualifiedName(Trigger trigger) {
        return workSchema + '.' + trigger.getName();
    }

    protected String getQualifiedName(Sequence sequence) {
        return workSchema + '.' + sequence.getName();
    }
}
