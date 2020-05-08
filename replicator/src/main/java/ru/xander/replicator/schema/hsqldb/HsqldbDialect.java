package ru.xander.replicator.schema.hsqldb;

import ru.xander.replicator.schema.AbstractDialect;
import ru.xander.replicator.schema.CheckConstraint;
import ru.xander.replicator.schema.Column;
import ru.xander.replicator.schema.ColumnDiff;
import ru.xander.replicator.schema.Constraint;
import ru.xander.replicator.schema.ImportedKey;
import ru.xander.replicator.schema.Index;
import ru.xander.replicator.schema.PrimaryKey;
import ru.xander.replicator.schema.Sequence;
import ru.xander.replicator.schema.Table;
import ru.xander.replicator.schema.Trigger;

import java.util.stream.Collectors;

/**
 * @author Alexander Shakhov
 */
public class HsqldbDialect extends AbstractDialect {

    HsqldbDialect(String workSchema) {
        super(workSchema);
    }

    @Override
    public String testQuery() {
        return "SELECT 1 FROM INFORMATION_SCHEMA.SYSTEM_USERS";
    }

    @Override
    public String createTableQuery(Table table) {
        return "CREATE TABLE " + getQualifiedName(table) + '\n' +
                "(\n    " + table
                .getColumns()
                .stream()
                .map(HsqldbDialect::getColumnDefinition)
                .collect(Collectors.joining(",\n    ")) +
                "\n)";
    }

    @Override
    public String createTableCommentQuery(Table table) {
        return null;
    }

    @Override
    public String dropTableQuery(Table table) {
        return null;
    }

    @Override
    public String renameTableQuery(Table table, String newName) {
        return null;
    }

    @Override
    public String createColumnQuery(Column column) {
        return null;
    }

    @Override
    public String modifyColumnQuery(Column column, ColumnDiff... columnDiffs) {
        return null;
    }

    @Override
    public String dropColumnQuery(Column column) {
        return null;
    }

    @Override
    public String renameColumnQuery(Column column, String newName) {
        return null;
    }

    @Override
    public String createColumnCommentQuery(Column column) {
        return null;
    }

    @Override
    public String createPrimaryKeyQuery(PrimaryKey primaryKey) {
        return null;
    }

    @Override
    public String dropPrimaryKeyQuery(PrimaryKey primaryKey) {
        return null;
    }

    @Override
    public String createImportedKeyQuery(ImportedKey importedKey) {
        return null;
    }

    @Override
    public String createCheckConstraintQuery(CheckConstraint checkConstraint) {
        return null;
    }

    @Override
    public String dropConstraintQuery(Constraint constraint) {
        return null;
    }

    @Override
    public String toggleConstraintQuery(Constraint constraint, boolean enabled) {
        return null;
    }

    @Override
    public String renameConstraintQuery(Constraint constraint, String newConstraintName) {
        return null;
    }

    @Override
    public String createIndexQuery(Index index) {
        return null;
    }

    @Override
    public String dropIndexQuery(Index index) {
        return null;
    }

    @Override
    public String toggleIndexQuery(Index index, boolean enabled) {
        return null;
    }

    @Override
    public String createTriggerQuery(Trigger trigger) {
        return null;
    }

    @Override
    public String dropTriggerQuery(Trigger trigger) {
        return null;
    }

    @Override
    public String toggleTriggerQuery(Trigger trigger, boolean enabled) {
        return null;
    }

    @Override
    public String createSequenceQuery(Sequence sequence) {
        return null;
    }

    @Override
    public String dropSequenceQuery(Sequence sequence) {
        return null;
    }

    @Override
    public String analyzeTableQuery(Table table) {
        return null;
    }

    @Override
    public String selectQuery(Table table) {
        return null;
    }

    private static String getColumnDefinition(Column column) {
        StringBuilder definition = new StringBuilder();
        definition.append(column.getName()).append(' ').append(getDataType(column));
        if (column.getDefaultValue() != null) {
            definition.append(" DEFAULT ").append(column.getDefaultValue().trim());
        }
        if (!column.isNullable()) {
            definition.append(" NOT NULL");
        }
        return definition.toString();
    }

    private static String getDataType(Column column) {
        String dataType = column.getColumnType().toHsqldb();
        switch (column.getColumnType()) {
            case FLOAT:
                if (column.getSize() == 0) {
                    return dataType;
                }
                return dataType + "(" + column.getSize() + ", " + column.getScale() + ")";
            case CHAR:
            case STRING:
                return dataType + "(" + column.getSize() + ")";
            default:
                return dataType;
        }
    }
}
