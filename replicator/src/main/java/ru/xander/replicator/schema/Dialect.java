package ru.xander.replicator.schema;

/**
 * @author Alexander Shakhov
 */
public interface Dialect {
    String createTableQuery(Table table);

    String createTableCommentQuery(Table table);

    String dropTableQuery(Table table);

    String createColumnQuery(Column column);

    String modifyColumnQuery(Column column, ColumnDiff... columnDiffs);

    String dropColumnQuery(Column column);

    String createColumnCommentQuery(Column column);

    String createPrimaryKeyQuery(PrimaryKey primaryKey);

    String dropPrimaryKeyQuery(PrimaryKey primaryKey);

    String createImportedKeyQuery(ImportedKey importedKey);

    String createCheckConstraintQuery(CheckConstraint checkConstraint);

    String dropConstraintQuery(Constraint constraint);

    String toggleConstraintQuery(Constraint constraint, boolean enabled);

    String renameConstraintQuery(Constraint constraint, String newConstraintName);

    String createIndexQuery(Index index);

    String dropIndexQuery(Index index);

    String toggleIndexQuery(Index index, boolean enabled);

    String createTriggerQuery(Trigger trigger);

    String dropTriggerQuery(Trigger trigger);

    String toggleTriggerQuery(Trigger trigger, boolean enabled);

    String createSequenceQuery(Sequence sequence);

    String dropSequenceQuery(Sequence sequence);

    String analyzeTableQuery(Table table);

    default String commitQuery() {
        return "COMMIT";
    }

    String selectQuery(Table table);
}
