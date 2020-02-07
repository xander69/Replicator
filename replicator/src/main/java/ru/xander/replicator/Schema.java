package ru.xander.replicator;

import ru.xander.replicator.schema.BatchExecutor;
import ru.xander.replicator.schema.Column;
import ru.xander.replicator.schema.Constraint;
import ru.xander.replicator.schema.Ddl;
import ru.xander.replicator.schema.Dml;
import ru.xander.replicator.schema.ImportedKey;
import ru.xander.replicator.schema.Index;
import ru.xander.replicator.schema.PrimaryKey;
import ru.xander.replicator.schema.Sequence;
import ru.xander.replicator.schema.Table;
import ru.xander.replicator.schema.Trigger;
import ru.xander.replicator.schema.VendorType;

/**
 * @author Alexander Shakhov
 */
public interface Schema {

    VendorType getVendorType();

    Table getTable(String tableName);

    void createTable(Table table);

    void dropTable(Table table);

    void createTableComment(Table table);

    void createColumn(Column column);

    void modifyColumn(Column oldColumn, Column newColumn);

    void dropColumn(Column column);

    void createColumnComment(Column column);

    void createPrimaryKey(PrimaryKey primaryKey);

    void dropPrimaryKey(PrimaryKey primaryKey);

    void createImportedKey(ImportedKey importedKey);

    void dropConstraint(Constraint constraint);

    void toggleConstraint(Constraint constraint, boolean enabled);

    void createIndex(Index index);

    void dropIndex(Index index);

    void toggleIndex(Index index, boolean enabled);

    void createTrigger(Trigger trigger);

    void dropTrigger(Trigger trigger);

    void toggleTrigger(Trigger trigger, boolean enabled);

    void createSequence(Sequence sequence);

    void dropSequence(Sequence sequence);

    void analyzeTable(Table table);

    Ddl getDdl(Table table);

    Dml getDml(Table table);

    BatchExecutor createBatchExecutor();

}
