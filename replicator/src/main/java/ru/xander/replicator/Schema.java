package ru.xander.replicator;

import ru.xander.replicator.schema.CheckConstraint;
import ru.xander.replicator.schema.Column;
import ru.xander.replicator.schema.Constraint;
import ru.xander.replicator.schema.Index;
import ru.xander.replicator.schema.ModifyType;
import ru.xander.replicator.schema.PrimaryKey;
import ru.xander.replicator.schema.ImportedKey;
import ru.xander.replicator.schema.Sequence;
import ru.xander.replicator.schema.Table;
import ru.xander.replicator.schema.Trigger;

public interface Schema extends AutoCloseable {

    Table getTable(String tableName);

    void createTable(Table table);

    void dropTable(Table table);

    void createTableComment(Table table);

    void createColumn(Column column);

    void modifyColumn(Column column, ModifyType... modifyTypes);

    void dropColumn(Column column);

    void createColumnComment(Column column);

    void createPrimaryKey(PrimaryKey primaryKey);

    void dropPrimaryKey(PrimaryKey primaryKey);

    void createImportedKey(ImportedKey importedKey);

    void createCheckConstraint(CheckConstraint checkConstraint);

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

}
