package ru.xander.replicator;

import ru.xander.replicator.exception.ReplicatorException;
import ru.xander.replicator.listener.Alter;
import ru.xander.replicator.listener.DummyListener;
import ru.xander.replicator.listener.ReplicatorListener;
import ru.xander.replicator.schema.CheckConstraint;
import ru.xander.replicator.schema.Column;
import ru.xander.replicator.schema.Ddl;
import ru.xander.replicator.schema.ImportedKey;
import ru.xander.replicator.schema.Index;
import ru.xander.replicator.schema.ModifyType;
import ru.xander.replicator.schema.PrimaryKey;
import ru.xander.replicator.schema.Sequence;
import ru.xander.replicator.schema.Table;
import ru.xander.replicator.schema.Trigger;
import ru.xander.replicator.util.StringUtils;

import java.io.OutputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static ru.xander.replicator.listener.AlterType.*;

public class Replicator {

    private final Schema source;
    private final Schema target;
    private final ReplicatorListener listener;
    private final Set<String> createdTables;
    private final Set<String> droppedTables;

    public Replicator(Schema source, Schema target) {
        this(source, target, DummyListener.getInstance());
    }

    public Replicator(Schema source, Schema target, ReplicatorListener listener) {
        Objects.requireNonNull(source);
        Objects.requireNonNull(target);
        this.source = source;
        this.target = target;
        this.listener = listener == null ? DummyListener.getInstance() : listener;
        this.createdTables = new HashSet<>();
        this.droppedTables = new HashSet<>();
    }

    /**
     * Создает или обновляет таблицу на схеме-приемнике.
     *
     * @param tableName    Имя таблицы
     * @param withExported Выполнить метод для всех зависимых таблиц
     */
    public void replicate(String tableName, boolean withExported) {
        replicateTable(tableName, withExported);
    }

    public void drop(String tableName) {
        dropTable(tableName);
    }

    public void dump(String tableName, OutputStream output) {
        dumpTable(tableName, output);
    }

    private void replicateTable(String tableName, boolean withExported) {
        Table sourceTable = source.getTable(tableName);
        if (sourceTable == null) {
            listener.warning("Table " + tableName + " not found on source");
            return;
        }
        Table targetTable = target.getTable(tableName);
        if (targetTable == null) {
            createTable(sourceTable, withExported);
        } else {
            updateTable(targetTable, sourceTable, withExported);
        }
    }

    private void createTable(Table table, boolean withExported) {
        if (createdTables.contains(table.getName())) {
            return;
        }
        createdTables.add(table.getName());
        droppedTables.remove(table.getName());

        replicateImportTables(table);

        listener.alter(new Alter(CREATE_TABLE, table.getName()));
        target.createTable(table);
        if (!StringUtils.isEmpty(table.getComment())) {
            listener.alter(new Alter(CREATE_TABLE_COMMENT, table.getName()));
            target.createTableComment(table);
        }
        table.getColumns().forEach(column -> {
            listener.alter(new Alter(CREATE_COLUMN_COMMENT, column.getName()));
            target.createColumnComment(column);
        });
        PrimaryKey primaryKey = table.getPrimaryKey();
        if (primaryKey != null) {
            listener.alter(new Alter(CREATE_PRIMARY_KEY, primaryKey.getName()));
            target.createPrimaryKey(primaryKey);
        }
        table.getImportedKeys().forEach(importedKey -> {
            listener.alter(new Alter(CREATE_IMPORTED_KEY, importedKey.getName()));
            target.createImportedKey(importedKey);
        });
        table.getCheckConstraints().forEach(checkConstraint -> {
            listener.alter(new Alter(CREATE_CHECK_CONSTRAINT, checkConstraint.getName()));
            target.createCheckConstraint(checkConstraint);
        });
        table.getIndices().forEach(index -> {
            listener.alter(new Alter(CREATE_INDEX, index.getName()));
            target.createIndex(index);
        });
        // Триггеры создаем только на идентичных схемах
        if (source.getVendorType() == target.getVendorType()) {
            table.getTriggers().forEach(trigger -> {
                listener.alter(new Alter(CREATE_TRIGGER, trigger.getName()));
                target.createTrigger(trigger);
            });
        }
        Sequence sequence = table.getSequence();
        if (sequence != null) {
            listener.alter(new Alter(CREATE_SEQUENCE, sequence.getName()));
            target.createSequence(sequence);
        }
        listener.alter(new Alter(ANALYZE_TABLE, table.getName()));
        target.analyzeTable(table);

        if (withExported) {
            replicateExportTables(table);
        }
    }

    private void replicateImportTables(Table table) {
        table.getImportedKeys().forEach(importedKey -> replicateTable(importedKey.getPkTableName(), false));
    }

    private void replicateExportTables(Table table) {
        table.getExportedKeys().forEach(exportedKey -> {
            // бывает, таблица ссылается сама на себя. в этом случае не надо её снова создавать
            if (!Objects.equals(table.getName(), exportedKey.getFkTableName())) {
                replicateTable(exportedKey.getFkTableName(), false);
            }
        });
    }

    private void updateTable(Table targetTable, Table sourceTable, boolean withExported) {
        createdTables.add(targetTable.getName());
        droppedTables.remove(targetTable.getName());

        replicateImportTables(sourceTable);

        updateColumns(targetTable, sourceTable);
        updatePrimaryKey(targetTable, sourceTable);
        updateImportedKeys(targetTable, sourceTable);
        updateCheckConstraints(targetTable, sourceTable);
        updateIndexes(targetTable, sourceTable);
        // Триггеры обновляем только на идентичных схемах
        if (source.getVendorType() == target.getVendorType()) {
            updateTriggers(targetTable, sourceTable);
        }
        updateSequence(targetTable, sourceTable);
        updateComments(targetTable, sourceTable);

        if (withExported) {
            replicateExportTables(sourceTable);
        }
    }

    private void updateColumns(Table targetTable, Table sourceTable) {
        Map<String, Column> sourceColumns = sourceTable.getColumnMap();
        Map<String, Column> targetColumns = targetTable.getColumnMap();
        targetColumns.forEach((columnName, targetColumn) -> {
            if (!sourceColumns.containsKey(columnName)) {
                listener.alter(new Alter(DROP_COLUMN, columnName));
                target.dropColumn(targetColumn);
            }
        });
        sourceColumns.forEach((columnName, sourceColumn) -> {
            Column targetColumn = targetColumns.get(columnName);
            if (targetColumn == null) {
                listener.alter(new Alter(CREATE_COLUMN, columnName));
                target.createColumn(sourceColumn);
            } else {
                ModifyType[] modifyTypes = compareColumn(targetColumn, sourceColumn);
                if (modifyTypes.length > 0) {
                    listener.alter(new Alter(MODIFY_COLUMN, columnName, Arrays.toString(modifyTypes)));
                    target.modifyColumn(sourceColumn, modifyTypes);
                }
            }
        });
    }

    private void updatePrimaryKey(Table targetTable, Table sourceTable) {
        PrimaryKey sourcePrimaryKey = sourceTable.getPrimaryKey();
        PrimaryKey targetPrimaryKey = targetTable.getPrimaryKey();
        if ((targetPrimaryKey == null) && (sourcePrimaryKey == null)) {
            return;
        }
        if (sourcePrimaryKey == null) {
            listener.alter(new Alter(DROP_PRIMARY_KEY, targetPrimaryKey.getName()));
            //TODO: констрейнты удаляются вместе со столбцом
            // т.к. столбцы удаляются первее, поэтому здесь можем словить exception,
            // надо подумать, как сделать по-умному
            suppressException(() -> target.dropPrimaryKey(targetPrimaryKey));
        }
        if (targetPrimaryKey == null) {
            listener.alter(new Alter(CREATE_PRIMARY_KEY, sourcePrimaryKey.getName()));
            target.createPrimaryKey(sourcePrimaryKey);
            return;
        }
        if (Objects.equals(sourcePrimaryKey, targetPrimaryKey)) {
            return;
        }
        // Сюда по идее попадаем, когда у таблицы есть примари-кей, но называется он по-другому
        // Нужно пересоздать его с новым именем
        target.dropPrimaryKey(targetPrimaryKey);
        target.createPrimaryKey(sourcePrimaryKey);
    }

    private void updateImportedKeys(Table targetTable, Table sourceTable) {
        Map<String, ImportedKey> sourceImportedKeys = sourceTable.getImportedKeyMap();
        Map<String, ImportedKey> targetImportedKeys = targetTable.getImportedKeyMap();
        targetImportedKeys.forEach((importedKeyName, targetImportedKey) -> {
            if (!sourceImportedKeys.containsKey(importedKeyName)) {
                listener.alter(new Alter(DROP_CONSTRAINT, importedKeyName));
                //TODO: констрейнты удаляются вместе со столбцом
                // т.к. столбцы удаляются первее, поэтому здесь можем словить exception,
                // надо подумать, как сделать по-умному
                suppressException(() -> target.dropConstraint(targetImportedKey));
            }
        });
        sourceImportedKeys.forEach((importedKeyName, sourceImportedKey) -> {
            if (!targetImportedKeys.containsKey(importedKeyName)) {
                listener.alter(new Alter(CREATE_IMPORTED_KEY, importedKeyName));
                target.createImportedKey(sourceImportedKey);
            }
        });
    }

    private void updateCheckConstraints(Table targetTable, Table sourceTable) {
        Map<String, CheckConstraint> sourceCheckConstraints = sourceTable.getCheckConstraintMap();
        Map<String, CheckConstraint> targetCheckConstraints = targetTable.getCheckConstraintMap();
        targetCheckConstraints.forEach((constraintName, targetCheckConstraint) -> {
            if (!sourceCheckConstraints.containsKey(constraintName)) {
                listener.alter(new Alter(DROP_CONSTRAINT, constraintName));
                //TODO: констрейнты удаляются вместе со столбцом
                // т.к. столбцы удаляются первее, поэтому здесь можем словить exception,
                // надо подумать, как сделать по-умному
                suppressException(() -> target.dropConstraint(targetCheckConstraint));
            }
        });
        sourceCheckConstraints.forEach((constraintName, sourceCheckConstraint) -> {
            if (!targetCheckConstraints.containsKey(constraintName)) {
                listener.alter(new Alter(CREATE_CHECK_CONSTRAINT, constraintName));
                target.createCheckConstraint(sourceCheckConstraint);
            }
        });
    }

    private void updateIndexes(Table targetTable, Table sourceTable) {
        Map<String, Index> sourceIndexes = sourceTable.getIndexMap();
        Map<String, Index> targetIndexes = targetTable.getIndexMap();
        targetIndexes.forEach((indexName, targetIndex) -> {
            if (!sourceIndexes.containsKey(indexName)) {
                listener.alter(new Alter(DROP_INDEX, indexName));
                //TODO: индексы удаляются вместе со столбцом
                // т.к. столбцы удаляются первее, поэтому здесь можем словить exception,
                // надо подумать, как сделать по-умному
                suppressException(() -> target.dropIndex(targetIndex));
            }
        });
        sourceIndexes.forEach((indexName, sourceIndex) -> {
            if (!targetIndexes.containsKey(indexName)) {
                listener.alter(new Alter(CREATE_INDEX, indexName));
                target.createIndex(sourceIndex);
            }
        });
    }

    private void updateTriggers(Table targetTable, Table sourceTable) {
        Map<String, Trigger> sourceTriggers = sourceTable.getTriggerMap();
        Map<String, Trigger> targetTriggers = targetTable.getTriggerMap();
        targetTriggers.forEach((triggerName, targetTrigger) -> {
            if (!sourceTriggers.containsKey(triggerName)) {
                listener.alter(new Alter(DROP_TRIGGER, triggerName));
                target.dropTrigger(targetTrigger);
            }
        });
        sourceTriggers.forEach((triggerName, sourceTrigger) -> {
            Trigger targetTrigger = targetTriggers.get(triggerName);
            if (targetTrigger == null) {
                listener.alter(new Alter(CREATE_TRIGGER, triggerName));
                target.createTrigger(sourceTrigger);
            } else if (!StringUtils.equalsStringIgnoreWhiteSpace(sourceTrigger.getBody(), targetTrigger.getBody())) {
                listener.alter(new Alter(CREATE_TRIGGER, triggerName));
                target.createTrigger(sourceTrigger);
            }
        });
    }

    private void updateSequence(Table targetTable, Table sourceTable) {
        Sequence sourceSequence = sourceTable.getSequence();
        Sequence targetSequence = targetTable.getSequence();
        if ((targetSequence == null) && (sourceSequence == null)) {
            return;
        }
        if (sourceSequence == null) {
            listener.alter(new Alter(DROP_SEQUENCE, targetSequence.getName()));
            target.dropSequence(targetSequence);
            return;
        }
        if (targetTable.getSequence() == null) {
            listener.alter(new Alter(CREATE_SEQUENCE, sourceSequence.getName()));
            target.createSequence(sourceSequence);
            return;
        }
        if (Objects.equals(sourceSequence.getName(), targetSequence.getName())) {
            return;
        }

        // Сюда по идее попадаем, когда сиквенс у таблицы есть, но назван по-другому
        // Нужно пересоздать его с новым именем
        listener.alter(new Alter(DROP_SEQUENCE, targetSequence.getName()));
        target.dropSequence(targetSequence);
        listener.alter(new Alter(CREATE_SEQUENCE, sourceSequence.getName()));
        target.createSequence(sourceSequence);
    }

    private void updateComments(Table targetTable, Table sourceTable) {
        if (!StringUtils.equalsStringIgnoreWhiteSpace(targetTable.getComment(), sourceTable.getComment())) {
            listener.alter(new Alter(CREATE_TABLE_COMMENT, sourceTable.getName()));
            target.createTableComment(sourceTable);
        }
        Map<String, Column> targetColumns = targetTable.getColumnMap();
        sourceTable.getColumns().forEach(sourceColumn -> {
            //TODO: бывает, столбец добавляется в процессе обновления таблицы
            // в это случае columnMap не обновляется, поэтому здесь можем схватить NPE
            // по идее при создании столбца (да и прочих структур) надо как-то обновлять эту инфу в Table
            // пока закостылил простой проверкой на null - что не правильно
            Column targetColumn = targetColumns.get(sourceColumn.getName());
            if (targetColumn != null) {
                if (!StringUtils.equalsStringIgnoreWhiteSpace(sourceColumn.getComment(), targetColumn.getComment())) {
                    listener.alter(new Alter(CREATE_COLUMN_COMMENT, sourceColumn.getName()));
                    target.createColumnComment(sourceColumn);
                }
            }
        });
    }

    private ModifyType[] compareColumn(Column targetColumn, Column sourceColumn) {
        Set<ModifyType> modifyTypes = new HashSet<>();
        if (sourceColumn.getColumnType() != targetColumn.getColumnType()) {
            modifyTypes.add(ModifyType.DATATYPE);
        }
        if (sourceColumn.getSize() != targetColumn.getSize()) {
            modifyTypes.add(ModifyType.DATATYPE);
        }
        if (sourceColumn.getScale() != targetColumn.getScale()) {
            modifyTypes.add(ModifyType.DATATYPE);
        }
        if (!StringUtils.equalsStringIgnoreWhiteSpace(sourceColumn.getDefaultValue(), targetColumn.getDefaultValue())) {
            modifyTypes.add(ModifyType.DEFAULT);
        }
        if (targetColumn.isNullable() != sourceColumn.isNullable()) {
            modifyTypes.add(ModifyType.MANDATORY);
        }
        return modifyTypes.toArray(new ModifyType[0]);
    }

    private void dropTable(String tableName) {
        Table targetTable = target.getTable(tableName);
        if (targetTable == null) {
            listener.warning("Table " + tableName + " not found on target.");
            return;
        }
        dropTable(targetTable);
    }

    private void dropTable(Table table) {
        if (droppedTables.contains(table.getName())) {
            return;
        }
        droppedTables.add(table.getName());
        createdTables.remove(table.getName());

        dropExportTables(table);

        PrimaryKey primaryKey = table.getPrimaryKey();
        if (primaryKey != null) {
            listener.alter(new Alter(DROP_PRIMARY_KEY, primaryKey.getName()));
            target.dropPrimaryKey(primaryKey);
        }
        table.getImportedKeys().forEach(importedKey -> {
            listener.alter(new Alter(DROP_CONSTRAINT, importedKey.getName()));
            target.dropConstraint(importedKey);
        });
        table.getCheckConstraints().forEach(checkConstraint -> {
            listener.alter(new Alter(DROP_CONSTRAINT, checkConstraint.getName()));
            target.dropConstraint(checkConstraint);
        });
        table.getIndices().forEach(index -> {
            listener.alter(new Alter(DROP_INDEX, index.getName()));
            target.dropIndex(index);
        });
        table.getTriggers().forEach(trigger -> {
            listener.alter(new Alter(DROP_TRIGGER, trigger.getName()));
            target.dropTrigger(trigger);
        });
        Sequence sequence = table.getSequence();
        if (sequence != null) {
            listener.alter(new Alter(DROP_SEQUENCE, sequence.getName()));
            target.dropSequence(sequence);
        }

        listener.alter(new Alter(DROP_TABLE, table.getName()));
        target.dropTable(table);
    }

    private void dropExportTables(Table table) {
        table.getExportedKeys().forEach(exportedKey -> {
            // бывает, таблица ссылается сама на себя. в этом случае не надо её снова удалять
            if (!Objects.equals(table.getName(), exportedKey.getFkTableName())) {
                dropTable(exportedKey.getFkTableName());
            }
        });
    }

    private void dumpTable(String tableName, OutputStream output) {
        Table sourceTable = source.getTable(tableName);
        if (sourceTable == null) {
            listener.warning("Table " + tableName + " not found on source");
            return;
        }
        Ddl ddl = source.getDdl(sourceTable);
        try {
            output.write(ddl.getTable().getBytes());
            output.write(';');
            output.write('\n');
            output.write('\n');
            for (String constraint : ddl.getConstraints()) {
                output.write(constraint.getBytes());
                output.write(';');
                output.write('\n');
            }
            output.write('\n');
            for (String index : ddl.getIndices()) {
                output.write(index.getBytes());
                output.write(';');
                output.write('\n');
            }
            if (ddl.getSequence() != null) {
                output.write('\n');
                output.write(ddl.getSequence().getBytes());
                output.write(';');
                output.write('\n');
            }
            output.write('\n');
            for (String trigger : ddl.getTriggers()) {
                output.write(trigger.getBytes());
                output.write('\n');
            }
            output.write('\n');
        } catch (Exception e) {
            String errorMessage = "Failed to dump table " + tableName + ": " + e.getMessage();
            throw new ReplicatorException(errorMessage, e);
        }
    }

    private void suppressException(Runnable runnable) {
        try {
            runnable.run();
        } catch (Exception e) {
            listener.warning(e.getMessage());
        }
    }
}
