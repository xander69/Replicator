package ru.xander.replicator;

import ru.xander.replicator.compare.CompareDiff;
import ru.xander.replicator.compare.CompareKind;
import ru.xander.replicator.compare.CompareResult;
import ru.xander.replicator.compare.CompareResultType;
import ru.xander.replicator.exception.ReplicatorException;
import ru.xander.replicator.schema.BatchExecutor;
import ru.xander.replicator.schema.CheckConstraint;
import ru.xander.replicator.schema.Column;
import ru.xander.replicator.schema.Ddl;
import ru.xander.replicator.schema.Dml;
import ru.xander.replicator.schema.ImportedKey;
import ru.xander.replicator.schema.Index;
import ru.xander.replicator.schema.PrimaryKey;
import ru.xander.replicator.schema.Sequence;
import ru.xander.replicator.schema.Table;
import ru.xander.replicator.schema.Trigger;
import ru.xander.replicator.util.StringUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author Alexander Shakhov
 */
public class Replicator {

    /**
     * Выполняет репликацию таблицы со схемы источника, на схему приемника.
     */
    public void replicate(String tableName, ReplicateConfig config) {
        try (
                SchemaConnection source = new SchemaConnection(config.getSourceConfig(), config.getListener());
                SchemaConnection target = new SchemaConnection(config.getTargetConfig(), config.getListener())
        ) {
            Set<String> createdTables = new HashSet<>();
            replicateTable(tableName, source.getSchema(), target.getSchema(), createdTables);
        }
    }

    public void drop(String tableName) {
        //TODO:
//        dropTable(tableName);
    }

    public CompareResult compare(String tableName) {
        //TODO:
//        return compareTable(tableName);
        return null;
    }

    public void dump(String tableName, OutputStream output, DumpOptions dumpOptions) {
        //TODO:
//        dumpTable(tableName, output, dumpOptions);
    }

    public void pump(File scriptFile) {
        //TODO:
//        pumpScript(scriptFile);
    }

    private void replicateTable(String tableName, Schema source, Schema target, Set<String> createdTables) {
        if (createdTables.contains(tableName)) {
            return;
        }

        createdTables.add(tableName);

        Table sourceTable = source.getTable(tableName);
        if (sourceTable == null) {
            throw new ReplicatorException("Table " + tableName + " not found on source");
        }

        sourceTable.getImportedKeys().forEach(importedKey -> {
            String pkTableName = importedKey.getPkTableName();
            replicateTable(pkTableName, source, target, createdTables);
        });

        Table targetTable = target.getTable(tableName);
        if (targetTable == null) {
            createTable(target, sourceTable);
        } else {
            updateTable(target, targetTable, sourceTable);
        }
    }

    private void createTable(Schema target, Table table) {
        target.createTable(table);
        if (!StringUtils.isEmpty(table.getComment())) {
            target.createTableComment(table);
        }
        table.getColumns().forEach(target::createColumnComment);
        PrimaryKey primaryKey = table.getPrimaryKey();
        if (primaryKey != null) {
            target.createPrimaryKey(primaryKey);
        }
        table.getImportedKeys().forEach(target::createImportedKey);
        table.getCheckConstraints().forEach(target::createCheckConstraint);
        table.getIndices().forEach(target::createIndex);
        // TODO: триггеры и сиквенсы спрятать в схему. ввести тип данных Serial
        // Триггеры создаем только на идентичных схемах
//        if (source.getVendorType() == target.getVendorType()) {
//            table.getTriggers().forEach(target::createTrigger);
//        }
//        Sequence sequence = table.getSequence();
//        if (sequence != null) {
//            target.createSequence(sequence);
//        }
        target.analyzeTable(table);
    }

    private void updateTable(Schema target, Table targetTable, Table sourceTable) {
        updateColumns(target, targetTable, sourceTable);
        updatePrimaryKey(target, targetTable, sourceTable);
        updateImportedKeys(target, targetTable, sourceTable);
        updateCheckConstraints(target, targetTable, sourceTable);
        updateIndices(target, targetTable, sourceTable);
        updateComments(target, targetTable, sourceTable);
    }

    private void updateColumns(Schema target, Table targetTable, Table sourceTable) {
        Map<String, Column> sourceColumns = sourceTable.getColumnMap();
        Map<String, Column> targetColumns = targetTable.getColumnMap();

        List<Column> columnsToDrop = targetColumns.values().stream()
                .filter(column -> !sourceColumns.containsKey(column.getName()))
                .collect(Collectors.toList());
        columnsToDrop.forEach(target::dropColumn);
        columnsToDrop.forEach(targetTable::removeColumn);

        sourceColumns.forEach((columnName, sourceColumn) -> {
            Column targetColumn = targetColumns.get(columnName);
            if (targetColumn == null) {
                target.createColumn(sourceColumn);
            } else {
                target.modifyColumn(targetColumn, sourceColumn);
            }
            targetTable.addColumn(sourceColumn);
        });
    }

    private void updatePrimaryKey(Schema target, Table targetTable, Table sourceTable) {
        PrimaryKey sourcePrimaryKey = sourceTable.getPrimaryKey();
        PrimaryKey targetPrimaryKey = targetTable.getPrimaryKey();
        if ((targetPrimaryKey == null) && (sourcePrimaryKey == null)) {
            return;
        }
        if (sourcePrimaryKey == null) {
            target.dropPrimaryKey(targetPrimaryKey);
            targetTable.setPrimaryKey(null);
            return;
        }
        if (targetPrimaryKey == null) {
            target.createPrimaryKey(sourcePrimaryKey);
            targetTable.setPrimaryKey(sourcePrimaryKey);
            return;
        }
        if (Objects.equals(sourcePrimaryKey.getName(), targetPrimaryKey.getName())) {
            return;
        }
        // Сюда по идее попадаем, когда у таблицы есть примари-кей, но называется он по-другому
        // Нужно пересоздать его с новым именем
        target.dropPrimaryKey(targetPrimaryKey);
        target.createPrimaryKey(sourcePrimaryKey);
        targetTable.setPrimaryKey(sourcePrimaryKey);
    }

    private void updateImportedKeys(Schema target, Table targetTable, Table sourceTable) {
        Map<String, ImportedKey> sourceImportedKeys = sourceTable.getImportedKeyMap();
        Map<String, ImportedKey> targetImportedKeys = targetTable.getImportedKeyMap();

        List<ImportedKey> importedKeysToDrop = targetImportedKeys.values().stream()
                .filter(importedKey -> !sourceImportedKeys.containsKey(importedKey.getName()))
                .collect(Collectors.toList());

        importedKeysToDrop.forEach(target::dropConstraint);
        importedKeysToDrop.forEach(targetTable::removeImportedKey);

        sourceImportedKeys.forEach((importedKeyName, sourceImportedKey) -> {
            if (!targetImportedKeys.containsKey(importedKeyName)) {
                target.createImportedKey(sourceImportedKey);
                targetTable.addImportedKey(sourceImportedKey);
            }
        });
    }

    private void updateCheckConstraints(Schema target, Table targetTable, Table sourceTable) {
        Map<String, CheckConstraint> sourceCheckConstraints = sourceTable.getCheckConstraintMap();
        Map<String, CheckConstraint> targetCheckConstraints = targetTable.getCheckConstraintMap();

        List<CheckConstraint> checkConstraintsToDrop = targetCheckConstraints.values().stream()
                .filter(checkConstraint -> !sourceCheckConstraints.containsKey(checkConstraint.getName()))
                .collect(Collectors.toList());

        checkConstraintsToDrop.forEach(target::dropConstraint);
        checkConstraintsToDrop.forEach(targetTable::removeCheckConstraint);

        sourceCheckConstraints.forEach((constraintName, sourceCheckConstraint) -> {
            if (!targetCheckConstraints.containsKey(constraintName)) {
                target.createCheckConstraint(sourceCheckConstraint);
                targetTable.addCheckConstraint(sourceCheckConstraint);
            }
        });
    }

    private void updateIndices(Schema target, Table targetTable, Table sourceTable) {
        Map<String, Index> sourceIndexes = sourceTable.getIndexMap();
        Map<String, Index> targetIndexes = targetTable.getIndexMap();

        List<Index> indicesToDrop = targetIndexes.values().stream()
                .filter(index -> !sourceIndexes.containsKey(index.getName()))
                .collect(Collectors.toList());

        indicesToDrop.forEach(target::dropIndex);
        indicesToDrop.forEach(targetTable::removeIndex);

        sourceIndexes.forEach((indexName, sourceIndex) -> {
            if (!targetIndexes.containsKey(indexName)) {
                target.createIndex(sourceIndex);
                targetTable.addIndex(sourceIndex);
            }
        });
    }

//    private void updateTriggers(Table targetTable, Table sourceTable) {
//        Map<String, Trigger> sourceTriggers = sourceTable.getTriggerMap();
//        Map<String, Trigger> targetTriggers = targetTable.getTriggerMap();
//        targetTriggers.forEach((triggerName, targetTrigger) -> {
//            if (!sourceTriggers.containsKey(triggerName)) {
//                listener.alter(new Alter(DROP_TRIGGER, triggerName));
//                target.dropTrigger(targetTrigger);
//            }
//        });
//        sourceTriggers.forEach((triggerName, sourceTrigger) -> {
//            Trigger targetTrigger = targetTriggers.get(triggerName);
//            if (targetTrigger == null) {
//                listener.alter(new Alter(CREATE_TRIGGER, triggerName));
//                target.createTrigger(sourceTrigger);
//            } else if (!StringUtils.equalsStringIgnoreWhiteSpace(sourceTrigger.getBody(), targetTrigger.getBody())) {
//                listener.alter(new Alter(CREATE_TRIGGER, triggerName));
//                target.createTrigger(sourceTrigger);
//            }
//        });
//    }
//
//    private void updateSequence(Table targetTable, Table sourceTable) {
//        Sequence sourceSequence = sourceTable.getSequence();
//        Sequence targetSequence = targetTable.getSequence();
//        if ((targetSequence == null) && (sourceSequence == null)) {
//            return;
//        }
//        if (sourceSequence == null) {
//            listener.alter(new Alter(DROP_SEQUENCE, targetSequence.getName()));
//            target.dropSequence(targetSequence);
//            return;
//        }
//        if (targetTable.getSequence() == null) {
//            listener.alter(new Alter(CREATE_SEQUENCE, sourceSequence.getName()));
//            target.createSequence(sourceSequence);
//            return;
//        }
//        if (Objects.equals(sourceSequence.getName(), targetSequence.getName())) {
//            return;
//        }
//
//        // Сюда по идее попадаем, когда сиквенс у таблицы есть, но назван по-другому
//        // Нужно пересоздать его с новым именем
//        listener.alter(new Alter(DROP_SEQUENCE, targetSequence.getName()));
//        target.dropSequence(targetSequence);
//        listener.alter(new Alter(CREATE_SEQUENCE, sourceSequence.getName()));
//        target.createSequence(sourceSequence);
//    }

    private void updateComments(Schema target, Table targetTable, Table sourceTable) {
        if (!StringUtils.equalsStringIgnoreWhiteSpace(targetTable.getComment(), sourceTable.getComment())) {
            target.createTableComment(sourceTable);
            targetTable.setComment(sourceTable.getComment());
        }
        Map<String, Column> targetColumns = targetTable.getColumnMap();
        sourceTable.getColumns().forEach(sourceColumn -> {
            Column targetColumn = targetColumns.get(sourceColumn.getName());
            if (targetColumn != null) {
                if (!StringUtils.equalsStringIgnoreWhiteSpace(sourceColumn.getComment(), targetColumn.getComment())) {
                    target.createColumnComment(sourceColumn);
                }
            }
        });
    }

    private void dropTable(Schema target, String tableName) {
        Table targetTable = target.getTable(tableName);
        if (targetTable == null) {
            throw new ReplicatorException("Table " + tableName + " not found on target.");
        }
        dropTable(target, targetTable);
    }

    private void dropTable(Schema target, Table table) {
//        if (droppedTables.contains(table.getName())) {
//            return;
//        }
//        droppedTables.add(table.getName());
//        replicatedTables.remove(table.getName());

        dropExportTables(target, table);

        PrimaryKey primaryKey = table.getPrimaryKey();
        if (primaryKey != null) {
//            listener.alter(new Alter(DROP_PRIMARY_KEY, primaryKey.getName()));
            target.dropPrimaryKey(primaryKey);
        }
        table.getImportedKeys().forEach(importedKey -> {
//            listener.alter(new Alter(DROP_CONSTRAINT, importedKey.getName()));
            target.dropConstraint(importedKey);
        });
        table.getCheckConstraints().forEach(checkConstraint -> {
//            listener.alter(new Alter(DROP_CONSTRAINT, checkConstraint.getName()));
            target.dropConstraint(checkConstraint);
        });
        table.getIndices().forEach(index -> {
//            listener.alter(new Alter(DROP_INDEX, index.getName()));
            target.dropIndex(index);
        });
        table.getTriggers().forEach(trigger -> {
//            listener.alter(new Alter(DROP_TRIGGER, trigger.getName()));
            target.dropTrigger(trigger);
        });
        Sequence sequence = table.getSequence();
        if (sequence != null) {
//            listener.alter(new Alter(DROP_SEQUENCE, sequence.getName()));
            target.dropSequence(sequence);
        }

//        listener.alter(new Alter(DROP_TABLE, table.getName()));
        target.dropTable(table);
    }

    private void dropExportTables(Schema target, Table table) {
        table.getExportedKeys().forEach(exportedKey -> {
            // бывает, таблица ссылается сама на себя. в этом случае не надо её снова удалять
            if (!Objects.equals(table.getName(), exportedKey.getFkTableName())) {
                dropTable(target, exportedKey.getFkTableName());
            }
        });
    }

    private CompareResult compareTable(Schema target, Schema source, String tableName) {
        Table sourceTable = source.getTable(tableName);
        if (sourceTable == null) {
            return new CompareResult(CompareResultType.ABSENT_ON_SOURCE, Collections.emptyList());
        }
        Table targetTable = target.getTable(tableName);
        if (targetTable == null) {
            return new CompareResult(CompareResultType.ABSENT_ON_TARGET, Collections.emptyList());
        }
        List<CompareDiff> diffs = new LinkedList<>();
        compareTableComment(sourceTable, targetTable, diffs);
        compareColumns(sourceTable, targetTable, diffs);
        comparePrimaryKey(sourceTable, targetTable, diffs);
        compareImportedKeys(sourceTable, targetTable, diffs);
        compareCheckConstraints(sourceTable, targetTable, diffs);
        compareIndexes(sourceTable, targetTable, diffs);
        compareTriggers(sourceTable, targetTable, diffs);
        compareSequence(sourceTable, targetTable, diffs);
        if (diffs.isEmpty()) {
            return new CompareResult(CompareResultType.EQUALS, Collections.emptyList());
        } else {
            return new CompareResult(CompareResultType.DIFFERENT, diffs);
        }
    }

    private void compareTableComment(Table sourceTable, Table targetTable, List<CompareDiff> diffs) {
        if (!StringUtils.equalsStringIgnoreWhiteSpace(sourceTable.getComment(), targetTable.getComment())) {
            diffs.add(new CompareDiff(CompareKind.TABLE_COMMENT, sourceTable.getComment(), targetTable.getComment()));
        }
    }

    private void compareColumns(Table sourceTable, Table targetTable, List<CompareDiff> diffs) {
        Map<String, Column> sourceColumns = sourceTable.getColumnMap();
        Map<String, Column> targetColumns = targetTable.getColumnMap();
        sourceColumns.forEach((columnName, sourceColumn) -> {
            Column targetColumn = targetColumns.get(columnName);
            if (targetColumn == null) {
                diffs.add(new CompareDiff(CompareKind.COLUMN_ABSENT_ON_TARGET, columnName, null));
            } else {
//                ModifyType[] modifyTypes = compareColumn(sourceColumn, targetColumn);
//                for (ModifyType modifyType : modifyTypes) {
//                    switch (modifyType) {
//                        case DATATYPE:
//                            String sourceDatatype = sourceColumn.getColumnType()
//                                    + ", size: " + sourceColumn.getSize()
//                                    + ", scale: " + sourceColumn.getScale();
//                            String targetDatatype = targetColumn.getColumnType()
//                                    + ", size: " + targetColumn.getSize()
//                                    + ", scale: " + targetColumn.getScale();
//                            diffs.add(new CompareDiff(CompareKind.COLUMN_DATATYPE, sourceDatatype, targetDatatype));
//                            break;
//                        case MANDATORY:
//                            diffs.add(new CompareDiff(CompareKind.COLUMN_MANDATORY,
//                                    String.valueOf(sourceColumn.isNullable()),
//                                    String.valueOf(targetColumn.isNullable())));
//                            break;
//                        case DEFAULT:
//                            diffs.add(new CompareDiff(CompareKind.COLUMN_DEFAULT,
//                                    sourceColumn.getDefaultValue(),
//                                    targetColumn.getDefaultValue()));
//                            break;
//                    }
//                }
//                if (!StringUtils.equalsStringIgnoreWhiteSpace(sourceColumn.getComment(), targetColumn.getComment())) {
//                    diffs.add(new CompareDiff(CompareKind.COLUMN_COMMENT, sourceColumn.getComment(), targetColumn.getComment()));
//                }
            }
        });
        targetColumns.forEach((columnName, targetColumn) -> {
            if (!sourceColumns.containsKey(columnName)) {
                diffs.add(new CompareDiff(CompareKind.COLUMN_ABSENT_ON_SOURCE, null, columnName));
            }
        });
    }

    private void comparePrimaryKey(Table sourceTable, Table targetTable, List<CompareDiff> diffs) {
        PrimaryKey sourcePrimaryKey = sourceTable.getPrimaryKey();
        PrimaryKey targetPrimaryKey = targetTable.getPrimaryKey();
        if ((sourcePrimaryKey == null) && (targetPrimaryKey == null)) {
            return;
        }
        if (targetPrimaryKey == null) {
            diffs.add(new CompareDiff(CompareKind.PRIMARY_KEY_ABSENT_ON_TARGET, sourcePrimaryKey.getName(), null));
            return;
        }
        if (sourcePrimaryKey == null) {
            diffs.add(new CompareDiff(CompareKind.PRIMARY_KEY_ABSENT_ON_SOURCE, null, targetPrimaryKey.getName()));
            return;
        }
        if (!Objects.equals(sourcePrimaryKey.getName(), targetPrimaryKey.getName())) {
            diffs.add(new CompareDiff(CompareKind.PRIMARY_KEY_NAME, sourcePrimaryKey.getName(), targetPrimaryKey.getName()));
        }
        if (!Objects.equals(sourcePrimaryKey.getColumnName(), targetPrimaryKey.getColumnName())) {
            diffs.add(new CompareDiff(CompareKind.PRIMARY_KEY_COLUMN, sourcePrimaryKey.getColumnName(), targetPrimaryKey.getColumnName()));
        }
    }

    private void compareImportedKeys(Table sourceTable, Table targetTable, List<CompareDiff> diffs) {
        Map<String, ImportedKey> sourceImportedKeys = sourceTable.getImportedKeyMap();
        Map<String, ImportedKey> targetImportedKeys = targetTable.getImportedKeyMap();
        sourceImportedKeys.forEach((importedKeyName, sourceImportedKey) -> {
            ImportedKey targetImportedKey = targetImportedKeys.get(importedKeyName);
            if (targetImportedKey == null) {
                diffs.add(new CompareDiff(CompareKind.IMPORTED_KEY_ABSENT_ON_TARGET, importedKeyName, null));
            } else {
                if (!Objects.equals(sourceImportedKey.getColumnName(), targetImportedKey.getColumnName())) {
                    diffs.add(new CompareDiff(CompareKind.IMPORTED_KEY_COLUMN,
                            sourceImportedKey.getColumnName(),
                            targetImportedKey.getColumnName()));
                }
                if (!Objects.equals(sourceImportedKey.getPkName(), targetImportedKey.getPkName())) {
                    diffs.add(new CompareDiff(CompareKind.IMPORTED_KEY_PK_NAME,
                            sourceImportedKey.getPkName(),
                            targetImportedKey.getPkName()));
                }
                if (!Objects.equals(sourceImportedKey.getPkTableName(), targetImportedKey.getPkTableName())) {
                    diffs.add(new CompareDiff(CompareKind.IMPORTED_KEY_PK_TABLE,
                            sourceImportedKey.getPkTableName(),
                            targetImportedKey.getPkTableName()));
                }
                if (!Objects.equals(sourceImportedKey.getPkColumnName(), targetImportedKey.getPkColumnName())) {
                    diffs.add(new CompareDiff(CompareKind.IMPORTED_KEY_PK_COLUMN,
                            sourceImportedKey.getPkColumnName(),
                            targetImportedKey.getPkColumnName()));
                }
            }
        });
        targetImportedKeys.forEach((importedKeyName, targetImportedKey) -> {
            if (!sourceImportedKeys.containsKey(importedKeyName)) {
                diffs.add(new CompareDiff(CompareKind.IMPORTED_KEY_ABSENT_ON_SOURCE, null, importedKeyName));
            }
        });
    }

    private void compareCheckConstraints(Table sourceTable, Table targetTable, List<CompareDiff> diffs) {
        Map<String, CheckConstraint> sourceCheckConstraints = sourceTable.getCheckConstraintMap();
        Map<String, CheckConstraint> targetCheckConstraints = targetTable.getCheckConstraintMap();
        sourceCheckConstraints.forEach((checkConstraintName, sourceCheckConstraint) -> {
            CheckConstraint targetCheckConstraint = targetCheckConstraints.get(checkConstraintName);
            if (targetCheckConstraint == null) {
                diffs.add(new CompareDiff(CompareKind.CHECK_CONSTRAINT_ABSENT_ON_TARGET, checkConstraintName, null));
            } else {
                if (!Objects.equals(sourceCheckConstraint.getCondition(), targetCheckConstraint.getCondition())) {
                    diffs.add(new CompareDiff(CompareKind.CHECK_CONSTRAINT_CONDITION,
                            sourceCheckConstraint.getCondition(),
                            targetCheckConstraint.getCondition()));
                }
            }
        });
        targetCheckConstraints.forEach((checkConstraintName, targetCheckConstraint) -> {
            if (!sourceCheckConstraints.containsKey(checkConstraintName)) {
                diffs.add(new CompareDiff(CompareKind.CHECK_CONSTRAINT_ABSENT_ON_SOURCE, null, checkConstraintName));
            }
        });
    }

    private void compareIndexes(Table sourceTable, Table targetTable, List<CompareDiff> diffs) {
        Map<String, Index> sourceIndexes = sourceTable.getIndexMap();
        Map<String, Index> targetIndexes = targetTable.getIndexMap();
        sourceIndexes.forEach((indexName, sourceIndex) -> {
            Index targetIndex = targetIndexes.get(indexName);
            if (targetIndex == null) {
                diffs.add(new CompareDiff(CompareKind.INDEX_ABSENT_ON_TARGET, indexName, null));
            } else {
                if (!Objects.equals(sourceIndex.getType(), targetIndex.getType())) {
                    diffs.add(new CompareDiff(CompareKind.INDEX_TYPE,
                            String.valueOf(sourceIndex.getType()),
                            String.valueOf(targetIndex.getType())));
                }
                if (!Objects.equals(sourceIndex.getColumns(), targetIndex.getColumns())) {
                    diffs.add(new CompareDiff(CompareKind.INDEX_COLUMNS,
                            String.join(", ", sourceIndex.getColumns()),
                            String.join(", ", targetIndex.getColumns())));
                }
//                if (!Objects.equals(sourceIndex.getEnabled(), targetIndex.getEnabled())) {
//                    diffs.add(new CompareDiff(CompareKind.INDEX_ENABLED,
//                            String.valueOf(sourceIndex.getEnabled()),
//                            String.valueOf(targetIndex.getEnabled())));
//                }
            }
        });
        targetIndexes.forEach((indexName, targetIndex) -> {
            if (!sourceIndexes.containsKey(indexName)) {
                diffs.add(new CompareDiff(CompareKind.INDEX_ABSENT_ON_SOURCE, null, indexName));
            }
        });
    }

    private void compareTriggers(Table sourceTable, Table targetTable, List<CompareDiff> diffs) {
        Map<String, Trigger> sourceTriggers = sourceTable.getTriggerMap();
        Map<String, Trigger> targetTriggers = targetTable.getTriggerMap();
        sourceTriggers.forEach((triggerName, sourceTrigger) -> {
            Trigger targetTrigger = targetTriggers.get(triggerName);
            if (targetTrigger == null) {
                diffs.add(new CompareDiff(CompareKind.TRIGGER_ABSENT_ON_TARGET, triggerName, null));
            } else {
                if (!StringUtils.equalsStringIgnoreWhiteSpace(sourceTrigger.getBody(), targetTrigger.getBody())) {
                    diffs.add(new CompareDiff(CompareKind.TRIGGER_BODY, sourceTrigger.getBody(), targetTrigger.getBody()));
                }
//                if (!Objects.equals(sourceTrigger.getEnabled(), targetTrigger.getEnabled())) {
//                    diffs.add(new CompareDiff(CompareKind.TRIGGER_ENABLED,
//                            String.valueOf(sourceTrigger.getEnabled()),
//                            String.valueOf(targetTrigger.getEnabled())));
//                }
            }
        });
        targetTriggers.forEach((triggerName, targetTrigger) -> {
            if (!sourceTriggers.containsKey(triggerName)) {
                diffs.add(new CompareDiff(CompareKind.TRIGGER_ABSENT_ON_SOURCE, null, triggerName));
            }
        });
    }

    private void compareSequence(Table sourceTable, Table targetTable, List<CompareDiff> diffs) {
        Sequence sourceSequence = sourceTable.getSequence();
        Sequence targetSequence = targetTable.getSequence();
        if ((sourceSequence == null) && (targetSequence == null)) {
            return;
        }
        if (targetSequence == null) {
            diffs.add(new CompareDiff(CompareKind.SEQUENCE_ABSENT_ON_TARGET, sourceSequence.getName(), null));
            return;
        }
        if (sourceSequence == null) {
            diffs.add(new CompareDiff(CompareKind.SEQUENCE_ABSENT_ON_SOURCE, null, targetSequence.getName()));
            return;
        }
        if (!Objects.equals(sourceSequence.getName(), targetSequence.getName())) {
            diffs.add(new CompareDiff(CompareKind.SEQUENCE_NAME, sourceSequence.getName(), targetSequence.getName()));
        }
    }

    private void dumpTable(Schema source, String tableName, OutputStream output, DumpOptions dumpOptions) {
        Table sourceTable = source.getTable(tableName);
        if (sourceTable == null) {
            throw new ReplicatorException("Table " + tableName + " not found on source");
//            return;
        }
        try {
            if (dumpOptions.isDumpDdl()) {
                Ddl ddl = source.getDdl(sourceTable);
                dumpTableDdl(ddl, output, dumpOptions);
                if (dumpOptions.isDumpDml()) {
                    output.write('\n');
                    dumpTableDml(source, sourceTable, output, dumpOptions);
                }
                dumpTableObjectsDdl(ddl, output, dumpOptions);
            } else if (dumpOptions.isDumpDml()) {
                dumpTableDml(source, sourceTable, output, dumpOptions);
            }
        } catch (Exception e) {
            String errorMessage = "Failed to dump table " + tableName + ": " + e.getMessage();
            throw new ReplicatorException(errorMessage, e);
        }
    }

    private void dumpTableDdl(Ddl ddl, OutputStream output, DumpOptions dumpOptions) throws IOException {
        output.write(ddl.getTable().getBytes(dumpOptions.getCharset()));
        output.write(';');
        output.write('\n');
    }

    private void dumpTableObjectsDdl(Ddl ddl, OutputStream output, DumpOptions dumpOptions) throws IOException {
        final Charset charset = dumpOptions.getCharset();
        if (!ddl.getConstraints().isEmpty()) {
            output.write('\n');
            for (String constraint : ddl.getConstraints()) {
                output.write(constraint.getBytes(charset));
                output.write(';');
                output.write('\n');
            }
        }
        if (!ddl.getIndices().isEmpty()) {
            output.write('\n');
            for (String index : ddl.getIndices()) {
                output.write(index.getBytes(charset));
                output.write(';');
                output.write('\n');
            }
        }
        if (ddl.getSequence() != null) {
            output.write('\n');
            output.write(ddl.getSequence().getBytes(charset));
            output.write(';');
            output.write('\n');
        }
        if (!ddl.getTriggers().isEmpty()) {
            output.write('\n');
            for (String trigger : ddl.getTriggers()) {
                output.write(trigger.getBytes(charset));
                output.write('\n');
            }
        }
        output.write('\n');
        output.write(ddl.getAnalyze().getBytes(charset));
        output.write('\n');
    }

    private void dumpTableDml(Schema source, Table sourceTable, OutputStream output, DumpOptions dumpOptions) throws IOException {
        final Charset charset = dumpOptions.getCharset();
        final long verboseEach = dumpOptions.getVerboseEach();
        final long commitEach = dumpOptions.getCommitEach();
        try (Dml dml = source.getDml(sourceTable)) {
            final String commitStatement = dml.getCommitStatement();
            String insertQuery;
            long totalRows = dml.getTotalRows();
            long currentRow = 0;
            while ((insertQuery = dml.nextInsert()) != null) {
                output.write(insertQuery.getBytes(charset));
                output.write(';');
                output.write('\n');
                currentRow++;
                if ((commitEach > 0) && ((currentRow % commitEach) == 0)) {
                    output.write(commitStatement.getBytes(charset));
                    output.write(';');
                    output.write('\n');
                }
                if ((currentRow % verboseEach) == 0) {
                    //TODO:
//                    listener.progress(new Progress(currentRow, totalRows, "Dump table " + sourceTable.getName() + " from source"));
                }
            }
            if ((commitEach == 0) || ((currentRow % commitEach) != 0)) {
                output.write(commitStatement.getBytes(charset));
                output.write(';');
                output.write('\n');
            }
        }
    }

    private void pumpScript(Schema target, File scriptFile) {
        long fileSize = scriptFile.length();
        long verboseStep = 1024L * 1024L;//(long) (fileSize / 1000.0d);
        int lineNumber = 0;
        long readBytes = 0;
        long readBatch = 0;
        try (
                BufferedReader bufferedReader = new BufferedReader(new FileReader(scriptFile));
                BatchExecutor batchExecutor = target.createBatchExecutor()
        ) {
            String line;
            StringBuilder statement = new StringBuilder();
            boolean script = false;
            while ((line = bufferedReader.readLine()) != null) {
                int lineSize = line.length();
                readBytes += (lineSize + 1);
                readBatch += (lineSize + 1);
                lineNumber++;
                if (lineSize == 0) {
                    continue;
                }

                if (!script) {
                    script = line.startsWith("BEGIN");
                }

                boolean endStatement;
                if (script) {
                    endStatement = line.endsWith("END;");
                } else {
                    endStatement = line.charAt(lineSize - 1) == ';';
                }

                if (endStatement) {
                    if (script) {
                        statement.append(line);
                    } else {
                        statement.append(line, 0, line.length() - 1);
                    }
                    batchExecutor.execute(statement.toString());
                    statement.setLength(0);
                    script = false;
                } else {
                    statement.append(line).append('\n');
                }

                if (readBatch >= verboseStep) {
                    //TODO:
//                    listener.progress(new Progress(readBytes, fileSize, "Pump script " + scriptFile.getName() + " to target"));
                    readBatch = 0;
                }
            }
            batchExecutor.finish();
        } catch (Exception e) {
            String errorMessage = "Failed to pump script " + scriptFile.getAbsolutePath() + " at line " + lineNumber + ": " + e.getMessage();
            throw new ReplicatorException(errorMessage, e);
        }
    }

//    private void suppressException(Runnable runnable) {
//        try {
//            runnable.run();
//        } catch (QueryFailedException e) {
//            listener.warning(e.getOrigMessage());
//        } catch (Exception e) {
//            listener.warning(e.getMessage());
//        }
//    }
}
