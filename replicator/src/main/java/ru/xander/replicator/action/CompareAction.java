package ru.xander.replicator.action;

import ru.xander.replicator.compare.CompareDiff;
import ru.xander.replicator.compare.CompareKind;
import ru.xander.replicator.compare.CompareResult;
import ru.xander.replicator.compare.CompareResultType;
import ru.xander.replicator.schema.CheckConstraint;
import ru.xander.replicator.schema.Column;
import ru.xander.replicator.schema.ColumnDiff;
import ru.xander.replicator.schema.Dialect;
import ru.xander.replicator.schema.ImportedKey;
import ru.xander.replicator.schema.Index;
import ru.xander.replicator.schema.PrimaryKey;
import ru.xander.replicator.schema.Schema;
import ru.xander.replicator.schema.SchemaConfig;
import ru.xander.replicator.schema.SchemaConnection;
import ru.xander.replicator.schema.SchemaUtils;
import ru.xander.replicator.schema.Sequence;
import ru.xander.replicator.schema.Table;
import ru.xander.replicator.schema.Trigger;
import ru.xander.replicator.util.StringUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;

/**
 * @author Alexander Shakhov
 */
public class CompareAction implements Action {

    private final ExecutorService executorService;
    private final SchemaConfig sourceConfig;
    private final SchemaConfig targetConfig;
    private final String[] tables;

    public CompareAction(SchemaConfig sourceConfig, SchemaConfig targetConfig, String[] tables) {
        Objects.requireNonNull(sourceConfig, "Configure source schema");
        Objects.requireNonNull(targetConfig, "Configure target schema");
        Objects.requireNonNull(tables, "Tables for compare");
        if (tables.length == 0) {
            throw new IllegalArgumentException("At least one table must be specified for compare");
        }
        this.sourceConfig = sourceConfig;
        this.targetConfig = targetConfig;
        this.tables = tables;
        this.executorService = Executors.newFixedThreadPool(2);
    }

    public Map<String, CompareResult> execute() {
        try (
                SchemaConnection source = new SchemaConnection(sourceConfig);
                SchemaConnection target = new SchemaConnection(targetConfig)
        ) {
            Map<String, CompareResult> resultMap = new HashMap<>();
            for (String tableName : tables) {
                CompareResult compareResult = compareTable(tableName, source.getSchema(), target.getSchema());
                resultMap.put(tableName, compareResult);
            }
            return resultMap;
        }
    }

    private CompareResult compareTable(String tableName, Schema source, Schema target) {
        Future<Table> sourceTableFuture = executorService.submit(() -> source.getTable(tableName));
        Future<Table> targetTableFuture = executorService.submit(() -> target.getTable(tableName));

        Table sourceTable;
        Table targetTable;
        try {
            sourceTable = sourceTableFuture.get();
            targetTable = targetTableFuture.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("Interrupter get table task: " + e.getMessage());
        }

        if (sourceTable == null) {
            return new CompareResult(CompareResultType.ABSENT_ON_SOURCE, Collections.emptyList());
        }
        if (targetTable == null) {
            return new CompareResult(CompareResultType.ABSENT_ON_TARGET, Collections.emptyList());
        }
        DiffCollector diffCollector = new DiffCollector(target);
        compareTableComment(sourceTable, targetTable, diffCollector);
        compareColumns(sourceTable, targetTable, diffCollector);
        comparePrimaryKey(sourceTable, targetTable, diffCollector);
        compareImportedKeys(sourceTable, targetTable, diffCollector);
        compareCheckConstraints(sourceTable, targetTable, diffCollector);
        compareIndexes(sourceTable, targetTable, diffCollector);
        compareTriggers(sourceTable, targetTable, diffCollector);
        compareSequence(sourceTable, targetTable, diffCollector);
        if (diffCollector.diffs.isEmpty()) {
            return new CompareResult(CompareResultType.EQUALS, Collections.emptyList());
        } else {
            return new CompareResult(CompareResultType.DIFFERENT, diffCollector.diffs);
        }
    }

    private void compareTableComment(Table sourceTable, Table targetTable, DiffCollector diffCollector) {
        if (!StringUtils.equalsStringIgnoreWhiteSpace(sourceTable.getComment(), targetTable.getComment())) {
            diffCollector.add(
                    CompareKind.TABLE_COMMENT,
                    sourceTable.getComment(),
                    targetTable.getComment(),
                    dialect -> dialect.createTableCommentQuery(sourceTable));
        }
    }

    private void compareColumns(Table sourceTable, Table targetTable, DiffCollector diffCollector) {
        Map<String, Column> sourceColumns = sourceTable.getColumnMap();
        Map<String, Column> targetColumns = targetTable.getColumnMap();
        sourceColumns.forEach((columnName, sourceColumn) -> {
            Column targetColumn = targetColumns.get(columnName);
            if (targetColumn == null) {
                diffCollector.add(
                        CompareKind.COLUMN_ABSENT_ON_TARGET,
                        columnName,
                        null,
                        dialect -> dialect.createColumnQuery(sourceColumn));
            } else {
                ColumnDiff[] columnDiffs = SchemaUtils.compareColumns(sourceColumn, targetColumn);
                for (ColumnDiff columnDiff : columnDiffs) {
                    switch (columnDiff) {
                        case DATATYPE:
                            String sourceDatatype = sourceColumn.getColumnType()
                                    + ", size: " + sourceColumn.getSize()
                                    + ", scale: " + sourceColumn.getScale();
                            String targetDatatype = targetColumn.getColumnType()
                                    + ", size: " + targetColumn.getSize()
                                    + ", scale: " + targetColumn.getScale();

                            diffCollector.add(
                                    CompareKind.COLUMN_DATATYPE,
                                    sourceDatatype,
                                    targetDatatype,
                                    dialect -> dialect.modifyColumnQuery(sourceColumn, ColumnDiff.DATATYPE));
                            break;
                        case MANDATORY:
                            diffCollector.add(
                                    CompareKind.COLUMN_MANDATORY,
                                    String.valueOf(sourceColumn.isNullable()),
                                    String.valueOf(targetColumn.isNullable()),
                                    dialect -> dialect.modifyColumnQuery(sourceColumn, ColumnDiff.MANDATORY));
                            break;
                        case DEFAULT:
                            diffCollector.add(
                                    CompareKind.COLUMN_DEFAULT,
                                    sourceColumn.getDefaultValue(),
                                    targetColumn.getDefaultValue(),
                                    dialect -> dialect.modifyColumnQuery(sourceColumn, ColumnDiff.DEFAULT));
                            break;
                    }
                }
                if (!StringUtils.equalsStringIgnoreWhiteSpace(sourceColumn.getComment(), targetColumn.getComment())) {
                    diffCollector.add(
                            CompareKind.COLUMN_COMMENT,
                            sourceColumn.getComment(),
                            targetColumn.getComment(),
                            dialect -> dialect.createColumnCommentQuery(sourceColumn));
                }
            }
        });
        targetColumns.forEach((columnName, targetColumn) -> {
            if (!sourceColumns.containsKey(columnName)) {
                diffCollector.add(
                        CompareKind.COLUMN_ABSENT_ON_SOURCE,
                        null,
                        columnName,
                        dialect -> dialect.dropColumnQuery(targetColumn));
            }
        });
    }

    private void comparePrimaryKey(Table sourceTable, Table targetTable, DiffCollector diffCollector) {
        PrimaryKey sourcePrimaryKey = sourceTable.getPrimaryKey();
        PrimaryKey targetPrimaryKey = targetTable.getPrimaryKey();
        if ((sourcePrimaryKey == null) && (targetPrimaryKey == null)) {
            return;
        }
        if (targetPrimaryKey == null) {
            diffCollector.add(
                    CompareKind.PRIMARY_KEY_ABSENT_ON_TARGET,
                    sourcePrimaryKey.getName(),
                    null,
                    dialect -> dialect.createPrimaryKeyQuery(sourcePrimaryKey));
            return;
        }
        if (sourcePrimaryKey == null) {
            diffCollector.add(
                    CompareKind.PRIMARY_KEY_ABSENT_ON_SOURCE,
                    null,
                    targetPrimaryKey.getName(),
                    dialect -> dialect.dropPrimaryKeyQuery(targetPrimaryKey));
            return;
        }
        if (!Objects.equals(sourcePrimaryKey.getName(), targetPrimaryKey.getName())) {
            diffCollector.add(
                    CompareKind.PRIMARY_KEY_NAME,
                    sourcePrimaryKey.getName(),
                    targetPrimaryKey.getName(),
                    dialect -> dialect.renameConstraintQuery(targetPrimaryKey, sourcePrimaryKey.getName()));
        }
        if (!Arrays.equals(sourcePrimaryKey.getColumns(), targetPrimaryKey.getColumns())) {
            diffCollector.add(
                    CompareKind.PRIMARY_KEY_COLUMNS,
                    StringUtils.joinColumns(sourcePrimaryKey.getColumns()),
                    StringUtils.joinColumns(targetPrimaryKey.getColumns()),
                    // TODO: не красиво как-то. надо предусмотреть возможность нескольких запросов для одной операции
                    dialect -> dialect.dropPrimaryKeyQuery(targetPrimaryKey) + "\n" + dialect.createPrimaryKeyQuery(sourcePrimaryKey));
        }
    }

    private void compareImportedKeys(Table sourceTable, Table targetTable, DiffCollector diffCollector) {
        Map<String, ImportedKey> sourceImportedKeys = sourceTable.getImportedKeyMap();
        Map<String, ImportedKey> targetImportedKeys = targetTable.getImportedKeyMap();
        sourceImportedKeys.forEach((importedKeyName, sourceImportedKey) -> {
            ImportedKey targetImportedKey = targetImportedKeys.get(importedKeyName);
            if (targetImportedKey == null) {
                diffCollector.add(
                        CompareKind.IMPORTED_KEY_ABSENT_ON_TARGET,
                        importedKeyName,
                        null,
                        dialect -> dialect.createImportedKeyQuery(sourceImportedKey));
            } else {
                if (!Arrays.equals(sourceImportedKey.getColumns(), targetImportedKey.getColumns())) {
                    diffCollector.add(
                            CompareKind.IMPORTED_KEY_COLUMNS,
                            StringUtils.joinColumns(sourceImportedKey.getColumns()),
                            StringUtils.joinColumns(targetImportedKey.getColumns()),
                            // TODO: не красиво как-то. надо предусмотреть возможность нескольких запросов для одной операции
                            dialect -> dialect.dropConstraintQuery(targetImportedKey) + "\n" + dialect.createImportedKeyQuery(sourceImportedKey));
                }
                if (!Objects.equals(sourceImportedKey.getPkName(), targetImportedKey.getPkName())) {
                    diffCollector.add(
                            CompareKind.IMPORTED_KEY_PK_NAME,
                            sourceImportedKey.getPkName(),
                            targetImportedKey.getPkName(),
                            // TODO: не красиво как-то. надо предусмотреть возможность нескольких запросов для одной операции
                            dialect -> dialect.dropConstraintQuery(targetImportedKey) + "\n" + dialect.createImportedKeyQuery(sourceImportedKey));
                }
                if (!Objects.equals(sourceImportedKey.getPkTableName(), targetImportedKey.getPkTableName())) {
                    diffCollector.add(
                            CompareKind.IMPORTED_KEY_PK_TABLE,
                            sourceImportedKey.getPkTableName(),
                            targetImportedKey.getPkTableName(),
                            // TODO: не красиво как-то. надо предусмотреть возможность нескольких запросов для одной операции
                            dialect -> dialect.dropConstraintQuery(targetImportedKey) + "\n" + dialect.createImportedKeyQuery(sourceImportedKey));
                }
                if (!Arrays.equals(sourceImportedKey.getPkColumns(), targetImportedKey.getPkColumns())) {
                    diffCollector.add(
                            CompareKind.IMPORTED_KEY_PK_COLUMNS,
                            StringUtils.joinColumns(sourceImportedKey.getPkColumns()),
                            StringUtils.joinColumns(targetImportedKey.getPkColumns()),
                            // TODO: не красиво как-то. надо предусмотреть возможность нескольких запросов для одной операции
                            dialect -> dialect.dropConstraintQuery(targetImportedKey) + "\n" + dialect.createImportedKeyQuery(sourceImportedKey));
                }
            }
        });
        targetImportedKeys.forEach((importedKeyName, targetImportedKey) -> {
            if (!sourceImportedKeys.containsKey(importedKeyName)) {
                diffCollector.add(
                        CompareKind.IMPORTED_KEY_ABSENT_ON_SOURCE,
                        null,
                        importedKeyName,
                        dialect -> dialect.dropConstraintQuery(targetImportedKey));
            }
        });
    }

    private void compareCheckConstraints(Table sourceTable, Table targetTable, DiffCollector diffCollector) {
        Map<String, CheckConstraint> sourceCheckConstraints = sourceTable.getCheckConstraintMap();
        Map<String, CheckConstraint> targetCheckConstraints = targetTable.getCheckConstraintMap();
        sourceCheckConstraints.forEach((checkConstraintName, sourceCheckConstraint) -> {
            CheckConstraint targetCheckConstraint = targetCheckConstraints.get(checkConstraintName);
            if (targetCheckConstraint == null) {
                diffCollector.add(
                        CompareKind.CHECK_CONSTRAINT_ABSENT_ON_TARGET,
                        checkConstraintName,
                        null,
                        dialect -> dialect.createCheckConstraintQuery(sourceCheckConstraint));
            } else {
                if (!Arrays.equals(sourceCheckConstraint.getColumns(), targetCheckConstraint.getColumns())) {
                    diffCollector.add(
                            CompareKind.CHECK_CONSTRAINT_COLUMNS,
                            StringUtils.joinColumns(sourceCheckConstraint.getColumns()),
                            StringUtils.joinColumns(targetCheckConstraint.getColumns()),
                            // TODO: не красиво как-то. надо предусмотреть возможность нескольких запросов для одной операции
                            dialect -> dialect.dropConstraintQuery(targetCheckConstraint) + "\n" + dialect.createCheckConstraintQuery(sourceCheckConstraint));
                }
                if (!Objects.equals(sourceCheckConstraint.getCondition(), targetCheckConstraint.getCondition())) {
                    diffCollector.add(
                            CompareKind.CHECK_CONSTRAINT_CONDITION,
                            sourceCheckConstraint.getCondition(),
                            targetCheckConstraint.getCondition(),
                            // TODO: не красиво как-то. надо предусмотреть возможность нескольких запросов для одной операции
                            dialect -> dialect.dropConstraintQuery(targetCheckConstraint) + "\n" + dialect.createCheckConstraintQuery(sourceCheckConstraint));
                }
            }
        });
        targetCheckConstraints.forEach((checkConstraintName, targetCheckConstraint) -> {
            if (!sourceCheckConstraints.containsKey(checkConstraintName)) {
                diffCollector.add(
                        CompareKind.CHECK_CONSTRAINT_ABSENT_ON_SOURCE,
                        null,
                        checkConstraintName,
                        dialect -> dialect.dropConstraintQuery(targetCheckConstraint));
            }
        });
    }

    private void compareIndexes(Table sourceTable, Table targetTable, DiffCollector diffCollector) {
        Map<String, Index> sourceIndexes = sourceTable.getIndexMap();
        Map<String, Index> targetIndexes = targetTable.getIndexMap();
        sourceIndexes.forEach((indexName, sourceIndex) -> {
            Index targetIndex = targetIndexes.get(indexName);
            if (targetIndex == null) {
                diffCollector.add(
                        CompareKind.INDEX_ABSENT_ON_TARGET,
                        indexName,
                        null,
                        dialect -> dialect.createIndexQuery(sourceIndex));
            } else {
                if (!Objects.equals(sourceIndex.getType(), targetIndex.getType())) {
                    diffCollector.add(
                            CompareKind.INDEX_TYPE,
                            String.valueOf(sourceIndex.getType()),
                            String.valueOf(targetIndex.getType()),
                            // TODO: не красиво как-то. надо предусмотреть возможность нескольких запросов для одной операции
                            dialect -> dialect.dropIndexQuery(targetIndex) + "\n" + dialect.createIndexQuery(sourceIndex));
                }
                if (!Arrays.equals(sourceIndex.getColumns(), targetIndex.getColumns())) {
                    diffCollector.add(
                            CompareKind.INDEX_COLUMNS,
                            StringUtils.joinColumns(sourceIndex.getColumns()),
                            StringUtils.joinColumns(targetIndex.getColumns()),
                            // TODO: не красиво как-то. надо предусмотреть возможность нескольких запросов для одной операции
                            dialect -> dialect.dropIndexQuery(targetIndex) + "\n" + dialect.createIndexQuery(sourceIndex));
                }
            }
        });
        targetIndexes.forEach((indexName, targetIndex) -> {
            if (!sourceIndexes.containsKey(indexName)) {
                diffCollector.add(
                        CompareKind.INDEX_ABSENT_ON_SOURCE,
                        null,
                        indexName,
                        dialect -> dialect.dropIndexQuery(targetIndex));
            }
        });
    }

    private void compareTriggers(Table sourceTable, Table targetTable, DiffCollector diffCollector) {
        Map<String, Trigger> sourceTriggers = sourceTable.getTriggerMap();
        Map<String, Trigger> targetTriggers = targetTable.getTriggerMap();
        sourceTriggers.forEach((triggerName, sourceTrigger) -> {
            Trigger targetTrigger = targetTriggers.get(triggerName);
            if (targetTrigger == null) {
                diffCollector.add(
                        CompareKind.TRIGGER_ABSENT_ON_TARGET,
                        triggerName,
                        null,
                        dialect -> dialect.createTriggerQuery(sourceTrigger));
            } else {
                if (!StringUtils.equalsStringIgnoreWhiteSpace(sourceTrigger.getBody(), targetTrigger.getBody())) {
                    diffCollector.add(
                            CompareKind.TRIGGER_BODY,
                            sourceTrigger.getBody(),
                            targetTrigger.getBody(),
                            dialect -> dialect.createTriggerQuery(sourceTrigger));
                }
            }
        });
        targetTriggers.forEach((triggerName, targetTrigger) -> {
            if (!sourceTriggers.containsKey(triggerName)) {
                diffCollector.add(
                        CompareKind.TRIGGER_ABSENT_ON_SOURCE,
                        null,
                        triggerName,
                        dialect -> dialect.dropTriggerQuery(targetTrigger));
            }
        });
    }

    private void compareSequence(Table sourceTable, Table targetTable, DiffCollector diffCollector) {
        Sequence sourceSequence = sourceTable.getSequence();
        Sequence targetSequence = targetTable.getSequence();
        if ((sourceSequence == null) && (targetSequence == null)) {
            return;
        }
        if (targetSequence == null) {
            diffCollector.add(
                    CompareKind.SEQUENCE_ABSENT_ON_TARGET,
                    sourceSequence.getName(),
                    null,
                    dialect -> dialect.createSequenceQuery(sourceSequence));
            return;
        }
        if (sourceSequence == null) {
            diffCollector.add(
                    CompareKind.SEQUENCE_ABSENT_ON_SOURCE,
                    null,
                    targetSequence.getName(),
                    dialect -> dialect.dropSequenceQuery(targetSequence));
            return;
        }
        if (!Objects.equals(sourceSequence.getName(), targetSequence.getName())) {
            diffCollector.add(
                    CompareKind.SEQUENCE_NAME,
                    sourceSequence.getName(),
                    targetSequence.getName(),
                    // TODO: не красиво как-то. надо предусмотреть возможность нескольких запросов для одной операции
                    dialect -> dialect.dropSequenceQuery(targetSequence) + "\n" + dialect.createSequenceQuery(sourceSequence));
        }
        //TODO: сравнивать startWith, incrementBy, minValue, maxValue, cycle, cacheSize
    }

    private static class DiffCollector {
        private final List<CompareDiff> diffs;
        private final Schema target;

        private DiffCollector(Schema target) {
            this.diffs = new LinkedList<>();
            this.target = target;
        }

        private void add(CompareKind kind, String sourceValue, String targetValue, Function<Dialect, String> alterFunction) {
            CompareDiff compareDiff = new CompareDiff();
            compareDiff.setKind(kind);
            compareDiff.setSourceValue(sourceValue);
            compareDiff.setTargetValue(targetValue);
            compareDiff.setAlter(alterFunction.apply(target.getDialect()));
            this.diffs.add(compareDiff);
        }
    }
}
