package ru.xander.replicator.action;

import ru.xander.replicator.compare.CompareDiff;
import ru.xander.replicator.compare.CompareKind;
import ru.xander.replicator.compare.CompareResult;
import ru.xander.replicator.compare.CompareResultType;
import ru.xander.replicator.schema.Column;
import ru.xander.replicator.schema.ColumnDiff;
import ru.xander.replicator.schema.ImportedKey;
import ru.xander.replicator.schema.Index;
import ru.xander.replicator.schema.PrimaryKey;
import ru.xander.replicator.schema.Schema;
import ru.xander.replicator.schema.SchemaConfig;
import ru.xander.replicator.schema.SchemaConnection;
import ru.xander.replicator.schema.Sequence;
import ru.xander.replicator.schema.Table;
import ru.xander.replicator.schema.Trigger;
import ru.xander.replicator.util.StringUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * @author Alexander Shakhov
 */
public class CompareAction implements Action {

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
                ColumnDiff[] columnDiffs = sourceColumn.getDiffs(targetColumn);
                for (ColumnDiff columnDiff : columnDiffs) {
                    switch (columnDiff) {
                        case DATATYPE:
                            String sourceDatatype = sourceColumn.getColumnType()
                                    + ", size: " + sourceColumn.getSize()
                                    + ", scale: " + sourceColumn.getScale();
                            String targetDatatype = targetColumn.getColumnType()
                                    + ", size: " + targetColumn.getSize()
                                    + ", scale: " + targetColumn.getScale();
                            diffs.add(new CompareDiff(CompareKind.COLUMN_DATATYPE, sourceDatatype, targetDatatype));
                            break;
                        case MANDATORY:
                            diffs.add(new CompareDiff(CompareKind.COLUMN_MANDATORY,
                                    String.valueOf(sourceColumn.isNullable()),
                                    String.valueOf(targetColumn.isNullable())));
                            break;
                        case DEFAULT:
                            diffs.add(new CompareDiff(CompareKind.COLUMN_DEFAULT,
                                    sourceColumn.getDefaultValue(),
                                    targetColumn.getDefaultValue()));
                            break;
                    }
                }
                if (!StringUtils.equalsStringIgnoreWhiteSpace(sourceColumn.getComment(), targetColumn.getComment())) {
                    diffs.add(new CompareDiff(CompareKind.COLUMN_COMMENT, sourceColumn.getComment(), targetColumn.getComment()));
                }
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
}
