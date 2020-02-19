package ru.xander.replicator.action;

import ru.xander.replicator.exception.ReplicatorException;
import ru.xander.replicator.schema.Column;
import ru.xander.replicator.schema.ImportedKey;
import ru.xander.replicator.schema.Index;
import ru.xander.replicator.schema.PrimaryKey;
import ru.xander.replicator.schema.Schema;
import ru.xander.replicator.schema.SchemaConnection;
import ru.xander.replicator.schema.Sequence;
import ru.xander.replicator.schema.Table;
import ru.xander.replicator.schema.Trigger;
import ru.xander.replicator.util.StringUtils;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author Alexander Shakhov
 */
public class ReplicateAction {

    public void execute(String tableName, ReplicateConfig config) {
        Objects.requireNonNull(tableName, "tableName");
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(config.getSourceConfig(), "Configure source schema");
        Objects.requireNonNull(config.getTargetConfig(), "Configure target schema");
        try (
                SchemaConnection source = new SchemaConnection(config.getSourceConfig());
                SchemaConnection target = new SchemaConnection(config.getTargetConfig())
        ) {
            Set<String> createdTables = new HashSet<>();
            replicateTable(tableName, source.getSchema(), target.getSchema(), createdTables, config.isUpdateImported());
        }
    }

    private void replicateTable(String tableName, Schema source, Schema target, Set<String> createdTables, boolean updateImported) {
        if (createdTables.contains(tableName)) {
            return;
        }

        // обновляем, если это запрашиваемая таблица (createdTables пустой)
        // либо если это требуется параметрами
        boolean update = createdTables.isEmpty() || updateImported;

        createdTables.add(tableName);

        Table sourceTable = source.getTable(tableName);
        if (sourceTable == null) {
            throw new ReplicatorException("Table " + tableName + " not found on source");
        }

        sourceTable.getImportedKeys().forEach(importedKey -> {
            String pkTableName = importedKey.getPkTableName();
            replicateTable(pkTableName, source, target, createdTables, updateImported);
        });

        Table targetTable = target.getTable(tableName);
        if (targetTable == null) {
            createTable(target, sourceTable);
        } else if (update) {
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
        table.getIndices().forEach(target::createIndex);
        // Триггеры создаем только на идентичных схемах
        if (table.getVendorType() == target.getVendorType()) {
            table.getTriggers().forEach(target::createTrigger);
        }
        Sequence sequence = table.getSequence();
        if (sequence != null) {
            target.createSequence(sequence);
        }
        target.analyzeTable(table);
    }

    private void updateTable(Schema target, Table targetTable, Table sourceTable) {
        updateColumns(target, targetTable, sourceTable);
        updatePrimaryKey(target, targetTable, sourceTable);
        updateImportedKeys(target, targetTable, sourceTable);
        updateIndices(target, targetTable, sourceTable);
        updateComments(target, targetTable, sourceTable);
        updateSequence(target, targetTable, sourceTable);
        updateTriggers(target, targetTable, sourceTable);
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

    private void updateSequence(Schema target, Table targetTable, Table sourceTable) {
        Sequence sourceSequence = sourceTable.getSequence();
        Sequence targetSequence = targetTable.getSequence();
        if ((targetSequence == null) && (sourceSequence == null)) {
            return;
        }
        if (sourceSequence == null) {
            target.dropSequence(targetSequence);
            targetTable.setSequence(null);
            return;
        }
        if (targetTable.getSequence() == null) {
            target.createSequence(sourceSequence);
            targetTable.setSequence(sourceSequence);
            return;
        }
        if (Objects.equals(sourceSequence.getName(), targetSequence.getName())) {
            return;
        }

        // Сюда по идее попадаем, когда сиквенс у таблицы есть, но назван по-другому
        // Нужно пересоздать его с новым именем
        target.dropSequence(targetSequence);
        target.createSequence(sourceSequence);
        targetTable.setSequence(sourceSequence);
    }

    private void updateTriggers(Schema target, Table targetTable, Table sourceTable) {
        Map<String, Trigger> sourceTriggers = sourceTable.getTriggerMap();
        Map<String, Trigger> targetTriggers = targetTable.getTriggerMap();
        targetTriggers.forEach((triggerName, targetTrigger) -> {
            if (!sourceTriggers.containsKey(triggerName)) {
                target.dropTrigger(targetTrigger);
                targetTable.removeTrigger(targetTrigger);
            }
        });
        sourceTriggers.forEach((triggerName, sourceTrigger) -> {
            Trigger targetTrigger = targetTriggers.get(triggerName);
            if (targetTrigger == null) {
                target.createTrigger(sourceTrigger);
            } else if (!StringUtils.equalsStringIgnoreWhiteSpace(sourceTrigger.getBody(), targetTrigger.getBody())) {
                target.createTrigger(sourceTrigger);
            }
            targetTable.addTrigger(sourceTrigger);
        });
    }

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
}