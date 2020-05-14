package ru.xander.replicator.schema;

import ru.xander.replicator.util.StringUtils;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author Alexander Shakhov
 */
//TODO: реализовать поддержку партиций
public class Table {

    private String schema;
    private String name;
    private String comment;
    private Map<String, Column> columnMap;
    private PrimaryKey primaryKey;
    private Map<String, ImportedKey> importedKeyMap;
    private Map<String, ExportedKey> exportedKeyMap;
    private Map<String, CheckConstraint> checkConstraintMap;
    private Map<String, Index> indexMap;
    private Map<String, Trigger> triggerMap;
    private Sequence sequence;

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public Map<String, Column> getColumnMap() {
        if (columnMap == null) {
            return Collections.emptyMap();
        }
        return Collections.unmodifiableMap(columnMap);
    }

    public Collection<Column> getColumns() {
        if (columnMap == null) {
            return Collections.emptyList();
        }
        return columnMap.values();
    }

    public Column getColumn(String columnName) {
        if (columnMap == null) {
            return null;
        }
        return columnMap.get(columnName);
    }

    public void addColumn(Column column) {
        if (columnMap == null) {
            columnMap = new LinkedHashMap<>();
        }
        columnMap.put(column.getName(), column);
    }

    public void removeColumn(Column column) {
        if (columnMap == null) {
            return;
        }
        columnMap.remove(column.getName());
        if ((primaryKey != null) && StringUtils.arrayContains(primaryKey.getColumns(), column.getName())) {
            primaryKey = null;
        }
        getImportedKeys().stream()
                .filter(key -> StringUtils.arrayContains(key.getColumns(), column.getName()))
                .forEach(this::removeImportedKey);
        getExportedKeys().stream()
                .filter(key -> StringUtils.arrayContains(key.getColumns(), column.getName()))
                .forEach(this::removeExportedKey);
        getCheckConstraints().stream()
                .filter(key -> StringUtils.arrayContains(key.getColumns(), column.getName()))
                .forEach(this::removeCheckConstraint);
        getIndices().stream()
                .filter(key -> StringUtils.arrayContains(key.getColumns(), column.getName()))
                .forEach(this::removeIndex);
    }

    public PrimaryKey getPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(PrimaryKey primaryKey) {
        this.primaryKey = primaryKey;
    }

    public Map<String, ImportedKey> getImportedKeyMap() {
        if (importedKeyMap == null) {
            return Collections.emptyMap();
        }
        return Collections.unmodifiableMap(importedKeyMap);
    }

    public Collection<ImportedKey> getImportedKeys() {
        if (importedKeyMap == null) {
            return Collections.emptyList();
        }
        return importedKeyMap.values();
    }

    public ImportedKey getImportedKey(String keyName) {
        if (importedKeyMap == null) {
            return null;
        }
        return importedKeyMap.get(keyName);
    }

    public void addImportedKey(ImportedKey importedKey) {
        if (importedKeyMap == null) {
            importedKeyMap = new LinkedHashMap<>();
        }
        importedKeyMap.put(importedKey.getName(), importedKey);
    }

    public void removeImportedKey(ImportedKey importedKey) {
        if (importedKeyMap == null) {
            return;
        }
        importedKeyMap.remove(importedKey.getName());
    }

    public Map<String, ExportedKey> getExportedKeyMap() {
        if (exportedKeyMap == null) {
            return Collections.emptyMap();
        }
        return Collections.unmodifiableMap(exportedKeyMap);
    }

    public Collection<ExportedKey> getExportedKeys() {
        if (exportedKeyMap == null) {
            return Collections.emptyList();
        }
        return exportedKeyMap.values();
    }

    public ExportedKey getExportedKey(String keyName) {
        if (exportedKeyMap == null) {
            return null;
        }
        return exportedKeyMap.get(keyName);
    }

    public void addExportedKey(ExportedKey exportedKey) {
        if (exportedKeyMap == null) {
            exportedKeyMap = new LinkedHashMap<>();
        }
        exportedKeyMap.put(exportedKey.getFkName(), exportedKey);
    }

    public void removeExportedKey(ExportedKey exportedKey) {
        if (exportedKeyMap == null) {
            return;
        }
        exportedKeyMap.remove(exportedKey.getName());
    }

    public Map<String, CheckConstraint> getCheckConstraintMap() {
        if (checkConstraintMap == null) {
            return Collections.emptyMap();
        }
        return Collections.unmodifiableMap(checkConstraintMap);
    }

    public Collection<CheckConstraint> getCheckConstraints() {
        if (checkConstraintMap == null) {
            return Collections.emptyList();
        }
        return checkConstraintMap.values();
    }

    public CheckConstraint getCheckConstraint(String constraintName) {
        if (checkConstraintMap == null) {
            return null;
        }
        return checkConstraintMap.get(constraintName);
    }

    public void addCheckConstraint(CheckConstraint checkConstraint) {
        if (checkConstraintMap == null) {
            checkConstraintMap = new LinkedHashMap<>();
        }
        checkConstraintMap.put(checkConstraint.getName(), checkConstraint);
    }

    public void removeCheckConstraint(CheckConstraint checkConstraint) {
        if (checkConstraintMap == null) {
            return;
        }
        checkConstraintMap.remove(checkConstraint.getName());
    }

    public Map<String, Index> getIndexMap() {
        if (indexMap == null) {
            return Collections.emptyMap();
        }
        return Collections.unmodifiableMap(indexMap);
    }

    public Collection<Index> getIndices() {
        if (indexMap == null) {
            return Collections.emptyList();
        }
        return indexMap.values();
    }

    public Index getIndex(String indexName) {
        if (indexMap == null) {
            return null;
        }
        return indexMap.get(indexName);
    }

    public void addIndex(Index index) {
        if (indexMap == null) {
            indexMap = new LinkedHashMap<>();
        }
        indexMap.put(index.getName(), index);
    }

    public void removeIndex(Index index) {
        if (indexMap == null) {
            return;
        }
        indexMap.remove(index.getName());
    }

    public Map<String, Trigger> getTriggerMap() {
        if (triggerMap == null) {
            return Collections.emptyMap();
        }
        return Collections.unmodifiableMap(triggerMap);
    }

    public Collection<Trigger> getTriggers() {
        if (triggerMap == null) {
            return Collections.emptyList();
        }
        return triggerMap.values();
    }

    public Trigger getTrigger(String triggerName) {
        if (triggerMap == null) {
            return null;
        }
        return triggerMap.get(triggerName);
    }

    public void addTrigger(Trigger trigger) {
        if (triggerMap == null) {
            triggerMap = new LinkedHashMap<>();
        }
        triggerMap.put(trigger.getName(), trigger);
    }

    public void removeTrigger(Trigger targetTrigger) {
        if (triggerMap == null) {
            return;
        }
        triggerMap.remove(targetTrigger.getName());
    }

    public Sequence getSequence() {
        return sequence;
    }

    public void setSequence(Sequence sequence) {
        this.sequence = sequence;
    }
}
