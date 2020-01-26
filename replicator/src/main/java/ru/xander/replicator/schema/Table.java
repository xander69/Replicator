package ru.xander.replicator.schema;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

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
    //TODO: реализовать поддержку партиций
    private VendorType vendorType;

    public Table() {
    }

    public Table(String schema, String name) {
        this.schema = schema;
        this.name = name;
    }

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
        return columnMap;
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
        return importedKeyMap;
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

    public Map<String, ExportedKey> getExportedKeyMap() {
        if (exportedKeyMap == null) {
            return Collections.emptyMap();
        }
        return exportedKeyMap;
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

    public Map<String, CheckConstraint> getCheckConstraintMap() {
        if (checkConstraintMap == null) {
            return Collections.emptyMap();
        }
        return checkConstraintMap;
    }

    public Collection<CheckConstraint> getCheckConstraints() {
        if (checkConstraintMap == null) {
            return Collections.emptyList();
        }
        return checkConstraintMap.values();
    }

    public CheckConstraint getCheckConstraint(String keyName) {
        if (checkConstraintMap == null) {
            return null;
        }
        return checkConstraintMap.get(keyName);
    }

    public void addCheckConstraint(CheckConstraint checkConstraint) {
        if (checkConstraintMap == null) {
            checkConstraintMap = new LinkedHashMap<>();
        }
        checkConstraintMap.put(checkConstraint.getName(), checkConstraint);
    }

    public Map<String, Index> getIndexMap() {
        if (indexMap == null) {
            return Collections.emptyMap();
        }
        return indexMap;
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

    public Map<String, Trigger> getTriggerMap() {
        if (triggerMap == null) {
            return Collections.emptyMap();
        }
        return triggerMap;
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

    public Sequence getSequence() {
        return sequence;
    }

    public void setSequence(Sequence sequence) {
        this.sequence = sequence;
    }

    public VendorType getVendorType() {
        return vendorType;
    }

    public void setVendorType(VendorType vendorType) {
        this.vendorType = vendorType;
    }
}
