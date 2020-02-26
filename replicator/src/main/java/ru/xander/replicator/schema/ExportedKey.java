package ru.xander.replicator.schema;

/**
 * @author Alexander Shakhov
 */
public class ExportedKey extends Constraint {

    private String fkTableSchema;
    private String fkTableName;
    private String fkName;
    private String[] fkColumns;

    public String getFkTableSchema() {
        return fkTableSchema;
    }

    public void setFkTableSchema(String fkTableSchema) {
        this.fkTableSchema = fkTableSchema;
    }

    public String getFkTableName() {
        return fkTableName;
    }

    public void setFkTableName(String fkTableName) {
        this.fkTableName = fkTableName;
    }

    public String getFkName() {
        return fkName;
    }

    public void setFkName(String fkName) {
        this.fkName = fkName;
    }

    public String[] getFkColumns() {
        return fkColumns;
    }

    public void setFkColumns(String[] fkColumns) {
        this.fkColumns = fkColumns;
    }
}
