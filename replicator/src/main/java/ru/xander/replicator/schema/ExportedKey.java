package ru.xander.replicator.schema;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * @author Alexander Shakhov
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ExportedKey extends Constraint {

    private String fkTableSchema;
    private String fkTableName;
    private String fkName;
    private String fkColumnName;

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

    public String getFkColumnName() {
        return fkColumnName;
    }

    public void setFkColumnName(String fkColumnName) {
        this.fkColumnName = fkColumnName;
    }
}
