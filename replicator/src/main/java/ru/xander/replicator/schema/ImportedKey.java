package ru.xander.replicator.schema;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * @author Alexander Shakhov
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ImportedKey extends Constraint {

    private String pkTableSchema;
    private String pkTableName;
    private String pkName;
    private String pkColumnName;

    public String getPkTableSchema() {
        return pkTableSchema;
    }

    public void setPkTableSchema(String pkTableSchema) {
        this.pkTableSchema = pkTableSchema;
    }

    public String getPkTableName() {
        return pkTableName;
    }

    public void setPkTableName(String pkTableName) {
        this.pkTableName = pkTableName;
    }

    public String getPkName() {
        return pkName;
    }

    public void setPkName(String pkName) {
        this.pkName = pkName;
    }

    public String getPkColumnName() {
        return pkColumnName;
    }

    public void setPkColumnName(String pkColumnName) {
        this.pkColumnName = pkColumnName;
    }
}
