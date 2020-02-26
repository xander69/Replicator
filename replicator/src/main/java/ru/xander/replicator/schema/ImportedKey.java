package ru.xander.replicator.schema;

/**
 * @author Alexander Shakhov
 */
public class ImportedKey extends Constraint {

    private String pkTableSchema;
    private String pkTableName;
    private String pkName;
    private String[] pkColumns;

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

    public String[] getPkColumns() {
        return pkColumns;
    }

    public void setPkColumns(String[] pkColumns) {
        this.pkColumns = pkColumns;
    }
}
