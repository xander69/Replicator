package ru.xander.replicator.listener;

/**
 * @author Alexander Shakhov
 */
public class Alter {

    private AlterType type;
    private String tableName;
    private String objectName;
    private String extra;

    public AlterType getType() {
        return type;
    }

    public void setType(AlterType type) {
        this.type = type;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getObjectName() {
        return objectName;
    }

    public void setObjectName(String objectName) {
        this.objectName = objectName;
    }

    public String getExtra() {
        return extra;
    }

    public void setExtra(String extra) {
        this.extra = extra;
    }
}
