package ru.xander.replicator.listener;

/**
 * @author Alexander Shakhov
 */
public class Modify {

    private ModifyType modifyType;
    private String tableName;
    private String sql;
    private Integer affectedRows;

    public ModifyType getModifyType() {
        return modifyType;
    }

    public void setModifyType(ModifyType modifyType) {
        this.modifyType = modifyType;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public Integer getAffectedRows() {
        return affectedRows;
    }

    public void setAffectedRows(Integer affectedRows) {
        this.affectedRows = affectedRows;
    }
}
