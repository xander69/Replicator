package ru.xander.replicator.schema.hsqldb;

import ru.xander.replicator.exception.SchemaException;
import ru.xander.replicator.filter.Filter;
import ru.xander.replicator.schema.AbstractSchema;
import ru.xander.replicator.schema.CheckConstraint;
import ru.xander.replicator.schema.Column;
import ru.xander.replicator.schema.ColumnType;
import ru.xander.replicator.schema.Constraint;
import ru.xander.replicator.schema.DataFormatter;
import ru.xander.replicator.schema.Dialect;
import ru.xander.replicator.schema.ExportedKey;
import ru.xander.replicator.schema.ImportedKey;
import ru.xander.replicator.schema.Index;
import ru.xander.replicator.schema.IndexType;
import ru.xander.replicator.schema.PrimaryKey;
import ru.xander.replicator.schema.SchemaConfig;
import ru.xander.replicator.schema.Sequence;
import ru.xander.replicator.schema.Table;
import ru.xander.replicator.schema.Trigger;
import ru.xander.replicator.schema.VendorType;
import ru.xander.replicator.util.StringUtils;

import java.util.LinkedList;
import java.util.List;

/**
 * @author Alexander Shakhov
 */
public class HsqldbSchema extends AbstractSchema {

    private final HsqldbDialect dialect;
    private final HsqldbSchemaQueries schemaQueries;

    public HsqldbSchema(SchemaConfig config) {
        super(config);
        this.dialect = new HsqldbDialect(workSchema);
        this.schemaQueries = new HsqldbSchemaQueries(workSchema);
    }

    @Override
    public VendorType getVendorType() {
        return VendorType.HSQLDB;
    }

    @Override
    public Dialect getDialect() {
        return this.dialect;
    }

    @Override
    public DataFormatter getDataFormatter() {
        return new HsqldbDataFormatter();
    }

    @Override
    public List<String> getTables(List<Filter> filterList) {
        return findTables(filterList);
    }

    @Override
    public Table getTable(String tableName) {
        Table table = findTable(tableName);
        if (table == null) {
            return null;
        }
        findColumns(table);
        findConstraints(table);
        findIndices(table);
//        findTriggers(table);
//        findSequence(table);
        return table;
    }

    @Override
    public void createTable(Table table) {

    }

    @Override
    public void dropTable(Table table) {

    }

    @Override
    public void createTableComment(Table table) {

    }

    @Override
    public void createColumn(Column column) {

    }

    @Override
    public void modifyColumn(Column oldColumn, Column newColumn) {

    }

    @Override
    public void dropColumn(Column column) {

    }

    @Override
    public void createColumnComment(Column column) {

    }

    @Override
    public void createPrimaryKey(PrimaryKey primaryKey) {

    }

    @Override
    public void dropPrimaryKey(PrimaryKey primaryKey) {

    }

    @Override
    public void createImportedKey(ImportedKey importedKey) {

    }

    @Override
    public void createCheckConstraint(CheckConstraint checkConstraint) {

    }

    @Override
    public void dropConstraint(Constraint constraint) {

    }

    @Override
    public void toggleConstraint(Constraint constraint, boolean enabled) {

    }

    @Override
    public void createIndex(Index index) {

    }

    @Override
    public void dropIndex(Index index) {

    }

    @Override
    public void toggleIndex(Index index, boolean enabled) {

    }

    @Override
    public void createTrigger(Trigger trigger) {

    }

    @Override
    public void dropTrigger(Trigger trigger) {

    }

    @Override
    public void toggleTrigger(Trigger trigger, boolean enabled) {

    }

    @Override
    public void createSequence(Sequence sequence) {

    }

    @Override
    public void dropSequence(Sequence sequence) {

    }

    @Override
    public void analyzeTable(Table table) {

    }

    private List<String> findTables(List<Filter> filterList) {
        notify("Find tables: " + filterListToString(filterList));
        List<String> tableList = new LinkedList<>();
        select(schemaQueries.selectTables(filterList), rs -> tableList.add(rs.getString("TABLE_NAME")));
        return tableList;
    }

    private Table findTable(String tableName) {
        notify("Find table " + tableName);
        return selectOne(schemaQueries.selectTable(tableName),
                rs -> {
                    Table table = new Table();
                    table.setSchema(rs.getString("TABLE_SCHEM"));
                    table.setName(rs.getString("TABLE_NAME"));
                    table.setComment(rs.getString("REMARKS"));
                    return table;
                });
    }

    private void findColumns(Table table) {
        notify("Find columns for table " + table.getName());
        select(schemaQueries.selectColumns(table), rs -> {
            final String typeName = rs.getString("TYPE_NAME");
            final int size = rs.getInt("COLUMN_SIZE");
            final int scale = rs.getInt("DECIMAL_DIGITS");

            Column column = new Column();
            column.setTable(table);
            column.setNumber(rs.getInt("ORDINAL_POSITION"));
            column.setName(rs.getString("COLUMN_NAME"));
            column.setSize(size);
            column.setScale(scale);
            column.setNullable(1 == rs.getInt("NULLABLE"));
            column.setDefaultValue(rs.getString("COLUMN_DEF"));
            column.setComment(rs.getString("REMARKS"));

            switch (typeName) {
                case "BOOLEAN":
                    column.setColumnType(ColumnType.BOOLEAN);
                    break;
                case "TINYINT":
                case "SMALLINT":
                case "BIT":
                case "INT":
                case "INTEGER":
                case "BIGINT":
                    column.setColumnType(ColumnType.INTEGER);
                    break;
                case "NUMERIC":
                case "DECIMAL":
                    if (scale == 0) {
                        column.setColumnType(ColumnType.INTEGER);
                    } else {
                        column.setColumnType(ColumnType.FLOAT);
                    }
                    break;
                case "DOUBLE PRECISION":
                case "DOUBLE":
                case "FLOAT":
                case "REAL":
                    column.setColumnType(ColumnType.FLOAT);
                    break;
                case "CHAR":
                case "CHARACTER":
                    column.setColumnType(ColumnType.CHAR);
                    break;
                case "VARCHAR":
                case "CHARACTER VARYING":
                    column.setColumnType(ColumnType.STRING);
                    break;
                case "DATE":
                    column.setColumnType(ColumnType.DATE);
                    break;
                case "TIME":
                    column.setColumnType(ColumnType.TIME);
                    break;
                case "DATETIME":
                case "TIMESTAMP":
                    column.setColumnType(ColumnType.TIMESTAMP);
                    break;
                case "CLOB":
                case "CHARACTER LARGE OBJECT":
                    column.setColumnType(ColumnType.CLOB);
                    break;
                case "BLOB":
                case "BINARY":
                case "VARBINARY":
                case "BINARY LARGE OBJECT":
                    column.setColumnType(ColumnType.BLOB);
                    break;
                default:
                    throw new SchemaException("Unsupported data type '" + typeName + "'");
            }

            table.addColumn(column);
        });
    }

    private void findConstraints(Table table) {
        notify("Find constraints for table " + table.getName());
        select(schemaQueries.selectConstraints(table), rs -> {
            String constraintType = rs.getString("CONSTRAINT_TYPE").trim();
            switch (constraintType) {
                case "PRIMARY KEY":
                    PrimaryKey primaryKey = new PrimaryKey();
                    primaryKey.setTable(table);
                    primaryKey.setName(rs.getString("CONSTRAINT_NAME"));
                    primaryKey.setColumns(StringUtils.splitColumns(rs.getString("COLUMN_NAME")));
                    primaryKey.setEnabled(true);
                    table.setPrimaryKey(primaryKey);
                    break;
                case "FOREIGN KEY":
                    ImportedKey importedKey = new ImportedKey();
                    importedKey.setTable(table);
                    importedKey.setName(rs.getString("CONSTRAINT_NAME"));
                    importedKey.setColumns(StringUtils.splitColumns(rs.getString("COLUMN_NAME")));
                    importedKey.setEnabled(true);
                    importedKey.setPkTableSchema(rs.getString("R_TABLE_SCHEM"));
                    importedKey.setPkTableName(rs.getString("R_TABLE_NAME"));
                    importedKey.setPkName(rs.getString("R_CONSTRAINT_NAME"));
                    importedKey.setPkColumns(StringUtils.splitColumns(rs.getString("R_COLUMN_NAME")));
                    table.addImportedKey(importedKey);
                    break;
                case "CHECK CONSTRAINT":
                    CheckConstraint checkConstraint = new CheckConstraint();
                    checkConstraint.setTable(table);
                    checkConstraint.setName(rs.getString("CONSTRAINT_NAME"));
                    checkConstraint.setColumns(StringUtils.splitColumns(rs.getString("COLUMN_NAME")));
                    checkConstraint.setEnabled(true);
                    checkConstraint.setCondition(rs.getString("CONDITION"));
                    table.addCheckConstraint(checkConstraint);
                    break;
                case "EXPORTED KEY":
                    ExportedKey exportedKey = new ExportedKey();
                    exportedKey.setTable(table);
                    exportedKey.setName(rs.getString("R_CONSTRAINT_NAME"));
                    exportedKey.setColumns(StringUtils.splitColumns(rs.getString("R_COLUMN_NAME")));
                    exportedKey.setEnabled(true);
                    exportedKey.setFkTableSchema(rs.getString("TABLE_SCHEMA"));
                    exportedKey.setFkTableName(rs.getString("TABLE_NAME"));
                    exportedKey.setFkName(rs.getString("CONSTRAINT_NAME"));
                    exportedKey.setFkColumns(StringUtils.splitColumns(rs.getString("COLUMN_NAME")));
                    table.addExportedKey(exportedKey);
                    break;
            }
        });
    }

    private void findIndices(Table table) {
        notify("Find indices for table " + table.getName());
        select(schemaQueries.selectIndices(table), rs -> {
            Index index = new Index();
            index.setTable(table);
            index.setName(rs.getString("INDEX_NAME"));
            index.setType(rs.getBoolean("NON_UNIQUE") ? IndexType.NORMAL : IndexType.UNIQUE);
            index.setEnabled(true);
            index.setColumns(StringUtils.splitColumns(rs.getString("COLUMNS")));
            table.addIndex(index);
        });
    }
}
