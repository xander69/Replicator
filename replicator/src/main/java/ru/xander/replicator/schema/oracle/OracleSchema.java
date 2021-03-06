package ru.xander.replicator.schema.oracle;

import ru.xander.replicator.filter.Filter;
import ru.xander.replicator.listener.AlterType;
import ru.xander.replicator.listener.ModifyType;
import ru.xander.replicator.schema.AbstractSchema;
import ru.xander.replicator.schema.CheckConstraint;
import ru.xander.replicator.schema.Column;
import ru.xander.replicator.schema.ColumnDiff;
import ru.xander.replicator.schema.ColumnType;
import ru.xander.replicator.schema.Constraint;
import ru.xander.replicator.schema.DataFormatter;
import ru.xander.replicator.schema.Dialect;
import ru.xander.replicator.schema.ExportedKey;
import ru.xander.replicator.schema.ImportedKey;
import ru.xander.replicator.schema.Index;
import ru.xander.replicator.schema.PrimaryKey;
import ru.xander.replicator.schema.SchemaConfig;
import ru.xander.replicator.schema.SchemaUtils;
import ru.xander.replicator.schema.Sequence;
import ru.xander.replicator.schema.Table;
import ru.xander.replicator.schema.Trigger;
import ru.xander.replicator.schema.VendorType;
import ru.xander.replicator.util.StringUtils;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import static ru.xander.replicator.listener.AlterType.*;

/**
 * @author Alexander Shakhov
 */
public class OracleSchema extends AbstractSchema {

    private final OracleDialect dialect;
    private final OracleSchemaQueries schemaQueries;

    public OracleSchema(SchemaConfig config) {
        super(config);
        this.dialect = new OracleDialect(workSchema);
        this.schemaQueries = new OracleSchemaQueries(workSchema);
    }

    @Override
    public VendorType getVendorType() {
        return VendorType.ORACLE;
    }

    @Override
    public Dialect getDialect() {
        return dialect;
    }

    @Override
    public DataFormatter getDataFormatter() {
        return new OracleDataFormatter();
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
        findTriggers(table);
        findSequence(table);
        return table;
    }

    @Override
    public void createTable(Table table) {
        String sql = dialect.createTableQuery(table);
        alter(CREATE_TABLE, table.getName(), sql);
        execute(sql);
    }

    @Override
    public void dropTable(Table table) {
        String sql = dialect.dropTableQuery(table);
        alter(DROP_TABLE, table.getName(), sql);
        execute(sql);
    }

    @Override
    public void renameTable(Table table, String newName) {
        String sql = dialect.renameTableQuery(table, newName);
        alter(RENAME_TABLE, table.getName(), sql);
        execute(sql);
    }

    @Override
    public void createTableComment(Table table) {
        String sql = dialect.createTableCommentQuery(table);
        alter(CREATE_TABLE_COMMENT, table.getName(), sql);
        execute(sql);
    }

    @Override
    public void createColumn(Column column) {
        String sql = dialect.createColumnQuery(column);
        alter(CREATE_COLUMN, column.getTable().getName(), column.getName(), sql);
        execute(sql);
    }

    @Override
    public void modifyColumn(Column oldColumn, Column newColumn) {
        ColumnDiff[] columnDiffs = SchemaUtils.compareColumns(oldColumn, newColumn);
        if (columnDiffs.length > 0) {
            if (oldColumn.isNullable() && !newColumn.isNullable()) {
                Table table = oldColumn.getTable();
                CheckConstraint checkConstraint = SchemaUtils.getConstraintByColumnName(table.getCheckConstraints(), oldColumn.getName());
                if (checkConstraint != null) {
                    dropConstraint(checkConstraint);
                }
            }
            if (ColumnDiff.DATATYPE.anyOf(columnDiffs)) {

                String copyColumName = newColumn.getName() + '$';
                Column copyColumn = newColumn.copy();
                copyColumn.setName(copyColumName);

                createColumn(copyColumn);

                String updateCopyColumn = dialect.updateColumnQuery(copyColumn, oldColumn.getName());
//                modify(ModifyType.UPDATE, copyColumn.getTable().getName(), updateCopyColumn);
                int affectedRows = update(updateCopyColumn);
                modify(ModifyType.UPDATE, copyColumn.getTable().getName(), updateCopyColumn, affectedRows);

                dropColumn(oldColumn);

                renameColumn(copyColumn, newColumn.getName());

            } else {
                String sql = dialect.modifyColumnQuery(newColumn, columnDiffs);
                alter(MODIFY_COLUMN, newColumn.getTable().getName(), newColumn.getName(), Arrays.toString(columnDiffs), sql);
                execute(sql);
            }
        }
    }

    @Override
    public void dropColumn(Column column) {
        String sql = dialect.dropColumnQuery(column);
        alter(DROP_COLUMN, column.getTable().getName(), column.getName(), sql);
        execute(sql);
    }

    @Override
    public void renameColumn(Column column, String newName) {
        String sql = dialect.renameColumnQuery(column, newName);
        alter(RENAME_COLUMN, column.getTable().getName(), column.getName(), sql);
        execute(sql);
    }

    @Override
    public void createColumnComment(Column column) {
        String sql = dialect.createColumnCommentQuery(column);
        if (sql != null) {
            alter(CREATE_COLUMN_COMMENT, column.getTable().getName(), column.getName(), sql);
            execute(sql);
        }
    }

    @Override
    public void createPrimaryKey(PrimaryKey primaryKey) {
        String sql = dialect.createPrimaryKeyQuery(primaryKey);
        alter(CREATE_PRIMARY_KEY, primaryKey.getTable().getName(), primaryKey.getName(), sql);
        execute(sql);
    }

    @Override
    public void dropPrimaryKey(PrimaryKey primaryKey) {
        String sql = dialect.dropPrimaryKeyQuery(primaryKey);
        alter(DROP_PRIMARY_KEY, primaryKey.getTable().getName(), primaryKey.getName(), sql);
        execute(sql);
    }

    @Override
    public void createImportedKey(ImportedKey importedKey) {
        String sql = dialect.createImportedKeyQuery(importedKey);
        alter(CREATE_IMPORTED_KEY, importedKey.getTable().getName(), importedKey.getName(), sql);
        execute(sql);
    }

    @Override
    public void createCheckConstraint(CheckConstraint checkConstraint) {
        if (!isObjectExists(checkConstraint.getName(), "CONSTRAINT")) {
            String sql = dialect.createCheckConstraintQuery(checkConstraint);
            alter(CREATE_CHECK_CONSTRAINT, checkConstraint.getTable().getName(), checkConstraint.getName(), sql);
            execute(sql, true);
        }
    }

    @Override
    public void dropConstraint(Constraint constraint) {
        String sql = dialect.dropConstraintQuery(constraint);
        alter(DROP_CONSTRAINT, constraint.getTable().getName(), constraint.getName(), sql);
        execute(sql, true);
    }

    @Override
    public void toggleConstraint(Constraint constraint, boolean enabled) {
        String sql = dialect.toggleConstraintQuery(constraint, enabled);
        AlterType alterType = enabled ? ENABLE_CONSTRAINT : DISABLE_CONSTRAINT;
        alter(alterType, constraint.getTable().getName(), constraint.getName(), sql);
        execute(sql);
    }

    @Override
    public void createIndex(Index index) {
        String sql = dialect.createIndexQuery(index);
        alter(CREATE_INDEX, index.getTable().getName(), index.getName(), sql);
        execute(sql);
    }

    @Override
    public void dropIndex(Index index) {
        String sql = dialect.dropIndexQuery(index);
        alter(DROP_INDEX, index.getTable().getName(), index.getName(), sql);
        execute(sql);
    }

    @Override
    public void toggleIndex(Index index, boolean enabled) {
        String sql = dialect.toggleIndexQuery(index, enabled);
        AlterType alterType = enabled ? ENABLE_INDEX : DISABLE_INDEX;
        alter(alterType, index.getTable().getName(), index.getName(), sql);
        execute(sql);
    }

    @Override
    public void createTrigger(Trigger trigger) {
        if (trigger.getVendorType() != VendorType.ORACLE) {
            warning("Vendor type for " + trigger.getName() + " is not Oracle");
            return;
        }
        if (isObjectExists(trigger.getName(), "TRIGGER")) {
            warning("Trigger " + trigger.getName() + " already exists");
            return;
        }
        String sql = dialect.createTriggerQuery(trigger);
        alter(CREATE_TRIGGER, trigger.getTable().getName(), trigger.getName(), sql);
        execute(sql);
    }

    @Override
    public void dropTrigger(Trigger trigger) {
        String sql = dialect.dropTriggerQuery(trigger);
        alter(DROP_TRIGGER, trigger.getTable().getName(), trigger.getName(), sql);
        execute(sql);
    }

    @Override
    public void toggleTrigger(Trigger trigger, boolean enabled) {
        String sql = dialect.toggleTriggerQuery(trigger, enabled);
        AlterType alterType = enabled ? ENABLE_TRIGGER : DISABLE_TRIGGER;
        alter(alterType, trigger.getTable().getName(), trigger.getName(), sql);
        execute(sql);
    }

    @Override
    public void createSequence(Sequence sequence) {
        if (isObjectExists(sequence.getName(), "SEQUENCE")) {
            warning("Sequence " + sequence.getName() + " already exists");
            return;
        }
        String sql = dialect.createSequenceQuery(sequence);
        alter(CREATE_SEQUENCE, sequence.getTable().getName(), sequence.getName(), sql);
        execute(sql);
    }

    @Override
    public void dropSequence(Sequence sequence) {
        String sql = dialect.dropSequenceQuery(sequence);
        alter(DROP_SEQUENCE, sequence.getTable().getName(), sequence.getName(), sql);
        execute(sql);
    }

    @Override
    public void analyzeTable(Table table) {
        String sql = dialect.analyzeTableQuery(table);
        alter(ANALYZE_TABLE, table.getName(), sql);
        execute(sql);
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
                    table.setSchema(rs.getString("owner"));
                    table.setName(rs.getString("table_name"));
                    table.setComment(rs.getString("comments"));
                    return table;
                });
    }

    private void findColumns(Table table) {
        notify("Find columns for table " + table.getName());
        select(schemaQueries.selectColumns(table), rs -> {
//            int dataLength = rs.getInt("data_length");
            int dataPrecision = rs.getInt("data_precision");
            int dataScale = rs.getInt("data_scale");
            int charLength = rs.getInt("char_length");
            ColumnType columnType = OracleType.toColumnType(rs.getString("data_type"), dataScale);

            Column column = new Column();
            column.setTable(table);
            column.setNumber(rs.getInt("column_id"));
            column.setName(rs.getString("column_name"));
            column.setColumnType(columnType);
            column.setNullable("Y".equalsIgnoreCase(rs.getString("nullable")));
            column.setDefaultValue(rs.getString("data_default"));
            column.setComment(rs.getString("comments"));

            switch (columnType) {
                case INTEGER:
                    column.setSize(dataPrecision);
                    break;
                case FLOAT:
                    column.setSize(dataPrecision);
                    column.setScale(dataScale);
                    break;
                case CHAR:
                case STRING:
                    column.setSize(charLength);
                    break;
                case TIMESTAMP:
                    column.setScale(dataScale);
                    break;
            }

            table.addColumn(column);
        });
    }

    private void findConstraints(Table table) {
        notify("Find constraints for table " + table.getName());
        select(schemaQueries.selectConstraints(table), rs -> {
            String constraintType = rs.getString("constraint_type");
            switch (constraintType) {
                case "P":
                    PrimaryKey primaryKey = new PrimaryKey();
                    primaryKey.setTable(table);
                    primaryKey.setName(rs.getString("constraint_name"));
                    primaryKey.setColumns(StringUtils.splitColumns(rs.getString("column_name")));
                    primaryKey.setEnabled("ENABLED".equals(rs.getString("status")));
                    table.setPrimaryKey(primaryKey);
                    break;
                case "R":
                    ImportedKey importedKey = new ImportedKey();
                    importedKey.setTable(table);
                    importedKey.setName(rs.getString("constraint_name"));
                    importedKey.setColumns(StringUtils.splitColumns(rs.getString("column_name")));
                    importedKey.setEnabled("ENABLED".equals(rs.getString("status")));
                    importedKey.setPkTableSchema(rs.getString("r_owner"));
                    importedKey.setPkTableName(rs.getString("r_table_name"));
                    importedKey.setPkName(rs.getString("r_constraint_name"));
                    importedKey.setPkColumns(StringUtils.splitColumns(rs.getString("r_column_name")));
                    table.addImportedKey(importedKey);
                    break;
                case "C":
                    CheckConstraint checkConstraint = new CheckConstraint();
                    checkConstraint.setTable(table);
                    checkConstraint.setName(rs.getString("constraint_name"));
                    checkConstraint.setColumns(StringUtils.splitColumns(rs.getString("column_name")));
                    checkConstraint.setEnabled("ENABLED".equals(rs.getString("status")));
                    checkConstraint.setCondition(rs.getString("search_condition"));
                    table.addCheckConstraint(checkConstraint);
                    break;
                case "D":
                    ExportedKey exportedKey = new ExportedKey();
                    exportedKey.setTable(table);
                    exportedKey.setName(rs.getString("r_constraint_name"));
                    exportedKey.setColumns(StringUtils.splitColumns(rs.getString("r_column_name")));
                    exportedKey.setEnabled("ENABLED".equals(rs.getString("status")));
                    exportedKey.setFkTableSchema(rs.getString("owner"));
                    exportedKey.setFkTableName(rs.getString("table_name"));
                    exportedKey.setFkName(rs.getString("constraint_name"));
                    exportedKey.setFkColumns(StringUtils.splitColumns(rs.getString("column_name")));
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
            index.setName(rs.getString("index_name"));
            index.setType(OracleType.toIndexType(rs.getString("index_type")));
            index.setEnabled("VALID".equals(rs.getString("status")));
            index.setColumns(StringUtils.splitColumns(rs.getString("columns")));
            table.addIndex(index);
        });
    }

    private void findTriggers(Table table) {
        notify("Find trigger for table " + table.getName());
        select(schemaQueries.selectTriggers(table), rs -> {
            String description = rs.getString("description").trim();
            String whenClause = rs.getString("when_clause");
            String triggerBody = rs.getString("trigger_body").trim();
            boolean enabled = "ENABLED".equals(rs.getString("status"));

            Trigger trigger = new Trigger();
            trigger.setTable(table);
            trigger.setName(rs.getString("trigger_name"));

            List<OracleTriggerDependency> dependencies = new ArrayList<>();
            select(schemaQueries.selectTriggerDependencies(trigger), rsDeps -> {
                OracleTriggerDependency dependency = new OracleTriggerDependency();
                dependency.setSchema(rsDeps.getString("REFERENCED_OWNER"));
                dependency.setName(rsDeps.getString("REFERENCED_NAME"));
                dependency.setType(rsDeps.getString("REFERENCED_TYPE"));
                dependencies.add(dependency);
            });

            String body = prepareTriggerBody(dependencies, description, whenClause, triggerBody);
            trigger.setBody("CREATE OR REPLACE TRIGGER " + body);
            trigger.setEnabled(enabled);
            trigger.setVendorType(VendorType.ORACLE);
            table.addTrigger(trigger);
        });
    }

    private void findSequence(Table table) {
        notify("Find sequence for table " + table.getName());
        table.setSequence(selectOne(schemaQueries.selectSequence(table), rs -> {
            Sequence sequence = new Sequence();
            sequence.setTable(table);
            sequence.setSchema(rs.getString("SEQUENCE_OWNER"));
            sequence.setName(rs.getString("SEQUENCE_NAME"));
            sequence.setStartWith(new BigInteger(rs.getString("LAST_NUMBER")));
            sequence.setIncrementBy(new BigInteger(rs.getString("INCREMENT_BY")));
            sequence.setMinValue(new BigInteger(rs.getString("MIN_VALUE")));
            sequence.setMaxValue(new BigInteger(rs.getString("MAX_VALUE")));
            sequence.setCacheSize(new BigInteger(rs.getString("CACHE_SIZE")));
            sequence.setCycle("Y".equals(rs.getString("CYCLE_FLAG")));
            return sequence;
        }));
    }

    private boolean isObjectExists(String objectName, String objectType) {
        Boolean exists;
        if (objectType.equalsIgnoreCase("CONSTRAINT")) {
            exists = selectOne(schemaQueries.selectConstraint(objectName), rs -> true);
        } else {
            exists = selectOne(schemaQueries.selectObject(objectName, objectType), rs -> true);
        }
        return (exists != null);
    }

    private String prepareTriggerBody(List<OracleTriggerDependency> dependencies, String description, String whenClause, String body) {
        String upperedDescr = description.toUpperCase();
//        String upperedBody = body.toUpperCase();
        for (OracleTriggerDependency dependency : dependencies) {
            int descrIndex = upperedDescr.indexOf(dependency.getName());
            if (descrIndex > 0) {
                if (upperedDescr.charAt(descrIndex - 1) != '.') {
                    upperedDescr = upperedDescr.substring(0, descrIndex) + workSchema + '.' + upperedDescr.substring(descrIndex);
                    description = description.substring(0, descrIndex) + workSchema + '.' + description.substring(descrIndex);
                }
            }
//            int bodyIndex = upperedBody.indexOf(dependency.getName());
//            if (bodyIndex > 0) {
//                if (upperedBody.charAt(bodyIndex - 1) != '.') {
//                    upperedBody = upperedBody.substring(0, bodyIndex) + workSchema + '.' + upperedBody.substring(bodyIndex);
//                    body = body.substring(0, bodyIndex) + workSchema + '.' + body.substring(bodyIndex);
//                }
//            }
        }
        StringBuilder triggerBody = new StringBuilder();
        if (!upperedDescr.startsWith(workSchema)) {
            triggerBody.append(workSchema).append('.');
        }
        triggerBody.append(description);
        if (!StringUtils.isEmpty(whenClause)) {
            triggerBody.append(" WHEN (").append(whenClause.trim()).append(')');
        }
        triggerBody.append('\n').append(body);
        return triggerBody.toString();
    }
}
