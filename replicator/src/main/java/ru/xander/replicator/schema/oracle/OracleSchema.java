package ru.xander.replicator.schema.oracle;

import ru.xander.replicator.exception.SchemaException;
import ru.xander.replicator.listener.AlterType;
import ru.xander.replicator.listener.Listener;
import ru.xander.replicator.schema.AbstractSchema;
import ru.xander.replicator.schema.Column;
import ru.xander.replicator.schema.ColumnType;
import ru.xander.replicator.schema.Constraint;
import ru.xander.replicator.schema.Ddl;
import ru.xander.replicator.schema.Dml;
import ru.xander.replicator.schema.ExportedKey;
import ru.xander.replicator.schema.ImportedKey;
import ru.xander.replicator.schema.Index;
import ru.xander.replicator.schema.ModifyType;
import ru.xander.replicator.schema.PrimaryKey;
import ru.xander.replicator.schema.Sequence;
import ru.xander.replicator.schema.Table;
import ru.xander.replicator.schema.Trigger;
import ru.xander.replicator.schema.VendorType;
import ru.xander.replicator.util.StringUtils;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static ru.xander.replicator.listener.AlterType.*;

/**
 * @author Alexander Shakhov
 */
public class OracleSchema extends AbstractSchema {

    //    private final String workSchema;
    private final OracleDialect dialect;

    public OracleSchema(Connection connection, Listener listener, String workSchema) {
        super(connection, listener);
//        this.workSchema = workSchema;
        this.dialect = new OracleDialect(workSchema);
    }

    @Override
    public VendorType getVendorType() {
        return VendorType.ORACLE;
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
        ModifyType[] modifyTypes = compareColumn(oldColumn, newColumn);
        if (modifyTypes.length > 0) {
            String sql = dialect.modifyColumnQuery(newColumn, modifyTypes);
            alter(MODIFY_COLUMN, newColumn.getTable().getName(), newColumn.getName(), Arrays.toString(modifyTypes), sql);
            execute(sql);
        }
    }

    @Override
    public void dropColumn(Column column) {
        String sql = dialect.dropColumnQuery(column);
        alter(DROP_COLUMN, column.getTable().getName(), column.getName(), sql);
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
    public void dropConstraint(Constraint constraint) {
        String sql = dialect.dropConstraintQuery(constraint);
        alter(DROP_CONSTRAINT, constraint.getTable().getName(), constraint.getName(), sql);
        execute(sql);
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

    //TODO: порефачить
    @Override
    public Ddl getDdl(Table table) {
        notify("Get DDL for table " + table.getName());
        Ddl ddl = new Ddl();
        ddl.setTable(dialect.createTableQuery(table));
        if (table.getPrimaryKey() != null) {
            ddl.addConstraints(dialect.createPrimaryKeyQuery(table.getPrimaryKey()));
        }
        table.getImportedKeys().forEach(importedKey -> ddl.addConstraints(dialect.createImportedKeyQuery(importedKey)));
//        table.getCheckConstraints().forEach(checkConstraint -> ddl.addConstraints(dialect.createCheckConstraintQuery(checkConstraint)));
        table.getIndices().forEach(index -> ddl.addIndex(dialect.createIndexQuery(index)));
        table.getTriggers().forEach(trigger -> ddl.addTrigger(dialect.createTriggerQuery(trigger)));
        if (table.getSequence() != null) {
            ddl.setSequence(dialect.createSequenceQuery(table.getSequence()));
        }
        ddl.setAnalyze(dialect.analyzeTableQuery(table));
        return ddl;
    }

    //TODO: порефачить
    @Override
    public Dml getDml(Table table) {
        notify("Get DML for table " + table.getName());
        try {
            String selectQuery = dialect.selectQuery(table);

            long rowsCount = selectOne("select count(*) as cnt from (" + selectQuery + ")", rs -> rs.getLong("cnt"));

            return new Dml(
                    rowsCount,
                    connection.prepareStatement(selectQuery),
                    rs -> {
                        Map<String, Object> row = new HashMap<>();
                        for (Column column : table.getColumns()) {
                            row.put(column.getName(), rs.getObject(column.getName()));
                        }
                        return dialect.insertQuery(table, row);
                    },
                    "COMMIT"
            );
        } catch (Exception e) {
            String errorMessage = "Cannot get dml, cause by: " + e.getMessage();
            throw new SchemaException(errorMessage, e);
        }
    }

    private Table findTable(String tableName) {
        notify("Find table " + tableName);
        return selectOne(dialect.selectTableQuery(tableName),
                rs -> {
                    Table table = new Table();
                    table.setSchema(rs.getString("owner"));
                    table.setName(rs.getString("table_name"));
                    table.setComment(rs.getString("comments"));
                    table.setVendorType(getVendorType());
                    return table;
                });
    }

    private void findColumns(Table table) {
        notify("Find columns for table " + table.getName());
        select(dialect.selectColumnsQuery(table), rs -> {
            int dataLength = rs.getInt("data_length");
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
                case DECIMAL:
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
                case RAW:
                    column.setSize(dataLength);
                    break;
            }

            table.addColumn(column);
        });
    }

    private void findConstraints(Table table) {
        notify("Find constraints for table " + table.getName());
        select(dialect.selectConstraintsQuery(table), rs -> {
            String constraintType = rs.getString("constraint_type");
            switch (constraintType) {
                case "P":
                    PrimaryKey primaryKey = new PrimaryKey();
                    primaryKey.setTable(table);
                    primaryKey.setName(rs.getString("constraint_name"));
                    primaryKey.setEnabled("ENABLED".equals(rs.getString("status")));
                    primaryKey.setColumnName(rs.getString("column_name"));
                    table.setPrimaryKey(primaryKey);
                    break;
                case "R":
                    ImportedKey importedKey = new ImportedKey();
                    importedKey.setTable(table);
                    importedKey.setName(rs.getString("constraint_name"));
                    importedKey.setColumnName(rs.getString("column_name"));
                    importedKey.setEnabled("ENABLED".equals(rs.getString("status")));
                    importedKey.setPkTableSchema(rs.getString("r_owner"));
                    importedKey.setPkTableName(rs.getString("r_table_name"));
                    importedKey.setPkName(rs.getString("r_constraint_name"));
                    importedKey.setPkColumnName(rs.getString("r_column_name"));
                    table.addImportedKey(importedKey);
                    break;
                case "D":
                    ExportedKey exportedKey = new ExportedKey();
                    exportedKey.setTable(table);
                    exportedKey.setName(rs.getString("r_constraint_name"));
                    exportedKey.setColumnName(rs.getString("r_column_name"));
                    exportedKey.setEnabled("ENABLED".equals(rs.getString("status")));
                    exportedKey.setFkTableSchema(rs.getString("owner"));
                    exportedKey.setFkTableName(rs.getString("table_name"));
                    exportedKey.setFkName(rs.getString("constraint_name"));
                    exportedKey.setFkColumnName(rs.getString("column_name"));
                    table.addExportedKey(exportedKey);
                    break;
            }
        });
    }

    private void findIndices(Table table) {
        notify("Find indices for table " + table.getName());
        select(dialect.selectIndicesQuery(table), rs -> {
            Index index = new Index();
            index.setTable(table);
            index.setName(rs.getString("index_name"));
            index.setType(OracleType.toIndexType(rs.getString("index_type")));
            index.setEnabled("VALID".equals(rs.getString("status")));
            index.setColumns(Arrays.asList(rs.getString("columns").split(",")));
            table.addIndex(index);
        });
    }

    private void findTriggers(Table table) {
        notify("Find trigger for table " + table.getName());
        select(dialect.selectTriggersQuery(table), rs -> {
            String description = rs.getString("description").trim();
            String triggerBody = rs.getString("trigger_body").trim();
            boolean enabled = "ENABLED".equals(rs.getString("status"));

            Trigger trigger = new Trigger();
            trigger.setTable(table);
            trigger.setName(rs.getString("trigger_name"));

            List<OracleTriggerDependency> dependencies = new ArrayList<>();
            select(dialect.selectTriggerDependenciesQuery(trigger), rsDeps -> {
                OracleTriggerDependency dependency = new OracleTriggerDependency();
                dependency.setSchema(rsDeps.getString("REFERENCED_OWNER"));
                dependency.setName(rsDeps.getString("REFERENCED_NAME"));
                dependency.setType(rsDeps.getString("REFERENCED_TYPE"));
                dependencies.add(dependency);
            });

            String body = dialect.prepareTriggerBody(dependencies, description, triggerBody);
            trigger.setBody("CREATE OR REPLACE TRIGGER " + body);
            trigger.setEnabled(enabled);
            table.addTrigger(trigger);
        });
    }

    private void findSequence(Table table) {
        notify("Find sequence for table " + table.getName());
        table.setSequence(selectOne(dialect.selectSequenceQuery(table), rs -> {
            Sequence sequence = new Sequence();
            sequence.setTable(table);
            sequence.setSchema(rs.getString("sequence_owner"));
            sequence.setName(rs.getString("sequence_name"));
            sequence.setMinValue(rs.getString("min_value"));
            sequence.setMaxValue(rs.getString("max_value"));
            sequence.setIncrementBy(rs.getLong("increment_by"));
            sequence.setLastNumber(rs.getLong("last_number"));
            sequence.setCacheSize(rs.getLong("cache_size"));
            return sequence;
        }));
    }

    private ModifyType[] compareColumn(Column oldColumn, Column newColumn) {
        Set<ModifyType> modifyTypes = new HashSet<>();
        if (oldColumn.getColumnType() != newColumn.getColumnType()) {
            modifyTypes.add(ModifyType.DATATYPE);
        }
        if (oldColumn.getSize() != newColumn.getSize()) {
            modifyTypes.add(ModifyType.DATATYPE);
        }
        if (oldColumn.getScale() != newColumn.getScale()) {
            modifyTypes.add(ModifyType.DATATYPE);
        }
        if (!StringUtils.equalsStringIgnoreWhiteSpace(oldColumn.getDefaultValue(), newColumn.getDefaultValue())) {
            modifyTypes.add(ModifyType.DEFAULT);
        }
        if (newColumn.isNullable() != oldColumn.isNullable()) {
            modifyTypes.add(ModifyType.MANDATORY);
        }
        return modifyTypes.toArray(new ModifyType[0]);
    }

    private boolean isObjectExists(String objectName, String objectType) {
        Boolean exists = selectOne(dialect.selectObjectQuery(objectName, objectType), rs -> true);
        return (exists != null);
    }
}