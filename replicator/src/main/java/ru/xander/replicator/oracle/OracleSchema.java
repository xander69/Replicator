package ru.xander.replicator.oracle;

import ru.xander.replicator.AbstractSchema;
import ru.xander.replicator.SchemaOptions;
import ru.xander.replicator.schema.CheckConstraint;
import ru.xander.replicator.schema.Column;
import ru.xander.replicator.schema.ColumnType;
import ru.xander.replicator.schema.Constraint;
import ru.xander.replicator.schema.ExportedKey;
import ru.xander.replicator.schema.Index;
import ru.xander.replicator.schema.ModifyType;
import ru.xander.replicator.schema.PrimaryKey;
import ru.xander.replicator.schema.ImportedKey;
import ru.xander.replicator.schema.Sequence;
import ru.xander.replicator.schema.Table;
import ru.xander.replicator.schema.Trigger;

import java.util.Arrays;

public class OracleSchema extends AbstractSchema {

    private final String workSchema;
    private final OracleDialect dialect;

    public OracleSchema(SchemaOptions options) {
        super(options);
        this.workSchema = options.getWorkSchema();
        this.dialect = new OracleDialect();
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
        execute(dialect.createTableQuery(table));
    }

    @Override
    public void dropTable(Table table) {
        execute(dialect.dropTableQuery(table));
    }

    @Override
    public void createTableComment(Table table) {
        execute(dialect.createTableCommentQuery(table));
    }

    @Override
    public void createColumn(Column column) {
        execute(dialect.createColumnQuery(column));
    }

    @Override
    public void modifyColumn(Column column, ModifyType... modifyTypes) {
        String ddl = dialect.modifyColumnQuery(column, modifyTypes);
        if (ddl != null) {
            execute(ddl);
        }
    }

    @Override
    public void dropColumn(Column column) {
        execute(dialect.dropColumnQuery(column));
    }

    @Override
    public void createColumnComment(Column column) {
        execute(dialect.createColumnCommentQuery(column));
    }

    @Override
    public void createPrimaryKey(PrimaryKey primaryKey) {
        execute(dialect.createPrimaryKeyQuery(primaryKey));
    }

    @Override
    public void dropPrimaryKey(PrimaryKey primaryKey) {
        execute(dialect.dropPrimaryKeyQuery(primaryKey));
    }

    @Override
    public void createImportedKey(ImportedKey importedKey) {
        execute(dialect.createImportedKeyQuery(importedKey));
    }

    @Override
    public void createCheckConstraint(CheckConstraint checkConstraint) {
        execute(dialect.createCheckConstraintQuery(checkConstraint));
    }

    @Override
    public void dropConstraint(Constraint constraint) {
        execute(dialect.dropConstraintQuery(constraint));
    }

    @Override
    public void toggleConstraint(Constraint constraint, boolean enabled) {
        execute(dialect.toggleConstraintQuery(constraint, enabled));
    }

    @Override
    public void createIndex(Index index) {
        execute(dialect.createIndexQuery(index));
    }

    @Override
    public void dropIndex(Index index) {
        execute(dialect.dropIndexQuery(index));
    }

    @Override
    public void toggleIndex(Index index, boolean enabled) {
        execute(dialect.toggleIndexQuery(index, enabled));
    }

    @Override
    public void createTrigger(Trigger trigger) {
        execute(dialect.createTriggerQuery(trigger));
    }

    @Override
    public void dropTrigger(Trigger trigger) {
        execute(dialect.dropTriggerQuery(trigger));
    }

    @Override
    public void toggleTrigger(Trigger trigger, boolean enabled) {
        execute(dialect.toggleTriggerQuery(trigger, enabled));
    }

    @Override
    public void createSequence(Sequence sequence) {
        execute(dialect.createSequenceQuery(sequence));
    }

    @Override
    public void dropSequence(Sequence sequence) {
        execute(dialect.dropSequenceQuery(sequence));
    }

    @Override
    public void analyzeTable(Table table) {
        execute(dialect.analyzeTableQuery(table));
    }

    private Table findTable(String tableName) {
        return selectOne("select\n" +
                        "  t.owner,\n" +
                        "  t.table_name,\n" +
                        "  tc.comments\n" +
                        "from sys.all_tables t\n" +
                        "  left outer join sys.all_tab_comments tc on\n" +
                        "    t.owner = tc.owner\n" +
                        "    and t.table_name = tc.table_name\n" +
                        "where\n" +
                        "  t.owner = ?\n" +
                        "  and t.table_name = ?", new Object[]{workSchema, tableName},
                rs -> {
                    Table table = new Table();
                    table.setSchema(rs.getString("owner"));
                    table.setName(rs.getString("table_name"));
                    return table;
                });
    }

    private void findColumns(Table table) {
        select("select\n" +
                        "  c.owner,\n" +
                        "  c.table_name,\n" +
                        "  c.column_name,\n" +
                        "  c.data_type,\n" +
                        "  c.data_length,\n" +
                        "  c.data_precision,\n" +
                        "  c.data_scale,\n" +
                        "  c.char_length,\n" +
                        "  c.nullable,\n" +
                        "  c.data_default,\n" +
                        "  cc.comments\n" +
                        "from sys.all_tab_columns c\n" +
                        "  left outer join sys.all_col_comments cc on\n" +
                        "    c.owner = cc.owner\n" +
                        "    and c.table_name = cc.table_name\n" +
                        "    and c.column_name = cc.column_name\n" +
                        "where\n" +
                        "  c.owner = ?\n" +
                        "  and c.table_name = ?\n" +
                        "order by c.column_id", new Object[]{workSchema, table.getName()},
                rs -> {
                    int dataLength = rs.getInt("data_length");
                    int dataPrecision = rs.getInt("data_precision");
                    int dataScale = rs.getInt("data_scale");
                    int charLength = rs.getInt("char_length");
                    ColumnType columnType = OracleType.toColumnType(rs.getString("data_type"), dataScale);

                    Column column = new Column();
                    column.setTable(table);
                    column.setName(rs.getString("column_name"));
                    column.setColumnType(columnType);
                    column.setNullable("N".equalsIgnoreCase(rs.getString("nullable")));
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
        select("select\n" +
                        "  c.owner,\n" +
                        "  c.table_name,\n" +
                        "  c.constraint_name,\n" +
                        "  c.constraint_type,\n" +
                        "  c.status,\n" +
                        "  cc.column_name,\n" +
                        "  cr.owner as r_owner,\n" +
                        "  cr.table_name as r_table_name,\n" +
                        "  cr.constraint_name as r_constraint_name,\n" +
                        "  ccr.column_name as r_column_name,\n" +
                        "  c.search_condition\n" +
                        "from sys.all_constraints c\n" +
                        "  left outer join sys.all_cons_columns cc on\n" +
                        "    c.owner = cc.owner\n" +
                        "    and c.table_name = cc.table_name\n" +
                        "    and c.constraint_name = cc.constraint_name\n" +
                        "  left outer join sys.all_constraints cr on\n" +
                        "    c.r_owner = cr.owner\n" +
                        "    and c.r_constraint_name = cr.constraint_name\n" +
                        "  left outer join sys.all_cons_columns ccr on\n" +
                        "    cr.owner = ccr.owner\n" +
                        "    and cr.table_name = ccr.table_name\n" +
                        "    and cr.constraint_name = ccr.constraint_name\n" +
                        "where c.constraint_type in ('P', 'R', 'C')\n" +
                        "      and c.owner = ?\n" +
                        "      and c.table_name = ?\n" +
                        "union all\n" +
                        "select\n" +
                        "  c.owner,\n" +
                        "  c.table_name,\n" +
                        "  c.constraint_name,\n" +
                        "  'D' as constraint_type,\n" +
                        "  c.status,\n" +
                        "  cc.column_name,\n" +
                        "  cr.owner as r_owner,\n" +
                        "  cr.table_name as r_table_name,\n" +
                        "  cr.constraint_name as r_constraint_name,\n" +
                        "  ccr.column_name as r_column_name,\n" +
                        "  null as search_condition\n" +
                        "from sys.all_constraints c\n" +
                        "  inner join sys.all_constraints cr on\n" +
                        "    c.r_owner = cr.owner\n" +
                        "    and c.r_constraint_name = cr.constraint_name\n" +
                        "  inner join sys.all_cons_columns cc on\n" +
                        "    c.owner = cc.owner\n" +
                        "    and c.table_name = cc.table_name\n" +
                        "    and c.constraint_name = cc.constraint_name\n" +
                        "  inner join sys.all_cons_columns ccr on\n" +
                        "    cr.owner = ccr.owner\n" +
                        "    and cr.table_name = ccr.table_name\n" +
                        "    and cr.constraint_name = ccr.constraint_name\n" +
                        "where cr.owner = ?\n" +
                        "      and cr.table_name = ?", new Object[]{workSchema, table.getName(), workSchema, table.getName()},
                rs -> {
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
                        case "C":
                            CheckConstraint checkConstraint = new CheckConstraint();
                            checkConstraint.setTable(table);
                            checkConstraint.setName(rs.getString("constraint_name"));
                            checkConstraint.setEnabled("ENABLED".equals(rs.getString("status")));
                            checkConstraint.setCondition(rs.getString("search_condition"));
                            table.addCheckConstraint(checkConstraint);
                            break;
                    }
                });
    }

    private void findIndices(Table table) {
        select("select\n" +
                        "  i.owner,\n" +
                        "  i.index_name,\n" +
                        "  i.index_type,\n" +
                        "  i.table_owner,\n" +
                        "  i.table_name,\n" +
                        "  i.tablespace_name,\n" +
                        "  i.status,\n" +
                        "  listagg(ic.column_name, ',') within group (order by ic.column_position) as columns\n" +
                        "from sys.all_indexes i\n" +
                        "  inner join sys.all_ind_columns ic on\n" +
                        "    i.owner = ic.index_owner\n" +
                        "    and i.index_name = ic.index_name\n" +
                        "where\n" +
                        "  i.table_owner = ?\n" +
                        "  and i.table_name = ?\n" +
                        "  and (i.owner, i.index_name) not in\n" +
                        "      (\n" +
                        "        select distinct\n" +
                        "          c.index_owner,\n" +
                        "          c.index_name\n" +
                        "        from sys.all_constraints c\n" +
                        "        where c.owner = ?\n" +
                        "              and c.table_name = ?\n" +
                        "              and c.index_owner is not null\n" +
                        "              and c.index_name is not null\n" +
                        "      )\n" +
                        "group by\n" +
                        "  i.owner,\n" +
                        "  i.index_name,\n" +
                        "  i.index_type,\n" +
                        "  i.table_owner,\n" +
                        "  i.table_name,\n" +
                        "  i.tablespace_name,\n" +
                        "  i.status", new Object[]{workSchema, table.getName(), workSchema, table.getName()},
                rs -> {
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
        select("select t.owner,\n" +
                        "  t.trigger_name,\n" +
                        "  t.trigger_type,\n" +
                        "  t.triggering_event,\n" +
                        "  t.description,\n" +
                        "  t.trigger_body,\n" +
                        "  t.status\n" +
                        "from sys.all_triggers t\n" +
                        "where t.table_owner = ?\n" +
                        "      and t.table_name = ?", new Object[]{workSchema, table.getName()},
                rs -> {
                    Trigger trigger = new Trigger();
                    trigger.setTable(table);
                    trigger.setName(rs.getString("trigger_name"));
                    trigger.setBody("CREATE OR REPLACE TRIGGER " + rs.getString("description") + ' ' + rs.getString("trigger_body"));
                    trigger.setEnabled("ENABLED".equals(rs.getString("status")));
                    table.addTrigger(trigger);
                });
    }

    private void findSequence(Table table) {
        table.setSequence(selectOne("select\n" +
                        "  s.sequence_owner,\n" +
                        "  s.sequence_name,\n" +
                        "  s.min_value,\n" +
                        "  s.max_value,\n" +
                        "  s.increment_by,\n" +
                        "  s.last_number,\n" +
                        "  s.cache_size\n" +
                        "from sys.all_triggers t\n" +
                        "  inner join sys.all_dependencies d on\n" +
                        "    d.owner = t.owner\n" +
                        "    and d.name = t.trigger_name\n" +
                        "  inner join sys.all_sequences s on\n" +
                        "    s.sequence_owner = d.referenced_owner\n" +
                        "    and s.sequence_name = d.referenced_name\n" +
                        "where t.table_owner = ?\n" +
                        "      and t.table_name = ?", new Object[]{workSchema, table.getName()},
                rs -> {
                    Sequence sequence = new Sequence();
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
}
