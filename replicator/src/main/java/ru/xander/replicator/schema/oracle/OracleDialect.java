package ru.xander.replicator.schema.oracle;

import ru.xander.replicator.schema.AbstractDialect;
import ru.xander.replicator.schema.CheckConstraint;
import ru.xander.replicator.schema.Column;
import ru.xander.replicator.schema.ColumnDiff;
import ru.xander.replicator.schema.Constraint;
import ru.xander.replicator.schema.ImportedKey;
import ru.xander.replicator.schema.Index;
import ru.xander.replicator.schema.IndexType;
import ru.xander.replicator.schema.PrimaryKey;
import ru.xander.replicator.schema.SchemaUtils;
import ru.xander.replicator.schema.Sequence;
import ru.xander.replicator.schema.Table;
import ru.xander.replicator.schema.Trigger;
import ru.xander.replicator.util.StringUtils;

import java.util.stream.Collectors;

/**
 * @author Alexander Shakhov
 */
class OracleDialect extends AbstractDialect {

    private static final int MAX_NUMBER_SIZE = 38;
    private static final int MAX_NUMBER_SCALE = 127;
    static final int MAX_VARCHAR_SIZE = 2000;

    OracleDialect(String workSchema) {
        super(workSchema);
    }

    @Override
    public String testQuery() {
        return "SELECT 1 FROM DUAL";
    }

    @Override
    public String createTableQuery(Table table) {
        return "CREATE TABLE " + getQualifiedName(table) + '\n' +
                "(\n    " + table
                .getColumns()
                .stream()
                .map(OracleDialect::getColumnDefinition)
                .collect(Collectors.joining(",\n    ")) +
                "\n)";
    }

    @Override
    public String createTableCommentQuery(Table table) {
        if (StringUtils.isEmpty(table.getComment())) {
            return null;
        }
        return "COMMENT ON TABLE " + getQualifiedName(table) + " IS '" + table.getComment() + "'";
    }

    @Override
    public String dropTableQuery(Table table) {
        return "DROP TABLE " + getQualifiedName(table) + " PURGE";
    }

    @Override
    public String createColumnQuery(Column column) {
        return "ALTER TABLE " + getQualifiedName(column.getTable()) + " ADD " + getColumnDefinition(column);
    }

    @Override
    public String modifyColumnQuery(Column column, ColumnDiff... columnDiffs) {
        StringBuilder modify = new StringBuilder();
        if (ColumnDiff.DATATYPE.anyOf(columnDiffs)) {
            modify.append(getDataType(column)).append(' ');
        }
        if (ColumnDiff.DEFAULT.anyOf(columnDiffs)) {
            modify.append("DEFAULT ").append(column.getDefaultValue());
        }
        if (ColumnDiff.MANDATORY.anyOf(columnDiffs)) {
            if (column.isNullable()) {
                modify.append("NULL ");
            } else {
                Table table = column.getTable();
                CheckConstraint checkConstraint = SchemaUtils.getConstraintByColumnName(table.getCheckConstraints(), column.getName());
                if (checkConstraint != null) {
                    modify.append(" CONSTRAINT ").append(checkConstraint.getName()).append(' ');
                }
                modify.append("NOT NULL ");
            }
        }
        if (modify.length() == 0) {
            return null;
        }
        return "ALTER TABLE " + getQualifiedName(column.getTable())
                + " MODIFY " + column.getName() + ' ' + modify.toString().trim();
    }

    @Override
    public String dropColumnQuery(Column column) {
        return "ALTER TABLE " + getQualifiedName(column.getTable()) + " DROP COLUMN " + column.getName();
    }

    @Override
    public String createColumnCommentQuery(Column column) {
        if (StringUtils.isEmpty(column.getComment())) {
            return null;
        }
        return "COMMENT ON COLUMN " + getQualifiedName(column.getTable())
                + '.' + column.getName() + " IS '" + column.getComment() + "'";
    }

    @Override
    public String createPrimaryKeyQuery(PrimaryKey primaryKey) {
        return "ALTER TABLE " + getQualifiedName(primaryKey.getTable())
                + " ADD CONSTRAINT " + primaryKey.getName() + " PRIMARY KEY (" + StringUtils.joinColumns(primaryKey.getColumns()) + ')';
    }

    @Override
    public String dropPrimaryKeyQuery(PrimaryKey primaryKey) {
        return "ALTER TABLE " + getQualifiedName(primaryKey.getTable()) + " DROP PRIMARY KEY";
    }

    @Override
    public String createImportedKeyQuery(ImportedKey importedKey) {
        return "ALTER TABLE " + getQualifiedName(importedKey.getTable())
                + " ADD CONSTRAINT " + importedKey.getName()
                + " FOREIGN KEY (" + StringUtils.joinColumns(importedKey.getColumns()) + ")"
                + " REFERENCES " + workSchema + '.' + importedKey.getPkTableName()
                + " (" + StringUtils.joinColumns(importedKey.getPkColumns()) + ')';
    }

    @Override
    public String createCheckConstraintQuery(CheckConstraint checkConstraint) {
        return "ALTER TABLE " + getQualifiedName(checkConstraint.getTable())
                + " ADD CONSTRAINT " + checkConstraint.getName()
                + " CHECK (" + checkConstraint.getCondition() + ')';
    }

    @Override
    public String dropConstraintQuery(Constraint constraint) {
        return "ALTER TABLE " + getQualifiedName(constraint.getTable()) + " DROP CONSTRAINT " + constraint.getName();
    }

    @Override
    public String toggleConstraintQuery(Constraint constraint, boolean enabled) {
        return "ALTER TABLE " + getQualifiedName(constraint.getTable()) + ' '
                + (enabled ? "ENABLE" : "DISABLE") + " CONSTRAINT " + constraint.getName();
    }

    @Override
    public String renameConstraintQuery(Constraint constraint, String newConstraintName) {
        return "ALTER TABLE " + getQualifiedName(constraint.getTable()) + ' '
                + " RENAME CONSTRAINT " + constraint.getName() + " TO " + newConstraintName;
    }

    @Override
    public String createIndexQuery(Index index) {
        StringBuilder ddl = new StringBuilder();
        ddl.append("CREATE ");
        if (index.getType() == IndexType.BITMAP) {
            ddl.append("BITMAP ");
        } else if (index.getType() == IndexType.UNIQUE) {
            ddl.append("UNIQUE ");
        }
        ddl.append("INDEX ")
                .append(getQualifiedName(index)).append(" ON ")
                .append(getQualifiedName(index.getTable()))
                .append(" (").append(StringUtils.joinColumns(index.getColumns())).append(")");
        return ddl.toString();
    }

    @Override
    public String dropIndexQuery(Index index) {
        return "DROP INDEX " + getQualifiedName(index);
    }

    @Override
    public String toggleIndexQuery(Index index, boolean enabled) {
        if (enabled) {
            return "ALTER INDEX " + getQualifiedName(index) + " REBUILD";
        } else {
            return "ALTER INDEX " + getQualifiedName(index) + " UNUSABLE";
        }
    }

    @Override
    public String createTriggerQuery(Trigger trigger) {
        return trigger.getBody();
    }

    @Override
    public String dropTriggerQuery(Trigger trigger) {
        return "DROP TRIGGER " + getQualifiedName(trigger);
    }

    @Override
    public String toggleTriggerQuery(Trigger trigger, boolean enabled) {
        return "ALTER TRIGGER " + getQualifiedName(trigger) + ' ' + (enabled ? "ENABLE" : "DISABLE");
    }

    @Override
    public String createSequenceQuery(Sequence sequence) {
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE SEQUENCE ").append(getQualifiedName(sequence)).append('\n');
        if (sequence.getStartWith() == null) {
            sql.append("START WITH 1\n");
        } else {
            sql.append("START WITH ").append(sequence.getStartWith()).append("\n");
        }
        if (sequence.getIncrementBy() == null) {
            sql.append("INCREMENT BY 1\n");
        } else {
            sql.append("INCREMENT BY ").append(sequence.getIncrementBy()).append("\n");
        }
        if (sequence.getMinValue() != null) {
            sql.append("MINVALUE ").append(sequence.getMinValue()).append('\n');
        }
        if (sequence.getMaxValue() != null) {
            sql.append("MAXVALUE ").append(sequence.getMaxValue()).append('\n');
        }
        if ((sequence.getCacheSize() != null) && (sequence.getCacheSize().intValue() > 0)) {
            sql.append("CACHE ").append(sequence.getCacheSize()).append("\n");
        }/* else {
            sql.append("NOCACHE\n");
        }*/
        if (Boolean.TRUE.equals(sequence.getCycle())) {
            sql.append("CYCLE");
        } else {
            sql.append("NOCYCLE");
        }
        return sql.toString().trim();
    }

    @Override
    public String dropSequenceQuery(Sequence sequence) {
        return "DROP SEQUENCE " + getQualifiedName(sequence);
    }

    @Override
    public String analyzeTableQuery(Table table) {
        return "BEGIN\n" +
                "  SYS.DBMS_STATS.GATHER_TABLE_STATS\n" +
                "  (\n" +
                "      OWNNAME => '" + workSchema + "',\n" +
                "      TABNAME => '" + table.getName() + "',\n" +
                "      ESTIMATE_PERCENT => SYS.DBMS_STATS.AUTO_SAMPLE_SIZE,\n" +
                "      METHOD_OPT => 'FOR ALL COLUMNS SIZE AUTO'\n" +
                "  );\n" +
                "END;";
    }

    @Override
    public String selectQuery(Table table) {
        return "SELECT " +
                table.getColumns()
                        .stream()
                        .sorted()
                        .map(Column::getName)
                        .collect(Collectors.joining(",\n")) + '\n' +
                "FROM " + getQualifiedName(table);
    }

    private static String getColumnDefinition(Column column) {
        StringBuilder definition = new StringBuilder();
        definition.append(column.getName()).append(' ').append(getDataType(column));
        if (column.getDefaultValue() != null) {
            definition.append(" DEFAULT ").append(column.getDefaultValue().trim());
        }
        if (!column.isNullable()) {
            Table table = column.getTable();
            CheckConstraint checkConstraint = SchemaUtils.getConstraintByColumnName(table.getCheckConstraints(), column.getName());
            if (checkConstraint != null) {
                definition.append(" CONSTRAINT ").append(checkConstraint.getName());
            }
            definition.append(" NOT NULL");
        }
        return definition.toString();
    }

    private static String getDataType(Column column) {
        final String dataTypeName = column.getColumnType().toOracle();
        switch (column.getColumnType()) {
            case BOOLEAN:
                return dataTypeName + "(1)";
            case INTEGER:
                if (column.getSize() <= 0) {
                    return dataTypeName;
                }
                return dataTypeName + "(" + Math.min(column.getSize(), MAX_NUMBER_SIZE) + ")";
            case CHAR:
                if (column.getSize() > MAX_VARCHAR_SIZE) {
                    return "CLOB";
                }
                return dataTypeName + "(" + column.getSize() + ")";
            case FLOAT:
                return dataTypeName + "(" + Math.min(column.getSize(), MAX_NUMBER_SIZE) + ", " + Math.min(column.getScale(), MAX_NUMBER_SCALE) + ")";
            case STRING:
                if (column.getSize() > MAX_VARCHAR_SIZE) {
                    return "CLOB";
                }
                return dataTypeName + "(" + column.getSize() + " CHAR)";
            case TIMESTAMP:
                return dataTypeName + "(" + column.getScale() + ")";
            default:
                return dataTypeName;
        }
    }
}
