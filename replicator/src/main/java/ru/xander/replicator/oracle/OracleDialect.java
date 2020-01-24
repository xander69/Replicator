package ru.xander.replicator.oracle;

import ru.xander.replicator.exception.SchemaException;
import ru.xander.replicator.schema.CheckConstraint;
import ru.xander.replicator.schema.Column;
import ru.xander.replicator.schema.ColumnType;
import ru.xander.replicator.schema.Constraint;
import ru.xander.replicator.schema.ImportedKey;
import ru.xander.replicator.schema.Index;
import ru.xander.replicator.schema.IndexType;
import ru.xander.replicator.schema.ModifyType;
import ru.xander.replicator.schema.PrimaryKey;
import ru.xander.replicator.schema.Sequence;
import ru.xander.replicator.schema.Table;
import ru.xander.replicator.schema.Trigger;
import ru.xander.replicator.util.StringUtils;

import java.io.IOException;
import java.io.Reader;
import java.sql.Clob;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.stream.Collectors;

class OracleDialect {

    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static final SimpleDateFormat timestampFormat = new SimpleDateFormat("yyyy-MM.dd HH:mm:ss.SSS");

    String createTableQuery(Table table) {
        return "CREATE TABLE " + getQualifiedName(table) + '\n' +
                "(\n    " +
                table.getColumns().stream()
                        .map(OracleDialect::getColumnDefinition)
                        .collect(Collectors.joining(",\n    ")) +
                "\n)";
    }

    String createTableCommentQuery(Table table) {
        if (StringUtils.isEmpty(table.getComment())) {
            return null;
        }
        return "COMMENT ON TABLE " + getQualifiedName(table) + " IS '" + table.getComment() + "'";
    }

    String dropTableQuery(Table table) {
        return "DROP TABLE " + getQualifiedName(table) + " PURGE";
    }

    String createColumnQuery(Column column) {
        return "ALTER TABLE " + getQualifiedName(column.getTable()) + " ADD " + getColumnDefinition(column);
    }

    String modifyColumnQuery(Column column, ModifyType... modifyTypes) {
        StringBuilder modify = new StringBuilder();
        if (ModifyType.DATATYPE.anyOf(modifyTypes)) {
            modify.append(getDataType(column)).append(' ');
        }
        if (ModifyType.DEFAULT.anyOf(modifyTypes)) {
            modify.append("DEFAULT ").append(column.getDefaultValue());
        }
        // в Oralce NOT NULL реализован в виде check-констрейнта
//        if (ModifyType.MANDATORY.anyOf(modifyTypes)) {
//            modify.append(column.isNullable() ? "NULL " : "NOT NULL");
//        }
        if (modify.length() == 0) {
            return null;
        }
        return "ALTER TABLE " + getQualifiedName(column.getTable())
                + " MODIFY " + column.getName() + ' ' + modify.toString().trim();
    }

    String dropColumnQuery(Column column) {
        return "ALTER TABLE " + getQualifiedName(column.getTable()) + " DROP COLUMN " + column.getName();
    }

    String createColumnCommentQuery(Column column) {
        if (StringUtils.isEmpty(column.getComment())) {
            return null;
        }
        return "COMMENT ON COLUMN " + getQualifiedName(column.getTable())
                + '.' + column.getName() + " IS '" + column.getComment() + "'";
    }

    String createPrimaryKeyQuery(PrimaryKey primaryKey) {
        return "ALTER TABLE " + getQualifiedName(primaryKey.getTable())
                + " ADD CONSTRAINT " + primaryKey.getName() + " PRIMARY KEY (" + primaryKey.getColumnName() + ')';
    }

    String dropPrimaryKeyQuery(PrimaryKey primaryKey) {
        return "ALTER TABLE " + getQualifiedName(primaryKey.getTable()) + " DROP PRIMARY KEY";
    }

    String createImportedKeyQuery(ImportedKey importedKey) {
        return "ALTER TABLE " + getQualifiedName(importedKey.getTable())
                + " ADD CONSTRAINT " + importedKey.getName()
                + " FOREIGN KEY (" + importedKey.getColumnName() + ")"
                + " REFERENCES " + importedKey.getPkTableSchema() + '.' + importedKey.getPkTableName()
                + " (" + importedKey.getPkColumnName() + ')';
    }

    String createCheckConstraintQuery(CheckConstraint checkConstraint) {
        return "ALTER TABLE " + getQualifiedName(checkConstraint.getTable())
                + " ADD CONSTRAINT " + checkConstraint.getName()
                + " CHECK (" + checkConstraint.getCondition() + ')';
    }

    String dropConstraintQuery(Constraint constraint) {
        return "ALTER TABLE " + getQualifiedName(constraint.getTable()) + " DROP CONSTRAINT " + constraint.getName();
    }

    String toggleConstraintQuery(Constraint constraint, boolean enabled) {
        return "ALTER TABLE " + getQualifiedName(constraint.getTable()) + ' '
                + (enabled ? "ENABLE" : "DISABLE") + " CONSTRAINT " + constraint.getName();
    }

    String createIndexQuery(Index index) {
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
                .append(" (").append(String.join(", ", index.getColumns())).append(")");
        return ddl.toString();
    }

    String dropIndexQuery(Index index) {
        return "DROP INDEX " + getQualifiedName(index);
    }

    String toggleIndexQuery(Index index, boolean enabled) {
        if (enabled) {
            return "ALTER INDEX " + getQualifiedName(index) + " REBUILD";
        } else {
            return "ALTER INDEX " + getQualifiedName(index) + " UNUSABLE";
        }
    }

    String createTriggerQuery(Trigger trigger) {
        return trigger.getBody();
    }

    String dropTriggerQuery(Trigger trigger) {
        return "DROP TRIGGER " + getQualifiedName(trigger);
    }

    String toggleTriggerQuery(Trigger trigger, boolean enabled) {
        return "ALTER TRIGGER " + getQualifiedName(trigger) + ' ' + (enabled ? "ENABLE" : "DISABLE");
    }

    String createSequenceQuery(Sequence sequence) {
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE SEQUENCE ").append(getQualifiedName(sequence)).append('\n')
                .append("MINVALUE ").append(sequence.getMinValue()).append('\n')
                .append("MAXVALUE ").append(sequence.getMaxValue()).append('\n')
                .append("START WITH ").append(sequence.getLastNumber()).append('\n')
                .append("INCREMENT BY ").append(sequence.getIncrementBy()).append('\n');
        if (sequence.getCacheSize() > 0) {
            sql.append("CACHE ").append(sequence.getCacheSize());
        } else {
            sql.append("NOCACHE");
        }
        return sql.toString();
    }

    String dropSequenceQuery(Sequence sequence) {
        return "DROP SEQUENCE " + getQualifiedName(sequence);
    }

    String analyzeTableQuery(Table table) {
        return "BEGIN\n" +
                "  SYS.DBMS_STATS.GATHER_TABLE_STATS\n" +
                "  (\n" +
                "      OWNNAME => '" + table.getSchema() + "',\n" +
                "      TABNAME => '" + table.getName() + "',\n" +
                "      ESTIMATE_PERCENT => SYS.DBMS_STATS.AUTO_SAMPLE_SIZE,\n" +
                "      METHOD_OPT => 'FOR ALL COLUMNS SIZE AUTO'\n" +
                "  );\n" +
                "END;";
    }

    String selectQuery(Table table) {
        return "SELECT " +
                table.getColumns()
                        .stream()
                        .sorted()
                        .map(Column::getName)
                        .collect(Collectors.joining(",\n")) + '\n' +
                "FROM " + getQualifiedName(table);
    }

    String insertQuery(Table table) {
        return "INSERT INTO " + getQualifiedName(table) + '\n' +
                "(" +
                table.getColumns()
                        .stream()
                        .sorted()
                        .map(Column::getName)
                        .collect(Collectors.joining(", ")) + ")\n" +
                "VALUES (" +
                table.getColumns()
                        .stream()
                        .map(c -> "?")
                        .collect(Collectors.joining(", ")) + ')';
    }

    String insertQuery(Table table, Map<String, Object> values) {
        //TODO: не поддерживаются BLOB-поля
        return "INSERT INTO " + getQualifiedName(table) +
                " (" +
                table.getColumns()
                        .stream()
                        .filter(c -> c.getColumnType() != ColumnType.BLOB)
                        .sorted()
                        .map(Column::getName)
                        .collect(Collectors.joining(", ")) + ")\n" +
                "VALUES (" +
                table.getColumns()
                        .stream()
                        .filter(c -> c.getColumnType() != ColumnType.BLOB)
                        .map(c -> {
                            Object value = values.get(c.getName());
                            return formatValue(value, c);
                        })
                        .collect(Collectors.joining(", ")) + ')';
    }

    private static String getColumnDefinition(Column column) {
        StringBuilder definition = new StringBuilder();
        definition.append(column.getName()).append(' ').append(getDataType(column));
        if (column.getDefaultValue() != null) {
            definition.append(" DEFAULT ").append(column.getDefaultValue().trim());
        }
        // в Oralce NOT NULL реализован в виде check-констрейнта
//        if (!column.isNullable()) {
//            definition.append(" NOT NULL");
//        }
        return definition.toString();
    }

    private static String getDataType(Column column) {
        String dataType = OracleType.fromColumnType(column.getColumnType());
        switch (column.getColumnType()) {
            case BOOLEAN:
                return dataType + "(1)";
            case INTEGER:
                if (column.getSize() == 0) {
                    return dataType;
                }
                return dataType + "(" + column.getSize() + ")";
            case RAW:
            case CHAR:
                return dataType + "(" + column.getSize() + ")";
            case DECIMAL:
                return dataType + "(" + column.getSize() + ", " + column.getScale() + ")";
            case STRING:
                return dataType + "(" + column.getSize() + " CHAR)";
            case TIMESTAMP:
                return dataType + "(" + column.getScale() + ")";
            default:
                return dataType;
        }
    }

    private static String formatValue(Object value, Column column) {
        if (value == null) {
            return "NULL";
        }
        switch (column.getColumnType()) {
            case CHAR:
            case STRING:
            case RAW: {
                return quoteString(String.valueOf(value));
            }
            case CLOB: {
                String clob = readClob((Clob) value);
                if (clob.length() <= 2000) {
                    return quoteString(clob);
                }
                String[] parts = StringUtils.cutString(clob, 2000);
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < parts.length; i++) {
                    sb.append("TO_CLOB(").append(quoteString(parts[i])).append(")");
                    if (i < (parts.length - 1)) {
                        sb.append(" || ");
                    }
                }
                return sb.toString();
            }
            case DATE: {
                Date d = (Date) value;
                return "TO_DATE('" + dateFormat.format(d) + "', 'YYYY-MM-DD HH24:MI:SS')";
            }
            case TIMESTAMP: {
                Timestamp t = (Timestamp) value;
                return "TO_TIMESTAMP('" + timestampFormat.format(t) + "', 'YYYY-MM-DD HH24:MI:SS.FF3')";
            }
            case DECIMAL: {
                DecimalFormatSymbols formatSymbols = DecimalFormatSymbols.getInstance();
                formatSymbols.setDecimalSeparator('.');
                DecimalFormat decimalFormat = new DecimalFormat("##0.0" + StringUtils.repeat('#', column.getScale() - 1));
                decimalFormat.setDecimalFormatSymbols(formatSymbols);
                return decimalFormat.format(value);
            }
            default:
                return String.valueOf(value);
        }
    }

    private static String getQualifiedName(Table table) {
        return table.getSchema() + '.' + table.getName();
    }

    private static String getQualifiedName(Index index) {
        return index.getTable().getSchema() + '.' + index.getName();
    }

    private static String getQualifiedName(Trigger trigger) {
        return trigger.getTable().getSchema() + '.' + trigger.getName();
    }

    private static String getQualifiedName(Sequence sequence) {
        return sequence.getSchema() + '.' + sequence.getName();
    }

    private static String quoteString(String string) {
        return '\'' + string
                .replace("'", "''")
                .replace("\n", "'||CHR(10)||'")
                .replace("\r", "'||CHR(13)||'")
                + '\'';
    }

    private static String readClob(Clob clob) {
        StringBuilder value = new StringBuilder();
        try (Reader reader = clob.getCharacterStream()) {
            char[] buffer = new char[4096];
            int len;
            while ((len = reader.read(buffer)) != -1) {
                value.append(buffer, 0, len);
            }
        } catch (SQLException | IOException e) {
            String errorMessage = "Cannot read CLOB value: " + e.getMessage();
            throw new SchemaException(errorMessage, e);
        }
        return value.toString();
    }
}
