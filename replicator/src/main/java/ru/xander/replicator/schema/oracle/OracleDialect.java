package ru.xander.replicator.schema.oracle;

import ru.xander.replicator.exception.SchemaException;
import ru.xander.replicator.filter.Filter;
import ru.xander.replicator.filter.FilterType;
import ru.xander.replicator.schema.CheckConstraint;
import ru.xander.replicator.schema.Column;
import ru.xander.replicator.schema.ColumnDiff;
import ru.xander.replicator.schema.ColumnType;
import ru.xander.replicator.schema.Constraint;
import ru.xander.replicator.schema.Dialect;
import ru.xander.replicator.schema.ImportedKey;
import ru.xander.replicator.schema.Index;
import ru.xander.replicator.schema.IndexType;
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
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author Alexander Shakhov
 */
class OracleDialect implements Dialect {

    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static final SimpleDateFormat timestampFormat = new SimpleDateFormat("yyyy-MM.dd HH:mm:ss.SSS");

    private final String workSchema;

    OracleDialect(String workSchema) {
        this.workSchema = workSchema;
    }

    String selectTablesQuery(List<Filter> filterList) {
        StringBuilder sql = new StringBuilder();
        sql
                .append("SELECT\n")
                .append("  T.TABLE_NAME\n")
                .append("FROM SYS.ALL_TABLES T\n")
                .append("WHERE T.OWNER = '").append(workSchema).append("'\n");
        if (filterList != null) {
            for (Filter filter : filterList) {
                String op = filter.getType() == FilterType.NOT_LIKE ? "NOT LIKE" : "LIKE";
                sql.append("      AND T.TABLE_NAME ").append(op).append(" '").append(filter.getValue()).append("'\n");
            }
        }
        sql.append("ORDER BY T.TABLE_NAME");
        return sql.toString();
    }

    String selectTableQuery(String tableName) {
        return "SELECT\n" +
                "  T.OWNER,\n" +
                "  T.TABLE_NAME,\n" +
                "  TC.COMMENTS\n" +
                "FROM SYS.ALL_TABLES T\n" +
                "  LEFT OUTER JOIN SYS.ALL_TAB_COMMENTS TC ON\n" +
                "    T.OWNER = TC.OWNER\n" +
                "    AND T.TABLE_NAME = TC.TABLE_NAME\n" +
                "WHERE\n" +
                "  T.OWNER = UPPER('" + workSchema + "')\n" +
                "  AND T.TABLE_NAME = UPPER('" + tableName + ")'";
    }

    String selectColumnsQuery(Table table) {
        return "SELECT\n" +
                "  C.OWNER,\n" +
                "  C.TABLE_NAME,\n" +
                "  C.COLUMN_ID,\n" +
                "  C.COLUMN_NAME,\n" +
                "  C.DATA_TYPE,\n" +
                "  C.DATA_LENGTH,\n" +
                "  C.DATA_PRECISION,\n" +
                "  C.DATA_SCALE,\n" +
                "  C.CHAR_LENGTH,\n" +
                "  C.NULLABLE,\n" +
                "  C.DATA_DEFAULT,\n" +
                "  CC.COMMENTS\n" +
                "FROM SYS.ALL_TAB_COLUMNS C\n" +
                "  LEFT OUTER JOIN SYS.ALL_COL_COMMENTS CC ON\n" +
                "    C.OWNER = CC.OWNER\n" +
                "    AND C.TABLE_NAME = CC.TABLE_NAME\n" +
                "    AND C.COLUMN_NAME = CC.COLUMN_NAME\n" +
                "WHERE\n" +
                "  C.OWNER = UPPER('" + workSchema + "')\n" +
                "  AND C.TABLE_NAME = UPPER('" + table.getName() + "')\n" +
                "ORDER BY C.COLUMN_ID";
    }

    String selectConstraintsQuery(Table table) {
        return "SELECT\n" +
                "  C.OWNER,\n" +
                "  C.TABLE_NAME,\n" +
                "  C.CONSTRAINT_NAME,\n" +
                "  C.CONSTRAINT_TYPE,\n" +
                "  C.STATUS,\n" +
                "  CC.COLUMN_NAME,\n" +
                "  CR.OWNER AS R_OWNER,\n" +
                "  CR.TABLE_NAME AS R_TABLE_NAME,\n" +
                "  CR.CONSTRAINT_NAME AS R_CONSTRAINT_NAME,\n" +
                "  CCR.COLUMN_NAME AS R_COLUMN_NAME,\n" +
                "  C.SEARCH_CONDITION\n" +
                "FROM SYS.ALL_CONSTRAINTS C\n" +
                "  LEFT OUTER JOIN SYS.ALL_CONS_COLUMNS CC ON\n" +
                "    C.OWNER = CC.OWNER\n" +
                "    AND C.TABLE_NAME = CC.TABLE_NAME\n" +
                "    AND C.CONSTRAINT_NAME = CC.CONSTRAINT_NAME\n" +
                "  LEFT OUTER JOIN SYS.ALL_CONSTRAINTS CR ON\n" +
                "    C.R_OWNER = CR.OWNER\n" +
                "    AND C.R_CONSTRAINT_NAME = CR.CONSTRAINT_NAME\n" +
                "  LEFT OUTER JOIN SYS.ALL_CONS_COLUMNS CCR ON\n" +
                "    CR.OWNER = CCR.OWNER\n" +
                "    AND CR.TABLE_NAME = CCR.TABLE_NAME\n" +
                "    AND CR.CONSTRAINT_NAME = CCR.CONSTRAINT_NAME\n" +
                "WHERE C.CONSTRAINT_TYPE IN ('P', 'R', 'C')\n" +
                "      AND C.OWNER = UPPER('" + workSchema + "')\n" +
                "      AND C.TABLE_NAME = UPPER('" + table.getName() + "')\n" +
                "UNION ALL\n" +
                "SELECT\n" +
                "  C.OWNER,\n" +
                "  C.TABLE_NAME,\n" +
                "  C.CONSTRAINT_NAME,\n" +
                "  'D' AS CONSTRAINT_TYPE,\n" +
                "  C.STATUS,\n" +
                "  CC.COLUMN_NAME,\n" +
                "  CR.OWNER AS R_OWNER,\n" +
                "  CR.TABLE_NAME AS R_TABLE_NAME,\n" +
                "  CR.CONSTRAINT_NAME AS R_CONSTRAINT_NAME,\n" +
                "  CCR.COLUMN_NAME AS R_COLUMN_NAME,\n" +
                "  NULL AS SEARCH_CONDITION\n" +
                "FROM SYS.ALL_CONSTRAINTS C\n" +
                "  INNER JOIN SYS.ALL_CONSTRAINTS CR ON\n" +
                "    C.R_OWNER = CR.OWNER\n" +
                "    AND C.R_CONSTRAINT_NAME = CR.CONSTRAINT_NAME\n" +
                "  INNER JOIN SYS.ALL_CONS_COLUMNS CC ON\n" +
                "    C.OWNER = CC.OWNER\n" +
                "    AND C.TABLE_NAME = CC.TABLE_NAME\n" +
                "    AND C.CONSTRAINT_NAME = CC.CONSTRAINT_NAME\n" +
                "  INNER JOIN SYS.ALL_CONS_COLUMNS CCR ON\n" +
                "    CR.OWNER = CCR.OWNER\n" +
                "    AND CR.TABLE_NAME = CCR.TABLE_NAME\n" +
                "    AND CR.CONSTRAINT_NAME = CCR.CONSTRAINT_NAME\n" +
                "WHERE CR.OWNER = UPPER('" + workSchema + "')\n" +
                "      AND CR.TABLE_NAME = UPPER('" + table.getName() + ")\'";
    }

    String selectIndicesQuery(Table table) {
        return "SELECT\n" +
                "  I.OWNER,\n" +
                "  I.INDEX_NAME,\n" +
                "  I.INDEX_TYPE,\n" +
                "  I.TABLE_OWNER,\n" +
                "  I.TABLE_NAME,\n" +
                "  I.TABLESPACE_NAME,\n" +
                "  I.STATUS,\n" +
                "  LISTAGG(IC.COLUMN_NAME, ',') WITHIN GROUP (ORDER BY IC.COLUMN_POSITION) AS COLUMNS\n" +
                "FROM SYS.ALL_INDEXES I\n" +
                "  INNER JOIN SYS.ALL_IND_COLUMNS IC ON\n" +
                "    I.OWNER = IC.INDEX_OWNER\n" +
                "    AND I.INDEX_NAME = IC.INDEX_NAME\n" +
                "WHERE\n" +
                "  I.TABLE_OWNER = UPPER('" + workSchema + "')\n" +
                "  AND I.TABLE_NAME = UPPER('" + table.getName() + "')\n" +
                "  AND (/*I.OWNER, */I.INDEX_NAME) NOT IN\n" +
                "      (\n" +
                "        SELECT DISTINCT\n" +
                "          /*C.INDEX_OWNER,*/\n" +
                "          C.INDEX_NAME\n" +
                "        FROM SYS.ALL_CONSTRAINTS C\n" +
                "        WHERE C.OWNER = UPPER('" + workSchema + "')\n" +
                "              AND C.TABLE_NAME = UPPER('" + table.getName() + "')\n" +
                "              /*AND C.INDEX_OWNER IS NOT NULL\n*/" +
                "              AND C.INDEX_NAME IS NOT NULL\n" +
                "      )\n" +
                "GROUP BY\n" +
                "  I.OWNER,\n" +
                "  I.INDEX_NAME,\n" +
                "  I.INDEX_TYPE,\n" +
                "  I.TABLE_OWNER,\n" +
                "  I.TABLE_NAME,\n" +
                "  I.TABLESPACE_NAME,\n" +
                "  I.STATUS";
    }

    String selectTriggersQuery(Table table) {
        return "SELECT T.OWNER,\n" +
                "  T.TRIGGER_NAME,\n" +
                "  T.TRIGGER_TYPE,\n" +
                "  T.TRIGGERING_EVENT,\n" +
                "  T.DESCRIPTION,\n" +
                "  T.WHEN_CLAUSE,\n" +
                "  T.TRIGGER_BODY,\n" +
                "  T.STATUS\n" +
                "FROM SYS.ALL_TRIGGERS T\n" +
                "WHERE T.TABLE_OWNER = UPPER('" + workSchema + "')\n" +
                "      AND T.TABLE_NAME = UPPER('" + table.getName() + "')";
    }

    String selectTriggerDependenciesQuery(Trigger trigger) {
        return "SELECT D.REFERENCED_OWNER,\n" +
                "       D.REFERENCED_NAME,\n" +
                "       D.REFERENCED_TYPE\n" +
                "FROM SYS.ALL_TRIGGERS T\n" +
                "  INNER JOIN SYS.ALL_DEPENDENCIES D ON\n" +
                "    T.TABLE_OWNER = UPPER('" + workSchema + "')\n" +
                "    AND T.TABLE_NAME = UPPER('" + trigger.getTable().getName() + "')\n" +
                "    AND T.TRIGGER_NAME = UPPER('" + trigger.getName() + "')\n" +
                "    AND D.OWNER = T.OWNER\n" +
                "    AND D.NAME = T.TRIGGER_NAME\n" +
                "    AND D.REFERENCED_TYPE <> 'PACKAGE'";
    }

    String selectSequenceQuery(Table table) {
        return "SELECT\n" +
                "  S.SEQUENCE_OWNER,\n" +
                "  S.SEQUENCE_NAME,\n" +
                "  S.MIN_VALUE,\n" +
                "  S.MAX_VALUE,\n" +
                "  S.INCREMENT_BY,\n" +
                "  S.LAST_NUMBER,\n" +
                "  S.CACHE_SIZE\n" +
                "FROM SYS.ALL_TRIGGERS T\n" +
                "  INNER JOIN SYS.ALL_DEPENDENCIES D ON\n" +
                "    T.TABLE_OWNER = UPPER('" + workSchema + "')\n" +
                "    AND T.TABLE_NAME = UPPER('" + table.getName() + "')\n" +
                "    AND D.OWNER = T.OWNER\n" +
                "    AND D.NAME = T.TRIGGER_NAME\n" +
                "    AND D.REFERENCED_TYPE = 'SEQUENCE'\n" +
                "  INNER JOIN SYS.ALL_SEQUENCES S ON\n" +
                "    S.SEQUENCE_OWNER = D.REFERENCED_OWNER\n" +
                "    AND S.SEQUENCE_NAME = D.REFERENCED_NAME";
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
                CheckConstraint checkConstraint = column.getTable().getCheckConstraintByColumn(column.getName());
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
                + " ADD CONSTRAINT " + primaryKey.getName() + " PRIMARY KEY (" + primaryKey.getColumnName() + ')';
    }

    @Override
    public String dropPrimaryKeyQuery(PrimaryKey primaryKey) {
        return "ALTER TABLE " + getQualifiedName(primaryKey.getTable()) + " DROP PRIMARY KEY";
    }

    @Override
    public String createImportedKeyQuery(ImportedKey importedKey) {
        return "ALTER TABLE " + getQualifiedName(importedKey.getTable())
                + " ADD CONSTRAINT " + importedKey.getName()
                + " FOREIGN KEY (" + importedKey.getColumnName() + ")"
                + " REFERENCES " + workSchema + '.' + importedKey.getPkTableName()
                + " (" + importedKey.getPkColumnName() + ')';
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
                .append(" (").append(String.join(", ", index.getColumns())).append(")");
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
    public String insertQuery(Table table) {
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

    @Override
    public String insertQuery(Table table, Map<String, Object> values) {
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

    String selectQuery(Table table) {
        return "SELECT " +
                table.getColumns()
                        .stream()
                        .sorted()
                        .map(Column::getName)
                        .collect(Collectors.joining(",\n")) + '\n' +
                "FROM " + getQualifiedName(table);
    }

    String selectObjectQuery(String objectName, String objectType) {
        return "SELECT *\n" +
                "FROM SYS.ALL_OBJECTS\n" +
                "WHERE OWNER = '" + workSchema + "'\n" +
                "      AND OBJECT_NAME = '" + objectName + "'\n" +
                "      AND OBJECT_TYPE = '" + objectType + '\'';
    }

    String prepareTriggerBody(List<OracleTriggerDependency> dependencies, String description, String whenClause, String body) {
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

    private static String getColumnDefinition(Column column) {
        StringBuilder definition = new StringBuilder();
        definition.append(column.getName()).append(' ').append(getDataType(column));
        if (column.getDefaultValue() != null) {
            definition.append(" DEFAULT ").append(column.getDefaultValue().trim());
        }
        if (!column.isNullable()) {
            CheckConstraint checkConstraint = column.getTable().getCheckConstraintByColumn(column.getName());
            if (checkConstraint != null) {
                definition.append(" CONSTRAINT ").append(checkConstraint.getName());
            }
            definition.append(" NOT NULL");
        }
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

    private String getQualifiedName(Table table) {
        return workSchema + '.' + table.getName();
    }

    private String getQualifiedName(Index index) {
        return workSchema + '.' + index.getName();
    }

    private String getQualifiedName(Trigger trigger) {
        return workSchema + '.' + trigger.getName();
    }

    private String getQualifiedName(Sequence sequence) {
        return workSchema + '.' + sequence.getName();
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
