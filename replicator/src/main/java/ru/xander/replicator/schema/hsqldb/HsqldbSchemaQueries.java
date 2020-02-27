package ru.xander.replicator.schema.hsqldb;

import ru.xander.replicator.filter.Filter;
import ru.xander.replicator.filter.FilterType;
import ru.xander.replicator.schema.Table;

import java.util.List;

/**
 * @author Alexander Shakhov
 */
class HsqldbSchemaQueries {
    private final String workSchema;

    HsqldbSchemaQueries(String workSchema) {
        this.workSchema = workSchema;
    }

    String selectTables(List<Filter> filterList) {
        StringBuilder sql = new StringBuilder();
        sql
                .append("SELECT\n")
                .append("  T.TABLE_NAME\n")
                .append("FROM INFORMATION_SCHEMA.SYSTEM_TABLES T\n")
                .append("WHERE T.TABLE_SCHEM = UPPER('").append(workSchema).append("')\n");
        if (filterList != null) {
            for (Filter filter : filterList) {
                switch (filter.getType()) {
                    case LIKE:
                        sql.append("      AND T.TABLE_NAME LIKE '").append(filter.getValue()).append("'\n");
                        break;
                    case NOT_LIKE:
                        sql.append("      AND T.TABLE_NAME NOT LIKE '").append(filter.getValue()).append("'\n");
                        break;
                    case IN:
                        String tableList = filter.getValue().replace(",", "', '");
                        sql.append("      AND T.TABLE_NAME IN ('").append(tableList).append("')\n");
                        break;
                }
            }
        }
        sql.append("ORDER BY T.TABLE_NAME");
        return sql.toString();
    }

    String selectTable(String tableName) {
        return "SELECT T.TABLE_SCHEM,\n" +
                "       T.TABLE_NAME,\n" +
                "       T.REMARKS\n" +
                "FROM INFORMATION_SCHEMA.SYSTEM_TABLES T\n" +
                "WHERE T.TABLE_SCHEM = UPPER('" + workSchema + "')\n" +
                "  AND T.TABLE_NAME = UPPER('" + tableName + "')";
    }

    String selectColumns(Table table) {
        return "SELECT\n" +
                "  C.TABLE_SCHEM,\n" +
                "  C.TABLE_NAME,\n" +
                "  C.ORDINAL_POSITION,\n" +
                "  C.COLUMN_NAME,\n" +
                "  C.TYPE_NAME,\n" +
                "  C.COLUMN_SIZE,\n" +
                "  C.DECIMAL_DIGITS,\n" +
                "  C.NULLABLE,\n" +
                "  C.COLUMN_DEF,\n" +
                "  C.REMARKS\n" +
                "FROM INFORMATION_SCHEMA.SYSTEM_COLUMNS C\n" +
                "WHERE C.TABLE_SCHEM = UPPER('" + workSchema + "')\n" +
                "      AND C.TABLE_NAME = UPPER('" + table.getName() + "')\n" +
                "ORDER BY C.ORDINAL_POSITION";
    }

    String selectConstraints(Table table) {
        return "SELECT\n" +
                "  'PRIMARY KEY'               AS CONSTRAINT_TYPE,\n" +
                "  P.TABLE_SCHEM               AS TABLE_SCHEMA,\n" +
                "  P.TABLE_NAME                AS TABLE_NAME,\n" +
                "  P.PK_NAME                   AS CONSTRAINT_NAME,\n" +
                "  GROUP_CONCAT(P.COLUMN_NAME) AS COLUMN_NAME,\n" +
                "  NULL                        AS R_TABLE_SCHEM,\n" +
                "  NULL                        AS R_TABLE_NAME,\n" +
                "  NULL                        AS R_CONSTRAINT_NAME,\n" +
                "  NULL                        AS R_COLUMN_NAME,\n" +
                "  NULL                        AS CONDITION\n" +
                "FROM INFORMATION_SCHEMA.SYSTEM_PRIMARYKEYS P\n" +
                "WHERE P.TABLE_SCHEM = UPPER('" + workSchema + "')\n" +
                "      AND P.TABLE_NAME = UPPER('" + table.getName() + "')\n" +
                "GROUP BY P.TABLE_SCHEM, P.TABLE_NAME, P.PK_NAME\n" +
                "UNION ALL\n" +
                "SELECT\n" +
                "  'FOREIGN KEY'                 AS CONSTRAINT_TYPE,\n" +
                "  F.FKTABLE_SCHEM               AS TABLE_SCHEMA,\n" +
                "  F.FKTABLE_NAME                AS TABLE_NAME,\n" +
                "  F.FK_NAME                     AS CONSTRAINT_NAME,\n" +
                "  GROUP_CONCAT(F.FKCOLUMN_NAME) AS COLUMN_NAME,\n" +
                "  F.PKTABLE_SCHEM               AS R_TABLE_SCHEM,\n" +
                "  F.PKTABLE_NAME                AS R_TABLE_NAME,\n" +
                "  F.PK_NAME                     AS R_CONSTRAINT_NAME,\n" +
                "  GROUP_CONCAT(F.PKCOLUMN_NAME) AS R_COLUMN_NAME,\n" +
                "  NULL                          AS CONDITION\n" +
                "FROM INFORMATION_SCHEMA.SYSTEM_CROSSREFERENCE F\n" +
                "WHERE F.FKTABLE_SCHEM = UPPER('" + workSchema + "')\n" +
                "      AND F.FKTABLE_NAME = UPPER('" + table.getName() + "')\n" +
                "GROUP BY F.FKTABLE_SCHEM, F.FKTABLE_NAME, F.FK_NAME, F.PKTABLE_SCHEM, F.PKTABLE_NAME, F.PK_NAME\n" +
                "UNION ALL\n" +
                "SELECT\n" +
                "  'EXPORTED KEY'                AS CONSTRAINT_TYPE,\n" +
                "  E.FKTABLE_SCHEM               AS TABLE_SCHEMA,\n" +
                "  E.FKTABLE_NAME                AS TABLE_NAME,\n" +
                "  E.FK_NAME                     AS CONSTRAINT_NAME,\n" +
                "  GROUP_CONCAT(E.FKCOLUMN_NAME) AS COLUMN_NAME,\n" +
                "  E.PKTABLE_SCHEM               AS R_TABLE_SCHEM,\n" +
                "  E.PKTABLE_NAME                AS R_TABLE_NAME,\n" +
                "  E.PK_NAME                     AS R_CONSTRAINT_NAME,\n" +
                "  GROUP_CONCAT(E.PKCOLUMN_NAME) AS R_COLUMN_NAME,\n" +
                "  NULL                          AS CONDITION\n" +
                "FROM INFORMATION_SCHEMA.SYSTEM_CROSSREFERENCE E\n" +
                "WHERE E.PKTABLE_SCHEM = UPPER('" + workSchema + "')\n" +
                "      AND E.PKTABLE_NAME = UPPER('" + table.getName() + "')\n" +
                "GROUP BY E.FKTABLE_SCHEM, E.FKTABLE_NAME, E.FK_NAME, E.PKTABLE_SCHEM, E.PKTABLE_NAME, E.PK_NAME\n" +
                "UNION ALL\n" +
                "SELECT\n" +
                "  'CHECK CONSTRAINT' AS CONSTRAINT_TYPE,\n" +
                "  CCU.TABLE_SCHEMA   AS TABLE_SCHEMA,\n" +
                "  CCU.TABLE_NAME     AS TABLE_NAME,\n" +
                "  CC.CONSTRAINT_NAME AS CONSTRAINT_NAME,\n" +
                "  CCU.COLUMN_NAME    AS COLUMN_NAME,\n" +
                "  NULL               AS R_TABLE_SCHEM,\n" +
                "  NULL               AS R_TABLE_NAME,\n" +
                "  NULL               AS R_CONSTRAINT_NAME,\n" +
                "  NULL               AS R_COLUMN_NAME,\n" +
                "  CC.CHECK_CLAUSE    AS CONDITION\n" +
                "FROM INFORMATION_SCHEMA.CHECK_CONSTRAINTS CC,\n" +
                "  INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE CCU\n" +
                "WHERE CCU.TABLE_SCHEMA = UPPER('" + workSchema + "')\n" +
                "      AND CCU.TABLE_NAME = UPPER('" + table.getName() + "')\n" +
                "      AND CC.CONSTRAINT_SCHEMA = CCU.CONSTRAINT_SCHEMA\n" +
                "      AND CC.CONSTRAINT_NAME = CCU.CONSTRAINT_NAME";
    }

    String selectIndices(Table table) {
        return "SELECT\n" +
                "  TABLE_SCHEM,\n" +
                "  TABLE_NAME,\n" +
                "  INDEX_NAME,\n" +
                "  NON_UNIQUE,\n" +
                "  GROUP_CONCAT(COLUMN_NAME) AS COLUMNS\n" +
                "FROM INFORMATION_SCHEMA.SYSTEM_INDEXINFO\n" +
                "WHERE TABLE_SCHEM = UPPER('" + workSchema + "')\n" +
                "  AND TABLE_NAME = UPPER('" + table.getName() + "')\n" +
                "GROUP BY\n" +
                "  TABLE_SCHEM,\n" +
                "  TABLE_NAME,\n" +
                "  INDEX_NAME,\n" +
                "  NON_UNIQUE";
    }
}
