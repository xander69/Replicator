package ru.xander.replicator.schema.oracle;

import ru.xander.replicator.filter.Filter;
import ru.xander.replicator.schema.Table;
import ru.xander.replicator.schema.Trigger;

import java.util.List;

/**
 * @author Alexander Shakhov
 */
final class OracleSchemaQueries {

    private final String workSchema;

    OracleSchemaQueries(String workSchema) {
        this.workSchema = workSchema;
    }

    String selectTables(List<Filter> filterList) {
        StringBuilder sql = new StringBuilder();
        sql
                .append("SELECT\n")
                .append("  T.TABLE_NAME\n")
                .append("FROM SYS.ALL_TABLES T\n")
                .append("WHERE T.OWNER = UPPER('").append(workSchema).append("')\n");
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
                "  AND T.TABLE_NAME = UPPER('" + tableName + "')";
    }

    String selectColumns(Table table) {
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

    String selectConstraints(Table table) {
        //TODO: предусмотреть выборку нескольких столбцов на констрейнт
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
                "      AND CR.TABLE_NAME = UPPER('" + table.getName() + "')";
    }

    String selectIndices(Table table) {
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

    String selectTriggers(Table table) {
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

    String selectTriggerDependencies(Trigger trigger) {
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

    String selectSequence(Table table) {
        return "SELECT\n" +
                "  S.SEQUENCE_OWNER,\n" +
                "  S.SEQUENCE_NAME,\n" +
                "  S.LAST_NUMBER,\n" +
                "  S.INCREMENT_BY,\n" +
                "  S.MIN_VALUE,\n" +
                "  S.MAX_VALUE,\n" +
                "  S.CACHE_SIZE,\n" +
                "  S.CYCLE_FLAG\n" +
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

    String selectObject(String objectName, String objectType) {
        return "SELECT *\n" +
                "FROM SYS.ALL_OBJECTS\n" +
                "WHERE OWNER = '" + workSchema + "'\n" +
                "      AND OBJECT_NAME = '" + objectName + "'\n" +
                "      AND OBJECT_TYPE = '" + objectType + '\'';
    }

    String selectConstraint(String constraintName) {
        return "SELECT *\n" +
                "FROM SYS.ALL_CONSTRAINTS\n" +
                "WHERE OWNER = '" + workSchema + "'\n" +
                "      AND CONSTRAINT_NAME = '" + constraintName + '\'';
    }
}
