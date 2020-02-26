package ru.xander.replicator;

import ru.xander.replicator.listener.StdOutListener;
import ru.xander.replicator.schema.PrimaryKey;
import ru.xander.replicator.schema.SchemaConfig;
import ru.xander.replicator.schema.Sequence;
import ru.xander.replicator.schema.Table;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author Alexander Shakhov
 */
public final class TestUtils {

    public static SchemaConfig sourceSchemaHsqldb() {
        return SchemaConfig.builder()
                .jdbcDriver("org.hsqldb.jdbc.JDBCDriver")
                .jdbcUrl("jdbc:hsqldb:mem:test")
                .workSchema("DV")
                .listener(new StdOutListener("SOURCE"))
                .build();
    }

    public static SchemaConfig sourceSchemaOracle() {
        return SchemaConfig.builder()
                .jdbcDriver("oracle.jdbc.OracleDriver")
                .jdbcUrl("jdbc:oracle:thin:@<host>:1521:<sid>")
                .username("scott")
                .password("scott")
                .workSchema("scott")
                .listener(new StdOutListener("SOURCE"))
                .build();
    }

    public static SchemaConfig targetSchemaOracle() {
        return SchemaConfig.builder()
                .jdbcDriver("oracle.jdbc.OracleDriver")
                .jdbcUrl("jdbc:oracle:thin:@<host>:1521:<sid>")
                .username("tiger")
                .password("tiger")
                .workSchema("tiger")
                .listener(new StdOutListener("TARGET"))
                .build();
    }

    public static void printTable(Table table) {
        if (table.getComment() != null) {
            System.out.println("/*" + table.getComment() + "*/");
        }
        System.out.println(table.getSchema() + '.' + table.getName());
        System.out.println();

        table.getColumns().forEach(c ->
                System.out.println(c.getNumber() + ") " + c.getName()
                        + " " + c.getColumnType()
                        + ", size = " + c.getSize()
                        + ", scale = " + c.getScale()
                        + (c.getDefaultValue() != null ? ", default = " + c.getDefaultValue().trim() : "")
                        + (c.getComment() != null ? " // " + c.getComment().trim() : "")));
        System.out.println();

        PrimaryKey primaryKey = table.getPrimaryKey();
        if (primaryKey != null) {
            System.out.println("Primary key:");
            System.out.println(primaryKey.getName() + " (" + primaryKey.getColumnName() + ") " + primaryKey.getEnabled());
            System.out.println();
        }

        System.out.println("Imported keys:");
        table.getImportedKeys().forEach(r ->
                System.out.println(r.getName()
                        + " (" + r.getColumnName() + ") to "
                        + r.getPkTableSchema() + "." + r.getPkTableName() + "." + r.getPkName()
                        + " (" + r.getPkColumnName() + ") " + r.getEnabled()));
        System.out.println();

        System.out.println("Exported keys:");
        table.getExportedKeys().forEach(r ->
                System.out.println(r.getFkTableSchema() + '.' + r.getFkTableName() + '.' + r.getFkName()
                        + " (" + r.getFkColumnName() + ") to "
                        + r.getName() + " (" + r.getColumnName() + ") " + r.getEnabled()));
        System.out.println();

        System.out.println("Check constraints:");
        table.getCheckConstraints().forEach(r ->
                System.out.println(r.getName()
                        + " (" + r.getColumnName() + ')'
                        + " condition = '" + r.getCondition() + '\''));
        System.out.println();

        System.out.println("Indices:");
        table.getIndices().forEach(i ->
                System.out.println(i.getName()
                        + " " + i.getType()
                        + " " + i.getColumns()
                        + " " + i.getEnabled()));
        System.out.println();

        Sequence sequence = table.getSequence();
        if (sequence != null) {
            System.out.println("Sequence:");
            System.out.println(sequence.getSchema() + '.' + sequence.getName()
                    + ", min =" + sequence.getMinValue()
                    + ", max = " + sequence.getMaxValue()
                    + ", inc = " + sequence.getIncrementBy()
                    + ", last = " + sequence.getLastNumber()
                    + ", cache = " + sequence.getCacheSize());
            System.out.println();
        }

        System.out.println("Triggers:");
        table.getTriggers().forEach(t ->
                System.out.println(t.getName()
                        + " " + t.getEnabled()
                        + "\n" + t.getBody()));
        System.out.println();
    }

    public static void initHsqldbSchema(Connection connection) throws SQLException, IOException {
        try (
                InputStream schemaResource = TestUtils.class.getResourceAsStream("/hsqldb_schema.sql");
                BufferedReader reader = new BufferedReader(new InputStreamReader(schemaResource));
                Statement statement = connection.createStatement()
        ) {
            String line;
            StringBuilder sql = new StringBuilder();
            while ((line = reader.readLine()) != null) {
                sql.append(line);
                if (line.trim().endsWith(";")) {
                    String query = sql.toString();
                    try {
                        statement.execute(query);
                    } catch (SQLException e) {
                        throw new SQLException(e.getMessage() + "\nQuery:\n" + query, e.getSQLState(), e);
                    }
                    sql.setLength(0);
                } else {
                    sql.append('\n');
                }
            }
        }
    }
}
