package ru.xander.replicator;

import ru.xander.replicator.schema.PrimaryKey;
import ru.xander.replicator.schema.Sequence;
import ru.xander.replicator.schema.Table;

/**
 * @author Alexander Shakhov
 */
public final class TestUtils {
    public static SchemaConfig sourceSchemaOracle() {
        return SchemaConfig.builder()
                .jdbcDriver("oracle.jdbc.OracleDriver")
                .jdbcUrl("jdbc:oracle:thin:@<host>:1521:<sid>")
                .username("scott")
                .password("scott")
                .workSchema("scott")
                .listener(new TestListener("SOURCE"))
                .build();
    }

    public static SchemaConfig targetSchemaOracle() {
        return SchemaConfig.builder()
                .jdbcDriver("oracle.jdbc.OracleDriver")
                .jdbcUrl("jdbc:oracle:thin:@<host>:1521:<sid>")
                .username("tiger")
                .password("tiger")
                .workSchema("tiger")
                .listener(new TestListener("TARGET"))
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
            System.out.println(primaryKey.getName() + " (" + primaryKey.getColumnName() + ") " + primaryKey.getEnabled());
            System.out.println();
        }

        table.getImportedKeys().forEach(r ->
                System.out.println(r.getName()
                        + " (" + r.getColumnName() + ") to "
                        + r.getPkTableSchema() + "." + r.getPkTableName() + "." + r.getPkName()
                        + " (" + r.getPkColumnName() + ") " + r.getEnabled()));
        System.out.println();

        table.getExportedKeys().forEach(r ->
                System.out.println(r.getFkTableSchema() + '.' + r.getFkTableName() + '.' + r.getFkName()
                        + " (" + r.getFkColumnName() + ") to "
                        + r.getName() + " (" + r.getColumnName() + ") " + r.getEnabled()));
        System.out.println();

        table.getIndices().forEach(i ->
                System.out.println(i.getName()
                        + " " + i.getType()
                        + " " + i.getColumns()
                        + " " + i.getEnabled()));
        System.out.println();

        Sequence sequence = table.getSequence();
        if (sequence != null) {
            System.out.println(sequence.getSchema() + '.' + sequence.getName()
                    + ", min =" + sequence.getMinValue()
                    + ", max = " + sequence.getMaxValue()
                    + ", inc = " + sequence.getIncrementBy()
                    + ", last = " + sequence.getLastNumber()
                    + ", cache = " + sequence.getCacheSize());
            System.out.println();
        }

        table.getTriggers().forEach(t ->
                System.out.println(t.getName()
                        + " " + t.getEnabled()
                        + "\n" + t.getBody()));
        System.out.println();
    }
}
