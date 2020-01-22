package ru.xander.replicator;

import ru.xander.replicator.listener.SchemaListener;

public class SchemaOptionsFactory {
    public static SchemaOptions createSourceOracle() {
        SchemaOptions schemaOptions = new SchemaOptions();
        schemaOptions.setJdbcDriver("oracle.jdbc.OracleDriver");
        schemaOptions.setJdbcUrl("jdbc:oracle:thin:@<host>:1521:<sid>");
        schemaOptions.setUsername("scott");
        schemaOptions.setPassword("scott");
        schemaOptions.setWorkSchema("scott");
        schemaOptions.setListener(SchemaListener.stdout);
        return schemaOptions;
    }

    public static SchemaOptions createTargetOracle() {
        SchemaOptions schemaOptions = new SchemaOptions();
        schemaOptions.setJdbcDriver("oracle.jdbc.OracleDriver");
        schemaOptions.setJdbcUrl("jdbc:oracle:thin:@<host>:1521:<sid>");
        schemaOptions.setUsername("tiger");
        schemaOptions.setPassword("tiger");
        schemaOptions.setWorkSchema("tiger");
        schemaOptions.setListener(SchemaListener.stdout);
        return schemaOptions;
    }
}
