package ru.xander.replicator;

public class SchemaOptionsFactory {
    public static SchemaOptions createSourceOracle() {
        SchemaOptions schemaOptions = new SchemaOptions();
        schemaOptions.setJdbcDriver("oracle.jdbc.OracleDriver");
        schemaOptions.setJdbcUrl("jdbc:oracle:thin:@<host>:1521:<sid>");
        schemaOptions.setUsername("scott");
        schemaOptions.setPassword("scott");
        schemaOptions.setWorkSchema("scott");
        return schemaOptions;
    }

    public static SchemaOptions createTargetOracle() {
        SchemaOptions schemaOptions = new SchemaOptions();
        schemaOptions.setJdbcDriver("oracle.jdbc.OracleDriver");
        schemaOptions.setJdbcUrl("jdbc:oracle:thin:@<host>:1521:<sid>");
        schemaOptions.setUsername("tiger");
        schemaOptions.setPassword("tiger");
        schemaOptions.setWorkSchema("tiger");
        return schemaOptions;
    }
}
