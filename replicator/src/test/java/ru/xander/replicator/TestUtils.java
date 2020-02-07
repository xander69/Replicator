package ru.xander.replicator;

/**
 * @author Alexander Shakhov
 */
final class TestUtils {
    static SchemaConfig sourceSchemaOracle() {
        return SchemaConfig.builder()
                .jdbcDriver("oracle.jdbc.OracleDriver")
                .jdbcUrl("jdbc:oracle:thin:@<host>:1521:<sid>")
                .username("scott")
                .password("scott")
                .workSchema("scott")
                .build();
    }

    static SchemaConfig targetSchemaOracle() {
        return SchemaConfig.builder()
                .jdbcDriver("oracle.jdbc.OracleDriver")
                .jdbcUrl("jdbc:oracle:thin:@<host>:1521:<sid>")
                .username("tiger")
                .password("tiger")
                .workSchema("tiger")
                .build();
    }
}
