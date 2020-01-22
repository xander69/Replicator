package ru.xander.replicator.schema;

public enum VendorType {
    ORACLE(
            "oracle.jdbc.OracleDriver",
            "jdbc:oracle:thin:@<host>:1521:<sid>"
    ),
    POSTGRESQL(
            "org.postgresql.Driver",
            "jdbc:postgresql://<host>:5432/<sid>"
    );

    private final String jdbcDriver;
    private final String urlTemplate;

    VendorType(String jdbcDriver, String urlTemplate) {
        this.jdbcDriver = jdbcDriver;
        this.urlTemplate = urlTemplate;
    }

    public String getJdbcDriver() {
        return jdbcDriver;
    }

    public String getUrlTemplate() {
        return urlTemplate;
    }
}
