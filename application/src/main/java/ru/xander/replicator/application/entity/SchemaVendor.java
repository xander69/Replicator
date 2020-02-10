package ru.xander.replicator.application.entity;

import javafx.scene.image.Image;

public enum SchemaVendor {

    ORACLE(
            "oracle.jdbc.OracleDriver",
            "jdbc:oracle:thin:@<host>:1521:<sid>",
            "/img/icon-oracle-16.png"),
    POSTGRESQL(
            "org.postgresql.Driver",
            "jdbc:postgresql://<host>:5432/<sid>",
            "/img/icon-postgresql-16.png");

    private final String jdbcDriver;
    private final String urlTemplate;
    private final Image icon24;

    SchemaVendor(String jdbcDriver, String urlTemplate, String icon24url) {
        this.jdbcDriver = jdbcDriver;
        this.urlTemplate = urlTemplate;
        this.icon24 = new Image(getClass().getResourceAsStream(icon24url));
    }

    public String getJdbcDriver() {
        return jdbcDriver;
    }

    public String getUrlTemplate() {
        return urlTemplate;
    }

    public Image getIcon24() {
        return icon24;
    }
}
