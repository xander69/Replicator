package ru.xander.replicator.exception;

public class UnsupportedDriverException extends SchemaException {
    public UnsupportedDriverException(String jdbcDriver) {
        super("Unsupported jdbc driver: " + jdbcDriver);
    }
}
