package ru.xander.replicator.exception;

/**
 * @author Alexander Shakhov
 */
public class UnsupportedDriverException extends ReplicatorException {
    public UnsupportedDriverException(String jdbcDriver) {
        super("Unsupported jdbc driver: " + jdbcDriver);
    }
}
