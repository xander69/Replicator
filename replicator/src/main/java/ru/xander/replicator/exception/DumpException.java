package ru.xander.replicator.exception;

/**
 * @author Alexander Shakhov
 */
public class DumpException extends RuntimeException {
    public DumpException(String message) {
        super(message);
    }

    public DumpException(String message, Throwable cause) {
        super(message, cause);
    }
}
