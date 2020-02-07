package ru.xander.replicator.exception;

/**
 * @author Alexander Shakhov
 */
public class SchemaException extends ReplicatorException {
    public SchemaException(String message) {
        super(message);
    }

    public SchemaException(String message, Throwable cause) {
        super(message, cause);
    }
}
