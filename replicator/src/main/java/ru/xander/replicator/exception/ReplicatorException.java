package ru.xander.replicator.exception;

/**
 * @author Alexander Shakhov
 */
public class ReplicatorException extends RuntimeException {
    public ReplicatorException() {
    }

    public ReplicatorException(String message) {
        super(message);
    }

    public ReplicatorException(String message, Throwable cause) {
        super(message, cause);
    }
}
