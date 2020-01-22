package ru.xander.replicator.exception;

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
