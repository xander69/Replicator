package ru.xander.replicator.exception;

public class QueryFailedException extends ReplicatorException {
    private final String sql;
    private final String origMessage;

    public QueryFailedException(String sql, Throwable cause) {
        super("Failed to execute query: " + cause.getMessage() + "\nQuery:\n" + sql);
        this.sql = sql;
        this.origMessage = cause.getMessage();
    }

    public String getSql() {
        return sql;
    }

    public String getOrigMessage() {
        return origMessage;
    }
}
