package ru.xander.replicator.schema;

import ru.xander.replicator.exception.ReplicatorException;
import ru.xander.replicator.exception.UnsupportedDriverException;
import ru.xander.replicator.listener.Listener;
import ru.xander.replicator.schema.oracle.OracleSchema;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * @author Alexander Shakhov
 */
public class SchemaConnection implements AutoCloseable {

    private final Connection connection;
    private final Schema schema;
    private final Listener listener;

    public SchemaConnection(SchemaConfig config) {
        try {
            this.listener = config.getListener();
            final String jdbcDriver = config.getJdbcDriver();
            if ("oracle.jdbc.OracleDriver".equals(jdbcDriver)) {
                notify("Connect to " + config.getJdbcUrl());
                Class.forName(config.getJdbcDriver());
                this.connection = DriverManager.getConnection(config.getJdbcUrl(), config.getUsername(), config.getPassword());
                this.schema = new OracleSchema(connection, listener, config.getWorkSchema());
            } else {
                throw new UnsupportedDriverException(jdbcDriver);
            }
        } catch (ReplicatorException e) {
            throw e;
        } catch (Exception e) {
            String errorMessage = String.format(
                    "Error occurred while connecting to schema %s: %s",
                    config.getJdbcUrl(), e.getMessage());
            throw new ReplicatorException(errorMessage, e);
        }
    }

    public Schema getSchema() {
        return schema;
    }

    @Override
    public void close() {
        try {
            if (this.connection != null) {
                notify("Close connection");
                this.connection.close();
            }
        } catch (SQLException e) {
            String errorMessage = "Failed to close connection: " + e.getMessage();
            throw new ReplicatorException(errorMessage, e);
        }
    }

    private void notify(String message) {
        if (listener != null) {
            listener.notify(message);
        }
    }
}
