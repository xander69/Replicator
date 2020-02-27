package ru.xander.replicator.schema;

import ru.xander.replicator.exception.ReplicatorException;
import ru.xander.replicator.exception.SchemaException;
import ru.xander.replicator.listener.Listener;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * @author Alexander Shakhov
 */
public class SchemaConnection implements AutoCloseable {

    private final String jdbcDriver;
    private final String jdbcUrl;
    private final String username;
    private final String password;
    private final Listener listener;
    private Connection connection;

    public SchemaConnection(SchemaConfig config, Listener listener) {
        this.jdbcDriver = config.getJdbcDriver();
        this.jdbcUrl = config.getJdbcUrl();
        this.username = config.getUsername();
        this.password = config.getPassword();
        this.listener = listener;
    }

    public Connection getJdbcConnection() {
        if (connection == null) {
            try {
                notify("Connect to " + jdbcUrl + "...");
                Class.forName(jdbcDriver);
                connection = DriverManager.getConnection(jdbcUrl, username, password);
            } catch (SQLException | ClassNotFoundException e) {
                error(e);
                String errorMessage = String.format(
                        "Error occurred while connecting to schema %s: %s",
                        jdbcUrl, e.getMessage());
                throw new SchemaException(errorMessage);
            }
        }
        return connection;
    }

    @Override
    public void close() {
        try {
            if (this.connection != null) {
                notify("Close connection.");
                this.connection.close();
            }
        } catch (SQLException e) {
            error(e);
            String errorMessage = "Failed to close connection: " + e.getMessage();
            throw new ReplicatorException(errorMessage, e);
        }
    }

    private void notify(String message) {
        if (listener != null) {
            listener.notify(message);
        }
    }

    private void error(Exception e) {
        if (listener != null) {
            listener.error(e);
        }
    }
}
