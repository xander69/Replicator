package ru.xander.replicator;

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

    public SchemaConnection(SchemaConfig config, Listener listener) {
        String jdbcDriver = config.getJdbcDriver();
        try {
            if ("oracle.jdbc.OracleDriver".equals(jdbcDriver)) {
                Class.forName(config.getJdbcDriver());
                this.connection = DriverManager.getConnection(config.getJdbcUrl(), config.getUsername(), config.getPassword());
                this.schema = new OracleSchema(connection, listener, config.getWorkSchema());
            }
        } catch (Exception e) {
            String errorMessage = String.format(
                    "Error occurred while connecting to schema %s: %s",
                    config.getJdbcUrl(), e.getMessage());
            throw new ReplicatorException(errorMessage, e);
        }
        throw new UnsupportedDriverException(jdbcDriver);
    }

    public Schema getSchema() {
        return schema;
    }

    @Override
    public void close() {
        try {
            this.connection.close();
        } catch (SQLException e) {
            String errorMessage = "Failed to close connection: " + e.getMessage();
            throw new ReplicatorException(errorMessage, e);
        }
    }
}
