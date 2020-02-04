package ru.xander.replicator.schema;

import ru.xander.replicator.Schema;
import ru.xander.replicator.exception.QueryFailedException;
import ru.xander.replicator.exception.SchemaException;
import ru.xander.replicator.listener.Alter;
import ru.xander.replicator.listener.AlterType;
import ru.xander.replicator.listener.Listener;
import ru.xander.replicator.util.DataSetMapper;
import ru.xander.replicator.util.RowMapper;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public abstract class AbstractSchema implements Schema {

    private static final Object[] emptyArgs = new Object[0];

    protected final Connection connection;
    private final Listener listener;

    public AbstractSchema(Connection connection, Listener listener) {
        this.connection = connection;
        this.listener = listener;
//        try {
//            Class.forName(options.getJdbcDriver());
//            this.connection = DriverManager.getConnection(options.getJdbcUrl(), options.getUsername(), options.getPassword());
//        } catch (Exception e) {
//            String errorMessage = String.format(
//                    "Error occurred while connecting to schema %s: %s",
//                    options.getJdbcUrl(), e.getMessage());
//            throw new SchemaException(errorMessage, e);
//        }
    }

    protected <T> T selectOne(String sql, RowMapper<T> mapper) {
        try (
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)
        ) {
            if (resultSet.next()) {
                return mapper.map(resultSet);
            } else {
                return null;
            }
        } catch (Exception e) {
            throw new QueryFailedException(sql, e);
        }
    }

    protected void select(String sql, DataSetMapper mapper) {
        try (
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)
        ) {
            while (resultSet.next()) {
                mapper.map(resultSet);
            }
        } catch (Exception e) {
            throw new QueryFailedException(sql, e);
        }
    }

    protected void execute(String sql) {
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
        } catch (SQLException e) {
            throw new QueryFailedException(sql, e);
        }
    }

    @Override
    public BatchExecutor createBatchExecutor() {
        return new BatchExecutor(connection);
    }

    @Override
    public void close() {
        try {
            this.connection.close();
        } catch (SQLException e) {
            String errorMessage = "Failed to close connection: " + e.getMessage();
            throw new SchemaException(errorMessage, e);
        }
    }

    protected void notify(String message) {
        if (listener != null) {
            listener.notify(message);
        }
    }

    protected void error(Exception e, String sql) {
        if (listener != null) {
            listener.error(e, sql);
        }
    }

    protected void alter(AlterType type, String tableName) {
        alter(type, tableName, null);
    }

    protected void alter(AlterType type, String tableName, String objectName) {
        if (listener != null) {
            Alter alter = new Alter();
            alter.setType(type);
            alter.setTableName(tableName);
            alter.setObjectName(objectName);
            listener.alter(alter);
        }
    }
}
