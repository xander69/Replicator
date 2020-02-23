package ru.xander.replicator.schema;

import ru.xander.replicator.exception.QueryFailedException;
import ru.xander.replicator.listener.Alter;
import ru.xander.replicator.listener.AlterType;
import ru.xander.replicator.listener.Listener;
import ru.xander.replicator.util.DataSetMapper;
import ru.xander.replicator.util.RowMapper;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author Alexander Shakhov
 */
public abstract class AbstractSchema implements Schema {

    protected final Connection connection;
    private final Listener listener;

    public AbstractSchema(Connection connection, Listener listener) {
        this.connection = connection;
        this.listener = listener;
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

    @SuppressWarnings("SqlDialectInspection")
    protected long selectCount(String sql) {
        String countSql = "SELECT COUNT(*) AS C\nFROM\n(\n" + sql + "\n)";
        try (
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(countSql)
        ) {
            if (resultSet.next()) {
                return resultSet.getLong("C");
            }
            return 0L;
        } catch (Exception e) {
            throw new QueryFailedException(countSql, e);
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

    protected void notify(String message) {
        if (listener != null) {
            listener.notify(message);
        }
    }

    protected void warning(String message) {
        if (listener != null) {
            listener.warning(message);
        }
    }

    protected void error(Exception e, String sql) {
        if (listener != null) {
            listener.error(e, sql);
        }
    }

    protected void alter(AlterType type, String tableName, String sql) {
        alter(type, tableName, null, sql);
    }

    protected void alter(AlterType type, String tableName, String objectName, String sql) {
        alter(type, tableName, objectName, null, sql);
    }

    protected void alter(AlterType type, String tableName, String objectName, String extra, String sql) {
        if (listener != null) {
            Alter alter = new Alter();
            alter.setType(type);
            alter.setTableName(tableName);
            alter.setObjectName(objectName);
            alter.setExtra(extra);
            alter.setSql(sql);
            listener.alter(alter);
        }
    }
}
