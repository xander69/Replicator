package ru.xander.replicator.schema;

import ru.xander.replicator.exception.QueryFailedException;
import ru.xander.replicator.exception.SchemaException;
import ru.xander.replicator.filter.Filter;
import ru.xander.replicator.listener.Alter;
import ru.xander.replicator.listener.AlterType;
import ru.xander.replicator.listener.Listener;
import ru.xander.replicator.listener.Progress;
import ru.xander.replicator.util.DataSetMapper;
import ru.xander.replicator.util.RowMapper;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Alexander Shakhov
 */
public abstract class AbstractSchema implements Schema {

    private final SchemaConnection connection;
    private final Listener listener;
    protected final String workSchema;

    public AbstractSchema(SchemaConfig config) {
        this.listener = config.getListener();
        this.workSchema = config.getWorkSchema();
        this.connection = new SchemaConnection(config, listener);
    }

    public Connection getConnection() {
        // only for tests
        return connection.getJdbcConnection();
    }

    @Override
    public List<String> getTables() {
        return getTables(Collections.emptyList());
    }

    @Override
    public BatchExecutor createBatchExecutor() {
        return new BatchExecutor(connection.getJdbcConnection());
    }

    @Override
    public TableRowCursor selectRows(Table table, long verboseEach) {
        return new CommonTableRowCursor(table, verboseEach);
    }

    @Override
    public void close() {
        this.connection.close();
    }

    protected <T> T selectOne(String sql, RowMapper<T> mapper) {
        try (
                Statement statement = connection.getJdbcConnection().createStatement();
                ResultSet resultSet = statement.executeQuery(sql)
        ) {
            if (resultSet.next()) {
                return mapper.map(resultSet);
            } else {
                return null;
            }
        } catch (Exception e) {
            error(e, sql);
            throw new QueryFailedException(sql, e);
        }
    }

    protected void select(String sql, DataSetMapper mapper) {
        try (
                Statement statement = connection.getJdbcConnection().createStatement();
                ResultSet resultSet = statement.executeQuery(sql)
        ) {
            while (resultSet.next()) {
                mapper.map(resultSet);
            }
        } catch (Exception e) {
            error(e, sql);
            throw new QueryFailedException(sql, e);
        }
    }

    @SuppressWarnings("SqlDialectInspection")
    private long selectCount(String sql) {
        String countSql = "SELECT COUNT(*) AS C FROM(" + sql + ")";
        try (
                Statement statement = connection.getJdbcConnection().createStatement();
                ResultSet resultSet = statement.executeQuery(countSql)
        ) {
            if (resultSet.next()) {
                return resultSet.getLong("C");
            }
            return 0L;
        } catch (Exception e) {
            error(e, countSql);
            throw new QueryFailedException(countSql, e);
        }
    }

    protected void execute(String sql) {
        execute(sql, false);
    }

    protected void execute(String sql, boolean suppressException) {
        try (Statement statement = connection.getJdbcConnection().createStatement()) {
            statement.execute(sql);
        } catch (SQLException e) {
            if (suppressException) {
                warning(e.getMessage() + "\nQuery:\n" + sql);
            } else {
                error(e, sql);
                throw new QueryFailedException(sql, e);
            }
        }
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
            listener.errorSql(e, sql);
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

    protected static String filterListToString(List<Filter> filters) {
        if (filters.isEmpty()) {
            return "ALL";
        }
        return filters.stream().map(f -> f.getType() + " (" + f.getValue() + ')').collect(Collectors.joining(", "));
    }

    protected class CommonTableRowCursor implements TableRowCursor {

        private final PreparedStatement preparedStatement;
        private final ResultSet resultSet;
        private final Table table;
        private final TableField[] fields;
        private final long totalRows;
        private final long verboseEach;
        private int currentRow;

        CommonTableRowCursor(Table table, long verboseEach) {
            String selectQuery = getDialect().selectQuery(table);
            try {
                this.table = table;
                this.verboseEach = verboseEach;
                this.totalRows = selectCount(selectQuery);
                this.preparedStatement = connection.getJdbcConnection().prepareStatement(selectQuery);
                this.resultSet = this.preparedStatement.executeQuery();
                Collection<Column> columns = table.getColumns();
                this.fields = new TableField[columns.size()];
                int columnIndex = 0;
                for (Column column : columns) {
                    this.fields[columnIndex++] = new TableField(column);
                }
            } catch (SQLException e) {
                error(e, selectQuery);
                throw new QueryFailedException(selectQuery, e);
            }
        }

        @Override
        public TableRow nextRow() {
            try {
                return mapRow();
            } catch (Exception e) {
                String errorMessage = "Error occurred while select new row: " + e.getMessage();
                throw new SchemaException(errorMessage, e);
            }
        }

        private TableRow mapRow() throws SQLException {
            if (resultSet.next()) {
                progress();
                for (TableField field : fields) {
                    field.setValue(resultSet.getObject(field.getColumn().getName()));
                }
                return new TableRow(table, fields);
            }
            return null;
        }

        private void progress() {
            currentRow++;
            if ((currentRow % verboseEach) != 0) {
                return;
            }
            if (listener != null) {
                Progress progress = new Progress();
                progress.setMessage("Extract rows for table " + table.getName());
                progress.setValue(currentRow);
                progress.setTotal(totalRows);
                listener.progress(progress);
            }
        }

        @Override
        public void close() {
            try {
                this.resultSet.close();
                this.preparedStatement.close();
            } catch (SQLException e) {
                String errorMessage = "Failed to close cursor: " + e.getMessage();
                throw new SchemaException(errorMessage, e);
            }
        }
    }
}
