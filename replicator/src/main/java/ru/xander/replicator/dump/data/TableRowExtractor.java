package ru.xander.replicator.dump.data;

import ru.xander.replicator.action.DumpActionConfigurer;
import ru.xander.replicator.exception.QueryFailedException;
import ru.xander.replicator.exception.ReplicatorException;
import ru.xander.replicator.exception.SchemaException;
import ru.xander.replicator.listener.Listener;
import ru.xander.replicator.listener.Progress;
import ru.xander.replicator.schema.Column;
import ru.xander.replicator.schema.Dialect;
import ru.xander.replicator.schema.Schema;
import ru.xander.replicator.schema.SchemaConnection;
import ru.xander.replicator.schema.Table;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;

/**
 * @author Alexander Shakhov
 */
public class TableRowExtractor implements AutoCloseable {

    private final long totalRows;
    private final PreparedStatement preparedStatement;
    private final Listener listener;
    private final ResultSet resultSet;
    private final Table table;
    private final TableField[] fields;
    private int currentRow;
    private long verboseEach;

    public TableRowExtractor(SchemaConnection schemaConnection, Table table) {
        try {
            Schema schema = schemaConnection.getSchema();
            Dialect dialect = schema.getDialect();
            String selectQuery = dialect.selectQuery(table);
            this.totalRows = selectCount(schemaConnection.getConnection(), selectQuery);
            try {
                this.preparedStatement = schemaConnection.getConnection().prepareStatement(selectQuery);
                this.resultSet = preparedStatement.executeQuery();
            } catch (SQLException e) {
                throw new QueryFailedException(selectQuery, e);
            }
            this.listener = schemaConnection.getListener();
            this.table = table;
            this.verboseEach = DumpActionConfigurer.DEFAULT_VERBOSE_EACH;
            Collection<Column> columns = table.getColumns();
            this.fields = new TableField[columns.size()];
            int columnIndex = 0;
            for (Column column : columns) {
                this.fields[columnIndex++] = new TableField(column);
            }
        } /*catch (QueryFailedException e) {
            throw e;
        }*/ catch (Exception e) {
            throw new ReplicatorException("Cannot init table row extractor: " + e.getMessage(), e);
        }
    }

    public void setVerboseEach(long verboseEach) {
        if (verboseEach <= 0) {
            this.verboseEach = DumpActionConfigurer.DEFAULT_VERBOSE_EACH;
        } else {
            this.verboseEach = verboseEach;
        }
    }

    public TableRow nextRow() {
        try {
            return mapRow();
        } catch (Exception e) {
            String errorMessage = "Error occurred while prepare insert query: " + e.getMessage();
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

    @SuppressWarnings("SqlDialectInspection")
    private long selectCount(Connection connection, String sql) {
        String countSql = "SELECT COUNT(*) AS C FROM(" + sql + ")";
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
