package ru.xander.replicator.schema;

import ru.xander.replicator.exception.SchemaException;
import ru.xander.replicator.listener.Listener;
import ru.xander.replicator.listener.Progress;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Alexander Shakhov
 */
public class TableRowExtractor implements AutoCloseable {

    private final long totalRows;
    private final PreparedStatement ps;
    private final Listener listener;
    private int currentRow;
    private long verboseEach;
    private ResultSet resultSet;
    private String[] columns;

    public TableRowExtractor(long totalRows, PreparedStatement ps, Listener listener) {
        this.totalRows = totalRows;
        this.ps = ps;
        this.listener = listener;
    }

    public void setVerboseEach(long verboseEach) {
        this.verboseEach = verboseEach;
    }

    public Map<String, Object> nextRow() {
        try {
            if (resultSet == null) {
                resultSet = ps.executeQuery();
                currentRow = 0;
                initColumns();
            }
            if (resultSet.next()) {
                currentRow++;
                progress();
                return mapRow(resultSet);
            } else {
                return null;
            }
        } catch (Exception e) {
            String errorMessage = "Error occurred while prepare insert query: " + e.getMessage();
            throw new SchemaException(errorMessage, e);
        }
    }

    private void initColumns() throws SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        columns = new String[metaData.getColumnCount()];
        for (int colNum = 1; colNum <= metaData.getColumnCount(); colNum++) {
            columns[colNum - 1] = metaData.getColumnName(colNum);
        }
    }

    private Map<String, Object> mapRow(ResultSet resultSet) throws SQLException {
        Map<String, Object> row = new HashMap<>();
        for (String column : columns) {
            row.put(column, resultSet.getObject(column));
        }
        return row;
    }

    private void progress() {
        if ((currentRow % verboseEach) != 0) {
            return;
        }
        if (listener != null) {
            Progress progress = new Progress();
            progress.setMessage("Extract rows for table "); //TODO: подцепить имя таблицы
            progress.setValue(currentRow);
            progress.setTotal(totalRows);
            listener.progress(progress);
        }
    }

    @Override
    public void close() {
        try {
            if (this.resultSet != null) {
                this.resultSet.close();
            }
            this.ps.close();
        } catch (SQLException e) {
            String errorMessage = "Failed to close cursor: " + e.getMessage();
            throw new SchemaException(errorMessage, e);
        }
    }
}
