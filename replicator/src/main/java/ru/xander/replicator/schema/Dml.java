package ru.xander.replicator.schema;

import ru.xander.replicator.exception.SchemaException;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Alexander Shakhov
 */
public class Dml implements AutoCloseable {

    private final long totalRows;
    private final PreparedStatement ps;
    private ResultSet resultSet;
    private String[] columns;

    public Dml(long totalRows, PreparedStatement ps) {
        this.totalRows = totalRows;
        this.ps = ps;
    }

    public long getTotalRows() {
        return totalRows;
    }

    public Map<String, Object> nextRow() {
        try {
            if (resultSet == null) {
                resultSet = ps.executeQuery();
                initColumns();
            }
            if (resultSet.next()) {
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
