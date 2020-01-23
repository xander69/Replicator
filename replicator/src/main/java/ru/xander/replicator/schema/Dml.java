package ru.xander.replicator.schema;

import ru.xander.replicator.exception.SchemaException;
import ru.xander.replicator.util.RowMapper;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class Dml implements AutoCloseable {

    private final PreparedStatement ps;
    private final RowMapper<String> insertMapper;
    private final long totalRows;
    private ResultSet resultSet;

    public Dml(long totalRows, PreparedStatement ps, RowMapper<String> insertMapper) {
        this.totalRows = totalRows;
        this.ps = ps;
        this.insertMapper = insertMapper;
    }

    public long getTotalRows() {
        return totalRows;
    }

    public String nextInsert() {
        try {
            if (resultSet == null) {
                resultSet = ps.executeQuery();
            }
            if (resultSet.next()) {
                return insertMapper.map(resultSet);
            } else {
                return null;
            }
        } catch (Exception e) {
            String errorMessage = "Error occurred while prepare insert query: " + e.getMessage();
            throw new SchemaException(errorMessage, e);
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
