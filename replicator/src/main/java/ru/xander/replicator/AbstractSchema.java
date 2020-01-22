package ru.xander.replicator;

import ru.xander.replicator.exception.SchemaException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public abstract class AbstractSchema implements Schema {

    private static final Object[] emptyArgs = new Object[0];
    private final Connection connection;

    public AbstractSchema(SchemaOptions options) {
        try {
            Class.forName(options.getJdbcDriver());
            this.connection = DriverManager.getConnection(options.getJdbcUrl(), options.getUsername(), options.getPassword());
        } catch (Exception e) {
            String errorMessage = String.format(
                    "Error occurred while connecting to schema %s: %s",
                    options.getJdbcUrl(), e.getMessage());
            throw new SchemaException(errorMessage, e);
        }
    }

    protected <T> T selectOne(String sql, SingleMapper<T> mapper) {
        return selectOne(sql, emptyArgs, mapper);
    }

    protected <T> T selectOne(String sql, Object[] args, SingleMapper<T> mapper) {
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            for (int i = 0; i < args.length; i++) {
                ps.setObject(i + 1, args[i]);
            }
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return mapper.map(rs);
                } else {
                    return null;
                }
            }
        } catch (Exception e) {
            String errorMessage = "Failed to execute query: " + e.getMessage() + "\nQuery:\n" + sql;
            throw new SchemaException(errorMessage, e);
        }
    }

    protected void select(String sql, DataSetMapper mapper) {
        select(sql, emptyArgs, mapper);
    }

    protected void select(String sql, Object[] args, DataSetMapper mapper) {
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            for (int i = 0; i < args.length; i++) {
                ps.setObject(i + 1, args[i]);
            }
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    mapper.map(rs);
                }
            }
        } catch (Exception e) {
            String errorMessage = "Failed to execute query: " + e.getMessage() + "\nQuery:\n" + sql;
            throw new SchemaException(errorMessage, e);
        }
    }

    protected void execute(String sql) {
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
        } catch (SQLException e) {
            String errorMessage = "Failed to execute query: " + e.getMessage() + "\nQuery:\n" + sql;
            throw new SchemaException(errorMessage, e);
        }
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

    protected interface SingleMapper<T> {
        T map(ResultSet rs) throws SQLException;
    }

    protected interface DataSetMapper {
        void map(ResultSet rs) throws SQLException;
    }
}
