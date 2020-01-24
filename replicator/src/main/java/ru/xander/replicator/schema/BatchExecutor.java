package ru.xander.replicator.schema;

import ru.xander.replicator.exception.BatchException;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class BatchExecutor implements AutoCloseable {

    private static final int batchSize = 1000;
    private final Connection connection;
    private final Statement statement;
    private int currentBatchSize;

    public BatchExecutor(Connection connection) {
        try {
            this.connection = connection;
            this.statement = connection.createStatement();
            this.connection.setAutoCommit(false);
        } catch (SQLException e) {
            throw new BatchException(e.getMessage(), e);
        }
    }

    public void execute(String sql) {
        try {
            this.statement.addBatch(sql);
            this.currentBatchSize++;
            if ((currentBatchSize % batchSize) == 0) {
                this.statement.executeBatch();
                this.currentBatchSize = 0;
            }
        } catch (SQLException e) {
            throw new BatchException(e.getMessage(), e);
        }
    }

    public void finish() {
        try {
            if (this.currentBatchSize > 0) {
                this.statement.executeBatch();
                this.currentBatchSize = 0;
                this.connection.commit();
            }
        } catch (SQLException e) {
            throw new BatchException(e.getMessage(), e);
        }
    }

    @Override
    public void close() {
        try {
            this.statement.close();
        } catch (SQLException e) {
            throw new BatchException(e.getMessage(), e);
        }
    }
}
