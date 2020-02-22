package ru.xander.replicator.action;

import ru.xander.replicator.exception.ReplicatorException;
import ru.xander.replicator.schema.Ddl;
import ru.xander.replicator.schema.Dml;
import ru.xander.replicator.schema.Schema;
import ru.xander.replicator.schema.SchemaConfig;
import ru.xander.replicator.schema.SchemaConnection;
import ru.xander.replicator.schema.Table;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.Objects;

/**
 * @author Alexander Shakhov
 */
public class DumpAction implements Action {

    private final SchemaConfig schemaConfig;
    private final OutputStream output;
    private final boolean dumpDdl;
    private final boolean dumpDml;
    private final Charset charset;
    private final long verboseEach;
    private final long commitEach;
    private final String tableName;

    public DumpAction(SchemaConfig schemaConfig, OutputStream output, boolean dumpDdl, boolean dumpDml, Charset charset, long verboseEach, long commitEach, String tableName) {
        Objects.requireNonNull(schemaConfig, "Configure schema");
        Objects.requireNonNull(output, "Output stream");
        Objects.requireNonNull(tableName, "Table name for dump");
        this.schemaConfig = schemaConfig;
        this.output = output;
        this.dumpDdl = dumpDdl;
        this.dumpDml = dumpDml;
        this.charset = charset;
        this.verboseEach = verboseEach;
        this.commitEach = commitEach;
        this.tableName = tableName;
    }

    public void execute() {
        try (SchemaConnection schemaConnection = new SchemaConnection(schemaConfig)) {
            dumpTable(schemaConnection.getSchema());
        }
    }

    private void dumpTable(Schema schema) {
        Table table = schema.getTable(tableName);
        if (table == null) {
            throw new ReplicatorException("Table " + tableName + " not found on source");
//            return;
        }
        try {
            if (dumpDdl) {
                Ddl ddl = schema.getDdl(table);
                dumpTableDdl(ddl);
                if (dumpDml) {
                    output.write('\n');
                    dumpTableDml(schema, table);
                }
                dumpTableObjectsDdl(ddl);
            } else if (dumpDml) {
                dumpTableDml(schema, table);
            }
        } catch (Exception e) {
            String errorMessage = "Failed to dump table " + tableName + ": " + e.getMessage();
            throw new ReplicatorException(errorMessage, e);
        }
    }

    private void dumpTableDdl(Ddl ddl) throws IOException {
        output.write(ddl.getTable().getBytes(charset));
        output.write(';');
        output.write('\n');
    }

    private void dumpTableObjectsDdl(Ddl ddl) throws IOException {
        if (!ddl.getConstraints().isEmpty()) {
            output.write('\n');
            for (String constraint : ddl.getConstraints()) {
                output.write(constraint.getBytes(charset));
                output.write(';');
                output.write('\n');
            }
        }
        if (!ddl.getIndices().isEmpty()) {
            output.write('\n');
            for (String index : ddl.getIndices()) {
                output.write(index.getBytes(charset));
                output.write(';');
                output.write('\n');
            }
        }
        if (ddl.getSequence() != null) {
            output.write('\n');
            output.write(ddl.getSequence().getBytes(charset));
            output.write(';');
            output.write('\n');
        }
        if (!ddl.getTriggers().isEmpty()) {
            output.write('\n');
            for (String trigger : ddl.getTriggers()) {
                output.write(trigger.getBytes(charset));
                output.write('\n');
            }
        }
        output.write('\n');
        output.write(ddl.getAnalyze().getBytes(charset));
        output.write('\n');
    }

    private void dumpTableDml(Schema source, Table sourceTable) throws IOException {
        try (Dml dml = source.getDml(sourceTable)) {
            final String commitStatement = dml.getCommitStatement();
            String insertQuery;
            long totalRows = dml.getTotalRows();
            long currentRow = 0;
            while ((insertQuery = dml.nextInsert()) != null) {
                output.write(insertQuery.getBytes(charset));
                output.write(';');
                output.write('\n');
                currentRow++;
                if ((commitEach > 0) && ((currentRow % commitEach) == 0)) {
                    output.write(commitStatement.getBytes(charset));
                    output.write(';');
                    output.write('\n');
                }
                if ((currentRow % verboseEach) == 0) {
                    //TODO:
//                    listener.progress(new Progress(currentRow, totalRows, "Dump table " + sourceTable.getName() + " from source"));
                }
            }
            if ((commitEach == 0) || ((currentRow % commitEach) != 0)) {
                output.write(commitStatement.getBytes(charset));
                output.write(';');
                output.write('\n');
            }
        }
    }

}
