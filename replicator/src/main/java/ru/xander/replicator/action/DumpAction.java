package ru.xander.replicator.action;

import ru.xander.replicator.exception.ReplicatorException;
import ru.xander.replicator.schema.Ddl;
import ru.xander.replicator.schema.Dml;
import ru.xander.replicator.schema.Schema;
import ru.xander.replicator.schema.Table;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;

/**
 * @author Alexander Shakhov
 */
public class DumpAction {

    public void execute(DumpConfig config) {
        //TODO: implement me
    }

    private void dumpTable(Schema source, String tableName, OutputStream output, DumpConfig dumpConfig) {
        Table sourceTable = source.getTable(tableName);
        if (sourceTable == null) {
            throw new ReplicatorException("Table " + tableName + " not found on source");
//            return;
        }
        try {
            if (dumpConfig.isDumpDdl()) {
                Ddl ddl = source.getDdl(sourceTable);
                dumpTableDdl(ddl, output, dumpConfig);
                if (dumpConfig.isDumpDml()) {
                    output.write('\n');
                    dumpTableDml(source, sourceTable, output, dumpConfig);
                }
                dumpTableObjectsDdl(ddl, output, dumpConfig);
            } else if (dumpConfig.isDumpDml()) {
                dumpTableDml(source, sourceTable, output, dumpConfig);
            }
        } catch (Exception e) {
            String errorMessage = "Failed to dump table " + tableName + ": " + e.getMessage();
            throw new ReplicatorException(errorMessage, e);
        }
    }

    private void dumpTableDdl(Ddl ddl, OutputStream output, DumpConfig dumpConfig) throws IOException {
        output.write(ddl.getTable().getBytes(dumpConfig.getCharset()));
        output.write(';');
        output.write('\n');
    }

    private void dumpTableObjectsDdl(Ddl ddl, OutputStream output, DumpConfig dumpConfig) throws IOException {
        final Charset charset = dumpConfig.getCharset();
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

    private void dumpTableDml(Schema source, Table sourceTable, OutputStream output, DumpConfig dumpConfig) throws IOException {
        final Charset charset = dumpConfig.getCharset();
        final long verboseEach = dumpConfig.getVerboseEach();
        final long commitEach = dumpConfig.getCommitEach();
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
