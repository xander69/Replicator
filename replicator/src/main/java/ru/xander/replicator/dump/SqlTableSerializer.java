package ru.xander.replicator.dump;

import ru.xander.replicator.action.DumpActionConfigurer;
import ru.xander.replicator.schema.CheckConstraint;
import ru.xander.replicator.schema.Dialect;
import ru.xander.replicator.schema.ImportedKey;
import ru.xander.replicator.schema.Index;
import ru.xander.replicator.schema.Schema;
import ru.xander.replicator.schema.Table;
import ru.xander.replicator.schema.TableRowExtractor;
import ru.xander.replicator.schema.Trigger;
import ru.xander.replicator.util.StringUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.Map;

/**
 * @author Alexander Shakhov
 */
public class SqlTableSerializer implements TableSerializer {

    private final Schema schema;
    private final Dialect dialect;

    public SqlTableSerializer(Schema schema) {
        this.schema = schema;
        this.dialect = schema.getDialect();
    }

    @Override
    public void serialize(Table table, DumpOptions options) throws IOException {
        OutputStream output = options.getOutputStream();
        Charset charset = options.getCharset() == null ? DumpActionConfigurer.DEFAULT_CHARSET : options.getCharset();
        if (options.isDumpDdl()) {
            writeTable(table, output, charset);
            if (options.isDumpDml()) {
                output.write('\n');
                dumpTableDml(table, output, options);
            }
            writeTableObjects(table, output, charset);
            dumpAnalyze(table, output, charset);
        } else if (options.isDumpDml()) {
            dumpTableDml(table, output, options);
            dumpAnalyze(table, output, charset);
        }
    }

    private void writeTable(Table table, OutputStream output, Charset charset) throws IOException {
        output.write(dialect.createTableQuery(table).getBytes(charset));
        output.write(';');
        output.write('\n');
    }

    private void writeTableObjects(Table table, OutputStream output, Charset charset) throws IOException {
        if (table.getPrimaryKey() != null) {
            output.write('\n');
            output.write(dialect.createPrimaryKeyQuery(table.getPrimaryKey()).getBytes(charset));
            output.write(';');
            output.write('\n');
        }
        if (!table.getImportedKeys().isEmpty()) {
            output.write('\n');
            for (ImportedKey importedKey : table.getImportedKeys()) {
                output.write(dialect.createImportedKeyQuery(importedKey).getBytes(charset));
                output.write(';');
                output.write('\n');
            }
        }
        if (!table.getCheckConstraints().isEmpty()) {
            output.write('\n');
            for (CheckConstraint checkConstraint : table.getCheckConstraints()) {
                String checkConstraintQuery = dialect.createCheckConstraintQuery(checkConstraint);
                if (!StringUtils.isEmpty(checkConstraintQuery)) {
                    output.write(checkConstraintQuery.getBytes(charset));
                    output.write(';');
                    output.write('\n');
                }
            }
        }
        if (!table.getIndices().isEmpty()) {
            output.write('\n');
            for (Index index : table.getIndices()) {
                output.write(dialect.createIndexQuery(index).getBytes(charset));
                output.write(';');
                output.write('\n');
            }
        }
        if (table.getSequence() != null) {
            output.write('\n');
            output.write(dialect.createSequenceQuery(table.getSequence()).getBytes(charset));
            output.write(';');
            output.write('\n');
        }
        if (!table.getTriggers().isEmpty()) {
            output.write('\n');
            for (Trigger trigger : table.getTriggers()) {
                output.write(dialect.createTriggerQuery(trigger).getBytes(charset));
                output.write('\n');
            }
        }
    }

    private void dumpAnalyze(Table table, OutputStream output, Charset charset) throws IOException {
        output.write('\n');
        output.write(dialect.analyzeTableQuery(table).getBytes(charset));
        output.write('\n');
    }

    private void dumpTableDml(Table table, OutputStream output, DumpOptions options) throws IOException {
        try (TableRowExtractor rowExtractor = schema.getRows(table)) {
            rowExtractor.setVerboseEach(options.getVerboseEach());

            final long commitEach = options.getCommitEach();
            final Charset charset = options.getCharset();

            long currentRow = 0;
            Map<String, Object> row;
            while ((row = rowExtractor.nextRow()) != null) {
                String insertQuery = dialect.insertQuery(table, row);
                output.write(insertQuery.getBytes(charset));
                output.write(';');
                output.write('\n');

                currentRow++;
                if ((commitEach > 0) && ((currentRow % commitEach) == 0)) {
                    output.write(dialect.commitQuery().getBytes(charset));
                    output.write(';');
                    output.write('\n');
                }
            }

            if ((commitEach == 0) || ((currentRow % commitEach) != 0)) {
                output.write(dialect.commitQuery().getBytes(charset));
                output.write(';');
                output.write('\n');
            }
        }
    }
}
