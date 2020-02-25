package ru.xander.replicator.dump;

import ru.xander.replicator.action.DumpActionConfigurer;
import ru.xander.replicator.dump.data.TableRow;
import ru.xander.replicator.dump.data.TableRowExtractor;
import ru.xander.replicator.schema.CheckConstraint;
import ru.xander.replicator.schema.Dialect;
import ru.xander.replicator.schema.ImportedKey;
import ru.xander.replicator.schema.Index;
import ru.xander.replicator.schema.SchemaConnection;
import ru.xander.replicator.schema.Table;
import ru.xander.replicator.schema.Trigger;
import ru.xander.replicator.util.StringUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;

/**
 * @author Alexander Shakhov
 */
public class SqlTableSerializer implements TableSerializer {

    @Override
    public void serialize(Table table, SchemaConnection schemaConnection, OutputStream output, DumpOptions options) throws IOException {
        Charset charset = options.getCharset() == null ? DumpActionConfigurer.DEFAULT_CHARSET : options.getCharset();
        Dialect dialect = schemaConnection.getSchema().getDialect();
        if (options.isDumpDdl()) {
            serializeTable(table, dialect, output, charset);
            if (options.isDumpDml()) {
                output.write('\n');
                serializeRows(table, schemaConnection, output, options);
            }
            serializeTableObjects(table, dialect, output, charset);
            serializeAnalyze(table, dialect, output, charset);
        } else if (options.isDumpDml()) {
            serializeRows(table, schemaConnection, output, options);
            serializeAnalyze(table, dialect, output, charset);
        }
    }

    private void serializeTable(Table table, Dialect dialect, OutputStream output, Charset charset) throws IOException {
        output.write(dialect.createTableQuery(table).getBytes(charset));
        output.write(';');
        output.write('\n');
    }

    private void serializeTableObjects(Table table, Dialect dialect, OutputStream output, Charset charset) throws IOException {
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
                //TODO: для Oracle не надо сериализовать чек-констрейнты, т.к. они создаются вместе со столбцами
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

    private void serializeRows(Table table, SchemaConnection schemaConnection, OutputStream output, DumpOptions options) throws IOException {
        try (TableRowExtractor rowExtractor = new TableRowExtractor(schemaConnection, table)) {
            rowExtractor.setVerboseEach(options.getVerboseEach());

            final Dialect dialect = schemaConnection.getSchema().getDialect();
            final long commitEach = options.getCommitEach();
            final Charset charset = options.getCharset();

            long currentRow = 0;
            TableRow row;
            while ((row = rowExtractor.nextRow()) != null) {
                String insertQuery = dialect.insertQuery(row);
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

    private void serializeAnalyze(Table table, Dialect dialect, OutputStream output, Charset charset) throws IOException {
        output.write('\n');
        output.write(dialect.analyzeTableQuery(table).getBytes(charset));
        output.write('\n');
    }
}
