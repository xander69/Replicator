package ru.xander.replicator.dump;

import ru.xander.replicator.action.DumpActionConfigurer;
import ru.xander.replicator.exception.DumpException;
import ru.xander.replicator.schema.CheckConstraint;
import ru.xander.replicator.schema.Column;
import ru.xander.replicator.schema.DataFormatter;
import ru.xander.replicator.schema.Dialect;
import ru.xander.replicator.schema.ImportedKey;
import ru.xander.replicator.schema.Index;
import ru.xander.replicator.schema.Schema;
import ru.xander.replicator.schema.Table;
import ru.xander.replicator.schema.TableField;
import ru.xander.replicator.schema.TableRow;
import ru.xander.replicator.schema.TableRowCursor;
import ru.xander.replicator.schema.Trigger;
import ru.xander.replicator.util.StringUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * @author Alexander Shakhov
 */
public class SqlTableSerializer implements TableSerializer {

    @Override
    public void serialize(Table table, Schema schema, OutputStream output, DumpOptions options) throws IOException {
        Charset charset = options.getCharset() == null ? DumpActionConfigurer.DEFAULT_CHARSET : options.getCharset();
        Dialect dialect = schema.getDialect();
        if (options.isDumpDdl()) {
            serializeTable(table, dialect, output, charset);
            if (options.isDumpDml()) {
                output.write('\n');
                serializeRows(table, schema, output, options);
            }
            serializeTableObjects(table, dialect, output, charset);
            serializeAnalyze(table, dialect, output, charset);
        } else if (options.isDumpDml()) {
            serializeRows(table, schema, output, options);
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

    private void serializeRows(Table table, Schema schema, OutputStream output, DumpOptions options) throws IOException {
        try (TableRowCursor cursor = schema.selectRows(table, options.getVerboseEach())) {
            final Dialect dialect = schema.getDialect();
            final DataFormatter formatter = schema.getDataFormatter();
            final long commitEach = options.getCommitEach();
            final Charset charset = options.getCharset();

            long currentRow = 0;
            TableRow row;
            while ((row = cursor.nextRow()) != null) {
                String insertQuery = insertQuery(row, formatter);
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
        } catch (Exception e) {
            throw new IOException(e.getMessage(), e);
        }
    }

    private void serializeAnalyze(Table table, Dialect dialect, OutputStream output, Charset charset) throws IOException {
        output.write('\n');
        output.write(dialect.analyzeTableQuery(table).getBytes(charset));
        output.write('\n');
    }

    private String insertQuery(TableRow row, DataFormatter formatter) {
        Table table = row.getTable();
        return "INSERT INTO " + table.getSchema() + '.' + table.getName() +
                " (" +
                Arrays.stream(row.getFields())
                        .map(field -> field.getColumn().getName())
                        .collect(Collectors.joining(", ")) +
                ")\n" +
                "VALUES (" +
                Arrays.stream(row.getFields())
                        .map(field -> formatValue(field, formatter))
                        .collect(Collectors.joining(", ")) +
                ')';
    }

    private String formatValue(TableField field, DataFormatter formatter) {
        Column column = field.getColumn();
        Object value = field.getValue();
        if (value == null) {
            return formatter.formatNull(column);
        }
        switch (column.getColumnType()) {
            case BOOLEAN:
                return formatter.formatBoolean(value, column);
            case INTEGER:
                return formatter.formatInteger(value, column);
            case FLOAT:
                return formatter.formatFloat(value, column);
            case SERIAL:
                return formatter.formatSerial(value, column);
            case CHAR:
                return formatter.formatChar(value, column);
            case STRING:
                return formatter.formatString(value, column);
            case DATE:
                return formatter.formatDate(value, column);
            case TIME:
                return formatter.formatTime(value, column);
            case TIMESTAMP:
                return formatter.formatTimestamp(value, column);
            case CLOB:
                return formatter.formatClob(value, column);
            case BLOB:
                return formatter.formatBlob(value, column);
        }
        throw new DumpException("Unsupproted datatype <" + column.getColumnType() + ">");
    }
}
