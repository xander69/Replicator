package ru.xander.replicator.dump;

import ru.xander.replicator.action.DumpActionConfigurer;
import ru.xander.replicator.dump.data.TableField;
import ru.xander.replicator.dump.data.TableRow;
import ru.xander.replicator.dump.data.TableRowExtractor;
import ru.xander.replicator.schema.CheckConstraint;
import ru.xander.replicator.schema.Column;
import ru.xander.replicator.schema.ImportedKey;
import ru.xander.replicator.schema.Index;
import ru.xander.replicator.schema.PrimaryKey;
import ru.xander.replicator.schema.SchemaConnection;
import ru.xander.replicator.schema.Sequence;
import ru.xander.replicator.schema.Table;
import ru.xander.replicator.schema.Trigger;
import ru.xander.replicator.util.StringUtils;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.sql.Blob;
import java.util.Collection;
import java.util.Date;

/**
 * @author Alexander Shakhov
 */
public class XmlTableSerializer implements TableSerializer {
    private static final String XML_VERSION = "1.0";
    private Indenter indenter;

    @Override
    public void serialize(Table table, SchemaConnection schemaConnection, OutputStream output, DumpOptions options) throws IOException {
        Charset charset = options.getCharset() == null ? DumpActionConfigurer.DEFAULT_CHARSET : options.getCharset();
        try {
            XMLOutputFactory xmlFactory = XMLOutputFactory.newInstance();
            XMLStreamWriter writer = xmlFactory.createXMLStreamWriter(output, charset.name());

            indenter = new Indenter(writer, options.isFormat());

            writer.writeStartDocument(charset.displayName(), XML_VERSION);
            indenter.write(0);
            writer.writeStartElement("dump");
            if (options.isDumpDdl()) {
                writeTable(writer, table);
            }
            if (options.isDumpDml()) {
                writeRows(writer, table, schemaConnection);
            }
            writer.writeEndElement();

            indenter.write(0);
            writer.writeEndDocument();
        } catch (XMLStreamException e) {
            throw new IOException(e.getMessage(), e);
        }
    }

    private void writeTable(XMLStreamWriter writer, Table table) throws XMLStreamException {
        indenter.write(1);
        writer.writeStartElement("table");
        writer.writeAttribute("schema", table.getSchema());
        writer.writeAttribute("name", table.getName());
        writer.writeAttribute("comment", table.getComment());

        writeColumns(writer, table.getColumns());
        writePrimaryKey(writer, table.getPrimaryKey());
        writeImportedKeys(writer, table.getImportedKeys());
        writeCheckConstraints(writer, table.getCheckConstraints());
        writeIndices(writer, table.getIndices());
        writeSequence(writer, table.getSequence());
        writeTriggers(writer, table.getTriggers());

        indenter.write(1);
        writer.writeEndElement();
    }

    private void writeRows(XMLStreamWriter writer, Table table, SchemaConnection schemaConnection) throws XMLStreamException {
        try (TableRowExtractor rowExtractor = new TableRowExtractor(schemaConnection, table)) {
            indenter.write(1);
            writer.writeStartElement("rows");
            TableRow row;
            while ((row = rowExtractor.nextRow()) != null) {
                indenter.write(2);
                writer.writeStartElement("row");
                for (TableField field : row.getFields()) {
                    indenter.write(3);
                    writer.writeStartElement("field");
                    writer.writeAttribute("name", field.getColumn().getName());
                    writer.writeAttribute("type", String.valueOf(field.getColumn().getColumnType()));
                    Object value = field.getValue();
                    if (value == null) {
                        writer.writeCharacters("null");
                    } else {
                        switch (field.getColumn().getColumnType()) {
                            case CHAR:
                            case STRING:
                            case CLOB:
                                writer.writeCData((String) value);
                                break;
                            case DATE:
                            case TIMESTAMP:
                                writer.writeCharacters(DumpUtils.dateToString((Date) value));
                                break;
                            case BLOB:
                                writer.writeCharacters(DumpUtils.blobToBase64((Blob) value));
                                break;
                            default:
                                writer.writeCharacters(String.valueOf(value));
                                break;
                        }
                    }
                    writer.writeEndElement();
                }
                indenter.write(2);
                writer.writeEndElement();
            }
            indenter.write(1);
            writer.writeEndElement();
        }
    }

    private void writeColumns(XMLStreamWriter writer, Collection<Column> columns) throws XMLStreamException {
        indenter.write(2);
        writer.writeStartElement("columns");
        for (Column column : columns) {
            indenter.write(3);
            writer.writeStartElement("column");
            writer.writeAttribute("number", String.valueOf(column.getNumber()));
            writer.writeAttribute("name", column.getName());
            writer.writeAttribute("type", String.valueOf(column.getColumnType()));
            writer.writeAttribute("size", String.valueOf(column.getSize()));
            writer.writeAttribute("scale", String.valueOf(column.getScale()));
            writer.writeAttribute("nullable", String.valueOf(column.isNullable()));
            writer.writeAttribute("default", column.getDefaultValue());
            writer.writeAttribute("comment", column.getComment());
            writer.writeEndElement();
        }
        indenter.write(2);
        writer.writeEndElement();
    }

    private void writePrimaryKey(XMLStreamWriter writer, PrimaryKey primaryKey) throws XMLStreamException {
        if (primaryKey == null) {
            return;
        }
        indenter.write(2);
        writer.writeStartElement("primaryKey");
        writer.writeAttribute("name", primaryKey.getName());
        writer.writeAttribute("columns", StringUtils.joinColumns(primaryKey.getColumns()));
        writer.writeAttribute("enabled", String.valueOf(primaryKey.getEnabled()));
        writer.writeEndElement();
    }

    private void writeImportedKeys(XMLStreamWriter writer, Collection<ImportedKey> importedKeys) throws XMLStreamException {
        if (importedKeys.isEmpty()) {
            return;
        }
        indenter.write(2);
        writer.writeStartElement("importedKeys");
        for (ImportedKey importedKey : importedKeys) {
            indenter.write(3);
            writer.writeStartElement("importedKey");
            writer.writeAttribute("name", importedKey.getName());
            writer.writeAttribute("columns", StringUtils.joinColumns(importedKey.getColumns()));
            writer.writeAttribute("enabled", String.valueOf(importedKey.getEnabled()));
            writer.writeAttribute("pkName", importedKey.getPkName());
            writer.writeAttribute("pkTableSchema", importedKey.getPkTableSchema());
            writer.writeAttribute("pkTableName", importedKey.getPkTableName());
            writer.writeAttribute("pkColumns", StringUtils.joinColumns(importedKey.getPkColumns()));
            writer.writeEndElement();
        }
        indenter.write(2);
        writer.writeEndElement();
    }

    private void writeCheckConstraints(XMLStreamWriter writer, Collection<CheckConstraint> checkConstraints) throws XMLStreamException {
        if (checkConstraints.isEmpty()) {
            return;
        }
        indenter.write(2);
        writer.writeStartElement("checkConstraints");
        for (CheckConstraint checkConstraint : checkConstraints) {
            indenter.write(3);
            writer.writeStartElement("checkConstraint");
            writer.writeAttribute("name", checkConstraint.getName());
            writer.writeAttribute("columns", StringUtils.joinColumns(checkConstraint.getColumns()));
            writer.writeAttribute("enabled", String.valueOf(checkConstraint.getEnabled()));
            writer.writeCData(checkConstraint.getCondition());
            writer.writeEndElement();
        }
        indenter.write(2);
        writer.writeEndElement();
    }

    private void writeIndices(XMLStreamWriter writer, Collection<Index> indices) throws XMLStreamException {
        if (indices.isEmpty()) {
            return;
        }
        indenter.write(2);
        writer.writeStartElement("indices");
        for (Index index : indices) {
            indenter.write(3);
            writer.writeStartElement("index");
            writer.writeAttribute("name", index.getName());
            writer.writeAttribute("type", String.valueOf(index.getType()));
            writer.writeAttribute("columns", StringUtils.joinColumns(index.getColumns()));
            writer.writeAttribute("enabled", String.valueOf(index.getEnabled()));
            writer.writeEndElement();
        }
        indenter.write(2);
        writer.writeEndElement();
    }

    private void writeSequence(XMLStreamWriter writer, Sequence sequence) throws XMLStreamException {
        if (sequence == null) {
            return;
        }
        indenter.write(2);
        writer.writeStartElement("sequence");
        writer.writeAttribute("schema", sequence.getSchema());
        writer.writeAttribute("name", sequence.getName());
        writer.writeAttribute("startWith", String.valueOf(sequence.getStartWith()));
        writer.writeAttribute("incrementBy", String.valueOf(sequence.getIncrementBy()));
        writer.writeAttribute("minValue", String.valueOf(sequence.getMinValue()));
        writer.writeAttribute("maxValue", String.valueOf(sequence.getMaxValue()));
        writer.writeAttribute("cacheSize", String.valueOf(sequence.getCacheSize()));
        writer.writeAttribute("cycle", String.valueOf(sequence.getCycle()));
        writer.writeEndElement();
    }

    private void writeTriggers(XMLStreamWriter writer, Collection<Trigger> triggers) throws XMLStreamException {
        if (triggers.isEmpty()) {
            return;
        }
        indenter.write(2);
        writer.writeStartElement("triggers");
        for (Trigger trigger : triggers) {
            indenter.write(3);
            writer.writeStartElement("trigger");
            writer.writeAttribute("name", trigger.getName());
            writer.writeAttribute("vendorType", String.valueOf(trigger.getVendorType()));
            writer.writeAttribute("enabled", String.valueOf(trigger.getEnabled()));
            writer.writeCData(trigger.getBody());
            writer.writeEndElement();
        }
        indenter.write(2);
        writer.writeEndElement();
    }

    private static class Indenter {
        private static final String INDENT = "  ";
        private final XMLStreamWriter writer;
        private final boolean format;

        Indenter(XMLStreamWriter writer, boolean format) {
            this.writer = writer;
            this.format = format;
        }

        private void write(int depth) throws XMLStreamException {
            if (!this.format) {
                return;
            }
            this.writer.writeCharacters("\n");
            for (int i = 0; i < depth; i++) {
                this.writer.writeCharacters(INDENT);
            }
        }
    }
}
