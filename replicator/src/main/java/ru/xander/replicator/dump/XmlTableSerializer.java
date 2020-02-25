package ru.xander.replicator.dump;

import ru.xander.replicator.action.DumpActionConfigurer;
import ru.xander.replicator.schema.CheckConstraint;
import ru.xander.replicator.schema.Column;
import ru.xander.replicator.schema.ImportedKey;
import ru.xander.replicator.schema.Index;
import ru.xander.replicator.schema.PrimaryKey;
import ru.xander.replicator.schema.Schema;
import ru.xander.replicator.schema.Sequence;
import ru.xander.replicator.schema.Table;
import ru.xander.replicator.schema.TableRowExtractor;
import ru.xander.replicator.schema.Trigger;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.sql.Blob;
import java.util.Collection;
import java.util.Date;
import java.util.Map;

/**
 * @author Alexander Shakhov
 */
public class XmlTableSerializer implements TableSerializer {
    private static final String XML_VERSION = "1.0";
    private Indenter indenter;

    @Override
    public void serialize(Table table, Schema schema, OutputStream output, DumpOptions options) throws IOException {
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
                writeRows(writer, table, schema);
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

    private void writeRows(XMLStreamWriter writer, Table table, Schema schema) throws XMLStreamException {
        try (TableRowExtractor rowExtractor = schema.getRows(table)) {
            indenter.write(1);
            writer.writeStartElement("rows");
            Map<String, Object> row;
            while ((row = rowExtractor.nextRow()) != null) {
                indenter.write(2);
                writer.writeStartElement("row");
                for (Map.Entry<String, Object> field : row.entrySet()) {
                    indenter.write(3);
                    writer.writeStartElement("field");
                    writer.writeAttribute("name", field.getKey());
                    Object value = field.getValue();
                    if (value == null) {
                        writer.writeCharacters("null");
                    } else if (value instanceof Date) {
                        writer.writeCharacters(DumpUtils.dateToString((Date) value));
                    } else if (value instanceof Blob) {
                        writer.writeCharacters(DumpUtils.blobToBase64((Blob) value));
                    } else if (value instanceof String) {
                        writer.writeCData((String) value);
                    } else {
                        writer.writeCharacters(String.valueOf(value));
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
        writer.writeAttribute("columnName", primaryKey.getColumnName());
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
            writer.writeAttribute("columnName", importedKey.getColumnName());
            writer.writeAttribute("enabled", String.valueOf(importedKey.getEnabled()));
            writer.writeAttribute("pkName", importedKey.getPkName());
            writer.writeAttribute("pkTableSchema", importedKey.getPkTableSchema());
            writer.writeAttribute("pkTableName", importedKey.getPkTableName());
            writer.writeAttribute("pkColumnName", importedKey.getPkColumnName());
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
            writer.writeAttribute("columnName", checkConstraint.getColumnName());
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
            writer.writeAttribute("columns", String.join(",", index.getColumns()));
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
        writer.writeAttribute("maxValue", sequence.getMaxValue());
        writer.writeAttribute("minValue", sequence.getMinValue());
        writer.writeAttribute("incrementBy", String.valueOf(sequence.getIncrementBy()));
        writer.writeAttribute("lastNumber", String.valueOf(sequence.getLastNumber()));
        writer.writeAttribute("cacheSize", String.valueOf(sequence.getCacheSize()));
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
