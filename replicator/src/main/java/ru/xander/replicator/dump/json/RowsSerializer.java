package ru.xander.replicator.dump.json;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import ru.xander.replicator.dump.DumpUtils;
import ru.xander.replicator.dump.data.TableField;
import ru.xander.replicator.dump.data.TableRow;
import ru.xander.replicator.dump.data.TableRowExtractor;

import java.io.IOException;
import java.sql.Blob;
import java.util.Date;

/**
 * @author Alexander Shakhov
 */
public class RowsSerializer extends JsonSerializer<TableRowExtractor> {
    @Override
    public void serialize(TableRowExtractor rowExtractor, JsonGenerator gen, SerializerProvider serializers) throws IOException {
        gen.writeStartArray();
        TableRow row;
        while ((row = rowExtractor.nextRow()) != null) {
            gen.writeStartObject();
            for (TableField field : row.getFields()) {
                gen.writeObjectFieldStart(field.getColumn().getName());
                Object value = field.getValue();
                gen.writeObjectField("type", field.getColumn().getColumnType());
                switch (field.getColumn().getColumnType()) {
                    case CHAR:
                    case STRING:
                    case CLOB:
                        gen.writeStringField("value", (String) value);
                        break;
                    case DATE:
                    case TIMESTAMP:
                        gen.writeStringField("value", DumpUtils.dateToString((Date) value));
                        break;
                    case BLOB:
                        gen.writeStringField("value", DumpUtils.blobToBase64((Blob) value));
                        break;
                    case BOOLEAN:
                        gen.writeBooleanField("value", (Boolean) value);
                        break;
                    case INTEGER:
                    case DECIMAL:
                    case SERIAL:
                        gen.writeObjectField("value", value);
                        break;
                    default:
                        gen.writeObjectField("value", String.valueOf(value));
                        break;
                }
                gen.writeEndObject();
            }
            gen.writeEndObject();
        }
        gen.writeEndArray();
    }
}
