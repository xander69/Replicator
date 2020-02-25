package ru.xander.replicator.dump;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import ru.xander.replicator.dump.data.TableField;
import ru.xander.replicator.dump.data.TableRow;
import ru.xander.replicator.dump.data.TableRowExtractor;

import java.io.IOException;
import java.sql.Blob;
import java.util.Date;

/**
 * @author Alexander Shakhov
 */
public class JsonRowsSerializer extends JsonSerializer<TableRowExtractor> {
    @Override
    public void serialize(TableRowExtractor rowExtractor, JsonGenerator gen, SerializerProvider serializers) throws IOException {
        gen.writeStartArray();
        TableRow row;
        while ((row = rowExtractor.nextRow()) != null) {
            gen.writeStartObject();
            for (TableField field : row.getFields()) {
                String fieldName = field.getColumn().getName();
                JsonField jsonField = new JsonField();
                jsonField.setType(field.getColumn().getColumnType());
                Object value = field.getValue();
                if (value == null) {
                    jsonField.setValue(null);
                } else {
                    switch (field.getColumn().getColumnType()) {
                        case DATE:
                        case TIMESTAMP:
                            jsonField.setValue(DumpUtils.dateToString((Date) value));
                            break;
                        case BLOB:
                            jsonField.setValue(DumpUtils.blobToBase64((Blob) value));
                            break;
                        default:
                            jsonField.setValue(String.valueOf(value));
                            break;
                    }
                }
                gen.writeObjectField(fieldName, jsonField);
            }
            gen.writeEndObject();
        }
        gen.writeEndArray();
    }
}
