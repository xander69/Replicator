package ru.xander.replicator.dump;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import ru.xander.replicator.schema.TableRowExtractor;

import java.io.IOException;
import java.sql.Blob;
import java.util.Date;
import java.util.Map;

/**
 * @author Alexander Shakhov
 */
public class JsonRowsSerializer extends JsonSerializer<TableRowExtractor> {
    @Override
    public void serialize(TableRowExtractor rowExtractor, JsonGenerator gen, SerializerProvider serializers) throws IOException {
        gen.writeStartArray();
        Map<String, Object> row;
        while ((row = rowExtractor.nextRow()) != null) {
            gen.writeStartObject();
            for (Map.Entry<String, Object> field : row.entrySet()) {
                Object value = field.getValue();
                if (value instanceof Date) {
                    gen.writeStringField(field.getKey(), DumpUtils.dateToString((Date) value));
                } else if (value instanceof Blob) {
                    gen.writeBinaryField(field.getKey(), DumpUtils.blobToBytes((Blob) value));
                } else {
                    gen.writeObjectField(field.getKey(), value);
                }
            }
            gen.writeEndObject();
        }
        gen.writeEndArray();
    }
}
