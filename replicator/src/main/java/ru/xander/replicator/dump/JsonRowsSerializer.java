package ru.xander.replicator.dump;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import ru.xander.replicator.exception.ReplicatorException;
import ru.xander.replicator.schema.TableRowExtractor;

import java.io.IOException;
import java.sql.Blob;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

/**
 * @author Alexander Shakhov
 */
public class JsonRowsSerializer extends JsonSerializer<TableRowExtractor> {

    private final DateFormat dateFormat = new SimpleDateFormat("yyyy'-'MM'-'dd'T'HH':'mm':'ss'.'SSS");

    @Override
    public void serialize(TableRowExtractor rowExtractor, JsonGenerator gen, SerializerProvider serializers) throws IOException {
        gen.writeStartArray();
        Map<String, Object> row;
        while ((row = rowExtractor.nextRow()) != null) {
            gen.writeStartObject();
            for (Map.Entry<String, Object> field : row.entrySet()) {
                Object value = field.getValue();
                if (value instanceof Date) {
                    gen.writeStringField(field.getKey(), dateFormat.format(value));
                } else if (value instanceof Blob) {
                    gen.writeBinaryField(field.getKey(), getBlobBytes((Blob) value));
                } else {
                    gen.writeObjectField(field.getKey(), value);
                }
            }
            gen.writeEndObject();
        }
        gen.writeEndArray();
    }

    private byte[] getBlobBytes(Blob blob) {
        try {
            return blob.getBytes(0, (int) blob.length());
        } catch (SQLException e) {
            throw new ReplicatorException("Cannot convert BLOB-value to BASE64: " + e.getMessage(), e);
        }
    }
}
