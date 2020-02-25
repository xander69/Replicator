package ru.xander.replicator.dump;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import ru.xander.replicator.schema.Schema;
import ru.xander.replicator.schema.Table;
import ru.xander.replicator.schema.TableRowExtractor;

import java.io.IOException;
import java.io.OutputStream;

/**
 * @author Alexander Shakhov
 */
public class JsonTableSerializer implements TableSerializer {
    @Override
    public void serialize(Table table, Schema schema, OutputStream output, DumpOptions options) throws IOException {
        try (TableRowExtractor rowExtractor = schema.getRows(table)) {
            ObjectMapper objectMapper = new ObjectMapper();
            if (options.isFormat()) {
                objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
            }
            JsonDumpContainer dumpContainer = new JsonDumpContainer();
            if (options.isDumpDdl()) {
                dumpContainer.setTable(table);
            }
            if (options.isDumpDml()) {
                dumpContainer.setRowExtractor(rowExtractor);
            }
            objectMapper.writeValue(output, dumpContainer);
        }
    }
}
