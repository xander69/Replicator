package ru.xander.replicator.dump;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import ru.xander.replicator.dump.data.TableRowExtractor;
import ru.xander.replicator.schema.SchemaConnection;
import ru.xander.replicator.schema.Table;

import java.io.IOException;
import java.io.OutputStream;

/**
 * @author Alexander Shakhov
 */
public class JsonTableSerializer implements TableSerializer {
    @Override
    public void serialize(Table table, SchemaConnection schemaConnection, OutputStream output, DumpOptions options) throws IOException {
        try (TableRowExtractor rowExtractor = new TableRowExtractor(schemaConnection, table)) {
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
