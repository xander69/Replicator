package ru.xander.replicator.dump;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.util.StdDateFormat;
import ru.xander.replicator.dump.data.TableRowExtractor;
import ru.xander.replicator.dump.json.JsonDump;
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
            objectMapper.setDateFormat(new StdDateFormat().withColonInTimeZone(true));
            if (options.isFormat()) {
                objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
            }
            JsonDump dump = new JsonDump();
            if (options.isDumpDdl()) {
                dump.setTable(table);
            }
            if (options.isDumpDml()) {
                dump.setRowExtractor(rowExtractor);
            }
            objectMapper.writeValue(output, dump);
        }
    }
}
