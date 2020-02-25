package ru.xander.replicator.dump;

import ru.xander.replicator.schema.SchemaConnection;
import ru.xander.replicator.schema.Table;

import java.io.IOException;
import java.io.OutputStream;

/**
 * @author Alexander Shakhov
 */
public interface TableSerializer {
    void serialize(Table table, SchemaConnection schemaConnection, OutputStream output, DumpOptions options) throws IOException;
}
