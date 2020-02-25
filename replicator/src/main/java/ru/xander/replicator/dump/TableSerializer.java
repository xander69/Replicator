package ru.xander.replicator.dump;

import ru.xander.replicator.schema.Schema;
import ru.xander.replicator.schema.Table;

import java.io.IOException;
import java.io.OutputStream;

/**
 * @author Alexander Shakhov
 */
public interface TableSerializer {
    void serialize(Table table, Schema schema, OutputStream output, DumpOptions options) throws IOException;
}
