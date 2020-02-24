package ru.xander.replicator.dump;

import ru.xander.replicator.schema.Table;

import java.io.IOException;

/**
 * @author Alexander Shakhov
 */
public interface TableSerializer {
    void serialize(Table table, DumpOptions options) throws IOException;
}
