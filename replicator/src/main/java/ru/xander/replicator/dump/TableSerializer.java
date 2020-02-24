package ru.xander.replicator.dump;

import ru.xander.replicator.schema.Table;

import java.io.IOException;
import java.io.OutputStream;

/**
 * @author Alexander Shakhov
 */
public interface TableSerializer {

    void serializeTable(Table table, OutputStream output) throws IOException;

    void serializeTableObjects(Table table, OutputStream output) throws IOException;

    void serializeRows(Table table, OutputStream output) throws IOException;

    void serializeAnalyze(Table table, OutputStream output) throws IOException;

}
