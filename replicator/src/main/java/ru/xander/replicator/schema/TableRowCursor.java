package ru.xander.replicator.schema;

/**
 * @author Alexander Shakhov
 */
public interface TableRowCursor extends AutoCloseable {
    TableRow nextRow();
}
