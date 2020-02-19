package ru.xander.replicator.schema;

/**
 * @author Alexander Shakhov
 */
public enum ColumnDiff {
    DATATYPE,
    DEFAULT,
    MANDATORY,
    NONE;

    public boolean anyOf(ColumnDiff... columnDiffs) {
        for (ColumnDiff columnDiff : columnDiffs) {
            if (this == columnDiff) {
                return true;
            }
        }
        return false;
    }
}
