package ru.xander.replicator.schema.hsqldb;

import ru.xander.replicator.schema.Column;
import ru.xander.replicator.schema.DataFormatter;

/**
 * @author Alexander Shakhov
 */
class HsqldbDataFormatter implements DataFormatter {
    @Override
    public String formatBoolean(Object value, Column column) {
        return null;
    }

    @Override
    public String formatInteger(Object value, Column column) {
        return null;
    }

    @Override
    public String formatFloat(Object value, Column column) {
        return null;
    }

    @Override
    public String formatSerial(Object value, Column column) {
        return null;
    }

    @Override
    public String formatChar(Object value, Column column) {
        return null;
    }

    @Override
    public String formatString(Object value, Column column) {
        return null;
    }

    @Override
    public String formatDate(Object value, Column column) {
        return null;
    }

    @Override
    public String formatTime(Object value, Column column) {
        return null;
    }

    @Override
    public String formatTimestamp(Object value, Column column) {
        return null;
    }

    @Override
    public String formatClob(Object value, Column column) {
        return null;
    }

    @Override
    public String formatBlob(Object value, Column column) {
        return null;
    }
}
