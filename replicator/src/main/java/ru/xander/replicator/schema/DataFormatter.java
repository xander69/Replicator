package ru.xander.replicator.schema;

/**
 * Используется при снятии дампа в формате SQL.
 * @author Alexander Shakhov
 */
public interface DataFormatter {

    default String formatNull(Column column) {
        return "NULL";
    }

    String formatBoolean(Object value, Column column);

    String formatInteger(Object value, Column column);

    String formatFloat(Object value, Column column);

    String formatSerial(Object value, Column column);

    String formatChar(Object value, Column column);

    String formatString(Object value, Column column);

    String formatDate(Object value, Column column);

    String formatTime(Object value, Column column);

    String formatTimestamp(Object value, Column column);

    String formatClob(Object value, Column column);

    String formatBlob(Object value, Column column);

}
