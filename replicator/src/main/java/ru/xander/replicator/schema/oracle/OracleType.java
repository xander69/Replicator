package ru.xander.replicator.schema.oracle;

import ru.xander.replicator.exception.SchemaException;
import ru.xander.replicator.schema.ColumnType;
import ru.xander.replicator.schema.IndexType;

/**
 * @author Alexander Shakhov
 */
abstract class OracleType {
    static ColumnType toColumnType(String dataType, int scale) {
        switch (dataType.toUpperCase()) {
            case "NUMBER":
                if (scale == 0) {
                    return ColumnType.INTEGER;
                }
                return ColumnType.DECIMAL;
            case "CHAR":
            case "NCHAR":
                return ColumnType.CHAR;
            case "VARCHAR2":
            case "NVARCHAR2":
                return ColumnType.STRING;
            case "DATE":
                return ColumnType.DATE;
            case "TIMESTAMP(6)":
            case "TIMESTAMP(9)":
                return ColumnType.TIMESTAMP;
            case "CLOB":
                return ColumnType.CLOB;
            case "BLOB":
                return ColumnType.BLOB;
            case "RAW":
                return ColumnType.RAW;
            default:
                throw new SchemaException("Unsupported data type = '" + dataType + "'");
        }
    }

    static String fromColumnType(ColumnType columnType) {
        switch (columnType) {
            case BOOLEAN:
            case INTEGER:
            case DECIMAL:
            case SERIAL:
                return "NUMBER";
            case CHAR:
                return "CHAR";
            case STRING:
                return "VARCHAR2";
            case DATE:
                return "DATE";
            case TIMESTAMP:
                return "TIMESTAMP";
            case CLOB:
                return "CLOB";
            case BLOB:
                return "BLOB";
            case RAW:
                return "RAW";
            default:
                return null;
        }
    }

    static IndexType toIndexType(String type) {
        if ("BITMAP".equals(type)) {
            return IndexType.BITMAP;
        }
        if ("UNIQUE".equals(type)) {
            return IndexType.UNIQUE;
        }
        return IndexType.NORMAL;
    }

}