package ru.xander.replicator.schema.hsqldb;

import ru.xander.replicator.exception.SchemaException;
import ru.xander.replicator.schema.ColumnType;

/**
 * @author Alexander Shakhov
 */
abstract class HsqldbTypes {
    static ColumnType toColumnType(String dataType, int scale) {
        switch (dataType.toUpperCase()) {
            case "BOOLEAN":
                return ColumnType.BOOLEAN;
            case "INT":
            case "INTEGER":
            case "BIGINT":
                return ColumnType.INTEGER;
            case "NUMERIC":
            case "DECIMAL":
                if (scale == 0) {
                    return ColumnType.INTEGER;
                }
                return ColumnType.FLOAT;
            case "DOUBLE PRECISION":
            case "FLOAT":
            case "REAL":
                return ColumnType.FLOAT;
            case "CHAR":
            case "CHARACTER":
                return ColumnType.CHAR;
            case "VARCHAR":
            case "CHARACTER VARYING":
                return ColumnType.STRING;
            case "DATE":
                return ColumnType.DATE;
            case "TIME":
                return ColumnType.TIME;
            case "DATETIME":
            case "TIMESTAMP":
                return ColumnType.TIMESTAMP;
            case "CLOB":
            case "CHARACTER LARGE OBJECT":
                return ColumnType.CLOB;
            case "BLOB":
            case "BINARY LARGE OBJECT":
                return ColumnType.BLOB;
            default:
                throw new SchemaException("Unsupported data type = '" + dataType + "'");
        }
    }

//    static String fromColumnType(ColumnType columnType) {
//        switch (columnType) {
//            case BOOLEAN:
//            case INTEGER:
//            case DECIMAL:
//            case SERIAL:
//                return "NUMBER";
//            case CHAR:
//                return "CHAR";
//            case STRING:
//                return "VARCHAR2";
//            case DATE:
//                return "DATE";
//            case TIMESTAMP:
//                return "TIMESTAMP";
//            case CLOB:
//                return "CLOB";
//            case BLOB:
//                return "BLOB";
//            case RAW:
//                return "RAW";
//            default:
//                return null;
//        }
//    }
//
//    static IndexType toIndexType(String type) {
//        if ("BITMAP".equals(type)) {
//            return IndexType.BITMAP;
//        }
//        if ("UNIQUE".equals(type)) {
//            return IndexType.UNIQUE;
//        }
//        return IndexType.NORMAL;
//    }

}