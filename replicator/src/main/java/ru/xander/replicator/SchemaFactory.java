package ru.xander.replicator;

import ru.xander.replicator.oracle.OracleSchema;

public class SchemaFactory {
    public static Schema create(SchemaOptions options) {
        String jdbcDriver = options.getJdbcDriver();
        if ("oracle.jdbc.OracleDriver".equals(jdbcDriver)) {
            return new OracleSchema(options);
        }
        throw new UnsupportedOperationException(jdbcDriver);
    }
}
