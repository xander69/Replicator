package ru.xander.replicator.schema;

import ru.xander.replicator.exception.UnsupportedDriverException;
import ru.xander.replicator.schema.hsqldb.HsqldbSchema;
import ru.xander.replicator.schema.oracle.OracleSchema;

/**
 * @author Alexander Shakhov
 */
public final class SchemaFactory {

    private static final SchemaFactory instance = new SchemaFactory();

    private SchemaFactory() {
    }

    public static SchemaFactory getInstance() {
        return instance;
    }

    public Schema create(SchemaConfig config) {
        final String jdbcDriver = config.getJdbcDriver();
        if ("oracle.jdbc.OracleDriver".equals(jdbcDriver)) {
            return new OracleSchema(config);
        } else if ("org.hsqldb.jdbc.JDBCDriver".equals(jdbcDriver)) {
            return new HsqldbSchema(config);
        }
        throw new UnsupportedDriverException(jdbcDriver);
    }
}
