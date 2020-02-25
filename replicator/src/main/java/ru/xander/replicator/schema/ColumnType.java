package ru.xander.replicator.schema;

/**
 * @author Alexander Shakhov
 */
public enum ColumnType {

    //@formatter:off
    BOOLEAN(        "NUMBER",           "BOOLEAN"),
    INTEGER(        "NUMBER",           "INTEGER"),
    FLOAT(          "NUMBER",           "NUMERIC"),
    SERIAL(         "NUMBER",           "INTEGER"),
    CHAR(           "CHAR",             "CHAR"),
    STRING(         "VARCHAR2",         "VARCHAR"),
    DATE(           "DATE",             "DATE"),
    TIME(           "DATE",             "TIME"),
    TIMESTAMP(      "TIMESTAMP",        "TIMESTAMP"),
    CLOB(           "CLOB",             "CLOB"),
    BLOB(           "BLOB",             "BLOB");
    //@formatter:on

    private final String oracle;
    private final String hsqldb;

    ColumnType(String oracle, String hsqldb) {
        this.oracle = oracle;
        this.hsqldb = hsqldb;
    }

    public String toOracle() {
        return oracle;
    }

    public String toHsqldb() {
        return hsqldb;
    }
}
