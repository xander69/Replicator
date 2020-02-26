package ru.xander.replicator.schema;

import java.math.BigInteger;

/**
 * @author Alexander Shakhov
 */
public class Sequence {

    private Table table;
    private String schema;
    private String name;
    private BigInteger startWith;
    private BigInteger incrementBy;
    private BigInteger minValue;
    private BigInteger maxValue;
    private BigInteger cacheSize;
    private Boolean cycle;

    public Table getTable() {
        return table;
    }

    public void setTable(Table table) {
        this.table = table;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public BigInteger getStartWith() {
        return startWith;
    }

    public void setStartWith(BigInteger startWith) {
        this.startWith = startWith;
    }

    public BigInteger getIncrementBy() {
        return incrementBy;
    }

    public void setIncrementBy(BigInteger incrementBy) {
        this.incrementBy = incrementBy;
    }

    public BigInteger getMinValue() {
        return minValue;
    }

    public void setMinValue(BigInteger minValue) {
        this.minValue = minValue;
    }

    public BigInteger getMaxValue() {
        return maxValue;
    }

    public void setMaxValue(BigInteger maxValue) {
        this.maxValue = maxValue;
    }

    public BigInteger getCacheSize() {
        return cacheSize;
    }

    public void setCacheSize(BigInteger cacheSize) {
        this.cacheSize = cacheSize;
    }

    public Boolean getCycle() {
        return cycle;
    }

    public void setCycle(Boolean cycle) {
        this.cycle = cycle;
    }
}
