package ru.xander.replicator.schema;

import ru.xander.replicator.util.StringUtils;

import java.util.HashSet;
import java.util.Set;

/**
 * @author Alexander Shakhov
 */
public class Column implements Comparable<Column> {

    private Table table;
    private int number;
    private String name;
    private ColumnType columnType;
    private int size;
    private int scale;
    private boolean nullable;
    private String defaultValue;
    private String comment;

    public Table getTable() {
        return table;
    }

    public void setTable(Table table) {
        this.table = table;
    }

    public int getNumber() {
        return number;
    }

    public void setNumber(int number) {
        this.number = number;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public ColumnType getColumnType() {
        return columnType;
    }

    public void setColumnType(ColumnType columnType) {
        this.columnType = columnType;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public int getScale() {
        return scale;
    }

    public void setScale(int scale) {
        this.scale = scale;
    }

    public boolean isNullable() {
        return nullable;
    }

    public void setNullable(boolean nullable) {
        this.nullable = nullable;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public void setDefaultValue(String defaultValue) {
        this.defaultValue = defaultValue;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    @Override
    public int compareTo(Column other) {
        return Integer.compare(number, other.number);
    }

    public ColumnDiff[] getDiffs(Column other) {
        Set<ColumnDiff> columnDiffs = new HashSet<>();
        if (columnType != other.columnType) {
            columnDiffs.add(ColumnDiff.DATATYPE);
        }
        if (size != other.size) {
            columnDiffs.add(ColumnDiff.DATATYPE);
        }
        if (scale != other.scale) {
            columnDiffs.add(ColumnDiff.DATATYPE);
        }
        if (!StringUtils.equalsStringIgnoreWhiteSpace(defaultValue, other.defaultValue)) {
            columnDiffs.add(ColumnDiff.DEFAULT);
        }
        if (other.nullable != nullable) {
            columnDiffs.add(ColumnDiff.MANDATORY);
        }
        return columnDiffs.toArray(new ColumnDiff[0]);
    }

}
