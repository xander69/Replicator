package ru.xander.replicator.schema;

import ru.xander.replicator.util.StringUtils;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * @author Alexander Shakhov
 */
public abstract class SchemaUtils {
    private SchemaUtils() {
        throw new IllegalStateException("Utility class");
    }

    public static ColumnDiff[] compareColumns(Column column1, Column column2) {
        if (column1 == null || column2 == null) {
            return new ColumnDiff[0];
        }
        Set<ColumnDiff> columnDiffs = new HashSet<>();
        if (column1.getColumnType() != column2.getColumnType()) {
            columnDiffs.add(ColumnDiff.DATATYPE);
        }
        if (column1.getSize() != column2.getSize()) {
            columnDiffs.add(ColumnDiff.DATATYPE);
        }
        if (column1.getScale() != column2.getScale()) {
            columnDiffs.add(ColumnDiff.DATATYPE);
        }
        if (!StringUtils.equalsStringIgnoreWhiteSpace(column1.getDefaultValue(), column2.getDefaultValue())) {
            columnDiffs.add(ColumnDiff.DEFAULT);
        }
        if (column1.isNullable() != column2.isNullable()) {
            columnDiffs.add(ColumnDiff.MANDATORY);
        }
        return columnDiffs.toArray(new ColumnDiff[0]);
    }

    public static <T extends Constraint> T getConstraintByColumnName(Collection<T> constraints, String columnName) {
        return constraints.stream()
                .filter(c -> StringUtils.arrayContains(c.getColumns(), columnName))
                .findFirst()
                .orElse(null);
    }

    public static Column cloneColumn(Column column) {
        Column cloned = new Column();
        cloned.setNumber(column.getNumber());
        cloned.setName(column.getName());
        cloned.setColumnType(column.getColumnType());
        cloned.setSize(column.getSize());
        cloned.setScale(column.getScale());
        cloned.setNullable(column.isNullable());
        cloned.setDefaultValue(column.getDefaultValue());
        cloned.setComment(column.getComment());
        return cloned;
    }
}
