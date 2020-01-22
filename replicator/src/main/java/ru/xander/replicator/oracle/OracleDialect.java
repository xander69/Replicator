package ru.xander.replicator.oracle;

import ru.xander.replicator.schema.CheckConstraint;
import ru.xander.replicator.schema.Column;
import ru.xander.replicator.schema.Constraint;
import ru.xander.replicator.schema.ImportedKey;
import ru.xander.replicator.schema.Index;
import ru.xander.replicator.schema.IndexType;
import ru.xander.replicator.schema.ModifyType;
import ru.xander.replicator.schema.PrimaryKey;
import ru.xander.replicator.schema.Sequence;
import ru.xander.replicator.schema.Table;
import ru.xander.replicator.schema.Trigger;
import ru.xander.replicator.util.StringUtils;

import java.util.stream.Collectors;

class OracleDialect {
    String createTableQuery(Table table) {
        return "CREATE TABLE " + getQualifiedName(table) + '\n' +
                "(\n    " +
                table.getColumns().stream()
                        .map(OracleDialect::getColumnDefinition)
                        .collect(Collectors.joining(",\n    ")) +
                "\n)";
    }

    String createTableCommentQuery(Table table) {
        if (StringUtils.isEmpty(table.getComment())) {
            return null;
        }
        return "COMMENT ON TABLE " + getQualifiedName(table) + " IS '" + table.getComment() + "'";
    }

    String dropTableQuery(Table table) {
        return "DROP TABLE " + getQualifiedName(table) + " PURGE";
    }

    String createColumnQuery(Column column) {
        return "ALTER TABLE " + getQualifiedName(column.getTable()) + " ADD " + getColumnDefinition(column);
    }

    String modifyColumnQuery(Column column, ModifyType... modifyTypes) {
        StringBuilder modify = new StringBuilder();
        if (ModifyType.DATATYPE.anyOf(modifyTypes)) {
            modify.append(getDataType(column)).append(' ');
        }
        if (ModifyType.DEFAULT.anyOf(modifyTypes)) {
            modify.append("DEFAULT ").append(column.getDefaultValue());
        }
        // в Oralce NOT NULL реализован в виде check-констрейнта
//        if (ModifyType.MANDATORY.anyOf(modifyTypes)) {
//            modify.append(column.isNullable() ? "NULL " : "NOT NULL");
//        }
        if (modify.length() == 0) {
            return null;
        }
        return "ALTER TABLE " + getQualifiedName(column.getTable())
                + " MODIFY " + column.getName() + ' ' + modify.toString().trim();
    }

    String dropColumnQuery(Column column) {
        return "ALTER TABLE " + getQualifiedName(column.getTable()) + " DROP COLUMN " + column.getName();
    }

    String createColumnCommentQuery(Column column) {
        if (StringUtils.isEmpty(column.getComment())) {
            return null;
        }
        return "COMMENT ON COLUMN " + getQualifiedName(column.getTable())
                + '.' + column.getName() + " IS '" + column.getComment() + "'";
    }

    String createPrimaryKeyQuery(PrimaryKey primaryKey) {
        return "ALTER TABLE " + getQualifiedName(primaryKey.getTable())
                + " ADD CONSTRAINT " + primaryKey.getName() + " PRIMARY KEY (" + primaryKey.getColumnName() + ')';
    }

    String dropPrimaryKeyQuery(PrimaryKey primaryKey) {
        return "ALTER TABLE " + getQualifiedName(primaryKey.getTable()) + " DROP PRIMARY KEY";
    }

    String createImportedKeyQuery(ImportedKey importedKey) {
        return "ALTER TABLE " + getQualifiedName(importedKey.getTable())
                + " ADD CONSTRAINT " + importedKey.getName()
                + " FOREIGN KEY (" + importedKey.getColumnName() + ")"
                + " REFERENCES " + importedKey.getPkTableSchema() + '.' + importedKey.getPkTableName()
                + " (" + importedKey.getPkColumnName() + ')';
    }

    String createCheckConstraintQuery(CheckConstraint checkConstraint) {
        return "ALTER TABLE " + getQualifiedName(checkConstraint.getTable())
                + " ADD CONSTRAINT " + checkConstraint.getName()
                + " CHECK (" + checkConstraint.getCondition() + ')';
    }

    String dropConstraintQuery(Constraint constraint) {
        return "ALTER TABLE " + getQualifiedName(constraint.getTable()) + " DROP CONSTRAINT " + constraint.getName();
    }

    String toggleConstraintQuery(Constraint constraint, boolean enabled) {
        return "ALTER TABLE " + getQualifiedName(constraint.getTable()) + ' '
                + (enabled ? "ENABLE" : "DISABLE") + " CONSTRAINT " + constraint.getName();
    }

    String createIndexQuery(Index index) {
        StringBuilder ddl = new StringBuilder();
        ddl.append("CREATE ");
        if (index.getType() == IndexType.BITMAP) {
            ddl.append("BITMAP ");
        } else if (index.getType() == IndexType.UNIQUE) {
            ddl.append("UNIQUE ");
        }
        ddl.append("INDEX ")
                .append(getQualifiedName(index)).append(" ON ")
                .append(getQualifiedName(index.getTable()))
                .append(" (").append(String.join(", ", index.getColumns())).append(")");
        return ddl.toString();
    }

    String dropIndexQuery(Index index) {
        return "DROP INDEX " + getQualifiedName(index);
    }

    String toggleIndexQuery(Index index, boolean enabled) {
        if (enabled) {
            return "ALTER INDEX " + getQualifiedName(index) + " REBUILD";
        } else {
            return "ALTER INDEX " + getQualifiedName(index) + " UNUSABLE";
        }
    }

    String createTriggerQuery(Trigger trigger) {
        return trigger.getBody();
    }

    String dropTriggerQuery(Trigger trigger) {
        return "DROP TRIGGER " + getQualifiedName(trigger);
    }

    String toggleTriggerQuery(Trigger trigger, boolean enabled) {
        return "ALTER TRIGGER " + getQualifiedName(trigger) + ' ' + (enabled ? "ENABLE" : "DISABLE");
    }

    String createSequenceQuery(Sequence sequence) {
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE SEQUENCE ").append(getQualifiedName(sequence)).append('\n')
                .append("MINVALUE ").append(sequence.getMinValue()).append('\n')
                .append("MAXVALUE ").append(sequence.getMaxValue()).append('\n')
                .append("START WITH ").append(sequence.getLastNumber()).append('\n')
                .append("INCREMENT BY ").append(sequence.getIncrementBy()).append('\n');
        if (sequence.getCacheSize() > 0) {
            sql.append("CACHE ").append(sequence.getCacheSize());
        } else {
            sql.append("NOCACHE");
        }
        return sql.toString();
    }

    String dropSequenceQuery(Sequence sequence) {
        return "DROP SEQUENCE " + getQualifiedName(sequence);
    }

    String analyzeTableQuery(Table table) {
        return "BEGIN\n" +
                "  SYS.DBMS_STATS.GATHER_TABLE_STATS\n" +
                "  (\n" +
                "      OWNNAME => '" + table.getSchema() + "',\n" +
                "      TABNAME => '" + table.getName() + "',\n" +
                "      ESTIMATE_PERCENT => SYS.DBMS_STATS.AUTO_SAMPLE_SIZE,\n" +
                "      METHOD_OPT => 'FOR ALL COLUMNS SIZE AUTO'\n" +
                "  );\n" +
                "END;";
    }

    private static String getColumnDefinition(Column column) {
        StringBuilder definition = new StringBuilder();
        definition.append(column.getName()).append(' ').append(getDataType(column));
        if (column.getDefaultValue() != null) {
            definition.append(" DEFAULT ").append(column.getDefaultValue().trim());
        }
        // в Oralce NOT NULL реализован в виде check-констрейнта
//        if (!column.isNullable()) {
//            definition.append(" NOT NULL");
//        }
        return definition.toString();
    }

    private static String getDataType(Column column) {
        String dataType = OracleType.fromColumnType(column.getColumnType());
        switch (column.getColumnType()) {
            case BOOLEAN:
                return dataType + "(1)";
            case INTEGER:
            case RAW:
            case CHAR:
                return dataType + "(" + column.getSize() + ")";
            case DECIMAL:
                return dataType + "(" + column.getSize() + ", " + column.getScale() + ")";
            case STRING:
                return dataType + "(" + column.getSize() + " CHAR)";
            case TIMESTAMP:
                return dataType + "(" + column.getScale() + ")";
            default:
                return dataType;
        }
    }

    private static String getQualifiedName(Table table) {
        return table.getSchema() + '.' + table.getName();
    }

    private static String getQualifiedName(Index index) {
        return index.getTable().getSchema() + '.' + index.getName();
    }

    private static String getQualifiedName(Trigger trigger) {
        return trigger.getTable().getSchema() + '.' + trigger.getName();
    }

    private static String getQualifiedName(Sequence sequence) {
        return sequence.getSchema() + '.' + sequence.getName();
    }
}
