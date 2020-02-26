package ru.xander.replicator.schema.hsqldb;

import org.hamcrest.MatcherAssert;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import ru.xander.replicator.TestUtils;
import ru.xander.replicator.schema.CheckConstraint;
import ru.xander.replicator.schema.Column;
import ru.xander.replicator.schema.ColumnType;
import ru.xander.replicator.schema.ExportedKey;
import ru.xander.replicator.schema.ImportedKey;
import ru.xander.replicator.schema.Index;
import ru.xander.replicator.schema.IndexType;
import ru.xander.replicator.schema.PrimaryKey;
import ru.xander.replicator.schema.SchemaConnection;
import ru.xander.replicator.schema.Table;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import static org.hamcrest.collection.IsIterableContainingInOrder.contains;

/**
 * @author Alexander Shakhov
 */
@SuppressWarnings("SameParameterValue")
public class HsqldbSchemaTest {

    private static SchemaConnection schemaConnection;

    @BeforeClass
    public static void setUp() throws Exception {
        schemaConnection = new SchemaConnection(TestUtils.sourceSchemaHsqldb());
        TestUtils.initHsqldbSchema(schemaConnection.getConnection());
    }

    @AfterClass
    public static void tearDown() {
        schemaConnection.close();
    }

    @Test
    public void getTables() {
        List<String> actual = schemaConnection.getSchema().getTables();
        MatcherAssert.assertThat(actual, contains("TABLE1", "TABLE2", "TABLE3", "TABLE4"));
    }

    @Test
    public void getTableWithColumns() {
        Table table1 = schemaConnection.getSchema().getTable("TABLE1");

        Assert.assertNotNull(table1);
        Assert.assertEquals("DV", table1.getSchema());
        Assert.assertEquals("TABLE1", table1.getName());
        Assert.assertEquals("Table 1 comment", table1.getComment());

        Collection<Column> columns = table1.getColumns();
        Assert.assertNotNull(columns);
        Assert.assertEquals(32, columns.size());
        Iterator<Column> columnIterator = columns.iterator();
        assertColumn(columnIterator.next(), 1, "BOOL", ColumnType.BOOLEAN);
        assertColumn(columnIterator.next(), 2, "INT1", ColumnType.INTEGER, "Integer field for Table 1", "100");
        assertColumn(columnIterator.next(), 3, "INT2", ColumnType.INTEGER);
        assertColumn(columnIterator.next(), 4, "TINY", ColumnType.INTEGER);
        assertColumn(columnIterator.next(), 5, "SMALL", ColumnType.INTEGER);
        assertColumn(columnIterator.next(), 6, "BIT_", ColumnType.INTEGER);
        assertColumn(columnIterator.next(), 7, "NUM1", ColumnType.INTEGER);
        assertColumn(columnIterator.next(), 8, "NUM2", ColumnType.INTEGER, 10);
        assertColumn(columnIterator.next(), 9, "NUM3", ColumnType.FLOAT, 17, 2);
        assertColumn(columnIterator.next(), 10, "DEC1", ColumnType.INTEGER);
        assertColumn(columnIterator.next(), 11, "DEC2", ColumnType.INTEGER, 12);
        assertColumn(columnIterator.next(), 12, "DEC3", ColumnType.FLOAT, 15, 4);
        assertColumn(columnIterator.next(), 13, "BIG", ColumnType.INTEGER);
        assertColumn(columnIterator.next(), 14, "FLOAT1", ColumnType.FLOAT);
        assertColumn(columnIterator.next(), 15, "FLOAT2", ColumnType.FLOAT);
        assertColumn(columnIterator.next(), 16, "REAL_", ColumnType.FLOAT);
        assertColumn(columnIterator.next(), 17, "CHR1", ColumnType.CHAR);
        assertColumn(columnIterator.next(), 18, "CHR2", ColumnType.CHAR, 5);
        assertColumn(columnIterator.next(), 19, "CHR3", ColumnType.CHAR);
        assertColumn(columnIterator.next(), 20, "CHR4", ColumnType.CHAR, 10);
        assertColumn(columnIterator.next(), 21, "STRING1", ColumnType.STRING, 500);
        assertColumn(columnIterator.next(), 22, "STRING2", ColumnType.STRING, 50000000);
        assertColumn(columnIterator.next(), 23, "DATE_", ColumnType.DATE);
        assertColumn(columnIterator.next(), 24, "DATETIME_", ColumnType.TIMESTAMP);
        assertColumn(columnIterator.next(), 25, "TIME_", ColumnType.TIME);
        assertColumn(columnIterator.next(), 26, "TIMESTAMP_", ColumnType.TIMESTAMP);
        assertColumn(columnIterator.next(), 27, "CLOB1", ColumnType.CLOB);
        assertColumn(columnIterator.next(), 28, "CLOB2", ColumnType.CLOB, 10000);
        assertColumn(columnIterator.next(), 29, "BLOB1", ColumnType.BLOB);
        assertColumn(columnIterator.next(), 30, "BLOB2", ColumnType.BLOB, 10000);
        assertColumn(columnIterator.next(), 31, "BIN", ColumnType.BLOB);
        assertColumn(columnIterator.next(), 32, "VARBIN", ColumnType.BLOB, 10000);

        Column firstColumn = table1.getColumn("BOOL");
        Assert.assertNotNull(firstColumn);
        Assert.assertEquals("BOOL", firstColumn.getName());
        Assert.assertEquals(table1, firstColumn.getTable());
        Assert.assertTrue(firstColumn.isNullable());
        Assert.assertNull(table1.getColumn("XXX"));

        Table xxx = schemaConnection.getSchema().getTable("XXX");
        Assert.assertNull(xxx);
    }

    @Test
    public void getTableWithConstraints() {
        Table table2 = schemaConnection.getSchema().getTable("TABLE2");
        Assert.assertNotNull(table2);
        Assert.assertEquals("TABLE2", table2.getName());

        PrimaryKey primaryKey = table2.getPrimaryKey();
        Assert.assertNotNull(primaryKey);
        Assert.assertEquals(table2, primaryKey.getTable());
        Assert.assertEquals("TAB2_PK", primaryKey.getName());
        Assert.assertArrayEquals(new String[]{"C1", "C2"}, primaryKey.getColumns());
        Assert.assertTrue(primaryKey.getEnabled());

        Collection<CheckConstraint> checkConstraints = table2.getCheckConstraints();
        Assert.assertNotNull(checkConstraints);
        Assert.assertEquals(1, checkConstraints.size());
        CheckConstraint checkConstraint = checkConstraints.iterator().next();
        Assert.assertNotNull(checkConstraint);
        Assert.assertEquals(table2, checkConstraint.getTable());
        Assert.assertEquals("TAB2_CHECK", checkConstraint.getName());
        Assert.assertArrayEquals(new String[]{"C3"}, checkConstraint.getColumns());
        Assert.assertTrue(checkConstraint.getEnabled());
        Assert.assertEquals("DV.TABLE2.C3 IS NOT NULL", checkConstraint.getCondition());
        Assert.assertEquals(checkConstraint, table2.getCheckConstraint("TAB2_CHECK"));
        Assert.assertNull(table2.getCheckConstraint("XXX"));

        Assert.assertNotNull(table2.getImportedKeys());
        Assert.assertTrue(table2.getImportedKeys().isEmpty());

        Collection<ExportedKey> exportedKeys = table2.getExportedKeys();
        Assert.assertNotNull(exportedKeys);
        Assert.assertEquals(1, exportedKeys.size());
        ExportedKey exportedKey = exportedKeys.iterator().next();
        Assert.assertNotNull(exportedKey);
        Assert.assertEquals(table2, exportedKey.getTable());
        Assert.assertEquals("TAB2_PK", exportedKey.getName());
        Assert.assertArrayEquals(new String[]{"C1", "C2"}, exportedKey.getColumns());
        Assert.assertTrue(exportedKey.getEnabled());
        Assert.assertEquals("DV", exportedKey.getFkTableSchema());
        Assert.assertEquals("TABLE3", exportedKey.getFkTableName());
        Assert.assertEquals("TAB3_FK", exportedKey.getFkName());
        Assert.assertArrayEquals(new String[]{"X1", "X2"}, exportedKey.getFkColumns());
        Assert.assertEquals(exportedKey, table2.getExportedKey("TAB3_FK"));
        Assert.assertNull(table2.getExportedKey("XXX"));

        Table table3 = schemaConnection.getSchema().getTable("TABLE3");
        Assert.assertNotNull(table3);
        Assert.assertEquals("TABLE3", table3.getName());

        Assert.assertNull(table3.getPrimaryKey());
        Assert.assertTrue(table3.getCheckConstraints().isEmpty());
        Assert.assertTrue(table3.getExportedKeys().isEmpty());

        Collection<ImportedKey> importedKeys = table3.getImportedKeys();
        Assert.assertNotNull(importedKeys);
        Assert.assertEquals(1, importedKeys.size());
        ImportedKey importedKey = importedKeys.iterator().next();
        Assert.assertNotNull(importedKey);
        Assert.assertEquals(table3, importedKey.getTable());
        Assert.assertEquals("TAB3_FK", importedKey.getName());
        Assert.assertArrayEquals(new String[]{"X1", "X2"}, importedKey.getColumns());

        Assert.assertTrue(importedKey.getEnabled());
        Assert.assertEquals("DV", importedKey.getPkTableSchema());
        Assert.assertEquals("TABLE2", importedKey.getPkTableName());
        Assert.assertEquals("TAB2_PK", importedKey.getPkName());
        Assert.assertArrayEquals(new String[]{"C1", "C2"}, importedKey.getPkColumns());
        Assert.assertEquals(importedKey, table3.getImportedKey("TAB3_FK"));
        Assert.assertNull(table2.getImportedKey("XXX"));
    }

    @Test
    public void getTableWithIndices() {
        Table table4 = schemaConnection.getSchema().getTable("TABLE4");
        Assert.assertNotNull(table4);
        Assert.assertEquals("TABLE4", table4.getName());

        Collection<Index> indices = table4.getIndices();
        Assert.assertNotNull(indices);
        Assert.assertEquals(2, indices.size());

        Index index = table4.getIndex("T4_INDEX");
        Assert.assertNotNull(index);
        Assert.assertEquals(table4, index.getTable());
        Assert.assertEquals("T4_INDEX", index.getName());
        Assert.assertEquals(IndexType.NORMAL, index.getType());
        Assert.assertTrue(index.getEnabled());
        Assert.assertArrayEquals(new String[]{"C1", "C2"}, index.getColumns());

        Index unique = table4.getIndex("T4_UNIQUE");
        Assert.assertNotNull(unique);
        Assert.assertEquals("T4_UNIQUE", unique.getName());
        Assert.assertEquals(IndexType.UNIQUE, unique.getType());
        Assert.assertArrayEquals(new String[]{"C3"}, unique.getColumns());

        Assert.assertNull(table4.getIndex("XXX"));
    }

    private void assertColumn(Column actualColumn, int expectedNumber, String expectedName, ColumnType expectedType) {
        Assert.assertNotNull(actualColumn);
        Assert.assertEquals(expectedNumber, actualColumn.getNumber());
        Assert.assertEquals(expectedName, actualColumn.getName());
        Assert.assertEquals(expectedType, actualColumn.getColumnType());
    }

    private void assertColumn(Column actualColumn, int expectedNumber, String expectedName, ColumnType expectedType, String expectedComment, String expectedDefaultValue) {
        assertColumn(actualColumn, expectedNumber, expectedName, expectedType);
        Assert.assertEquals(expectedComment, actualColumn.getComment());
        Assert.assertEquals(expectedDefaultValue, actualColumn.getDefaultValue());
    }

    private void assertColumn(Column actualColumn, int expectedNumber, String expectedName, ColumnType expectedType, int expectedSize) {
        assertColumn(actualColumn, expectedNumber, expectedName, expectedType);
        Assert.assertEquals(expectedSize, actualColumn.getSize());
    }

    private void assertColumn(Column actualColumn, int expectedNumber, String expectedName, ColumnType expectedType, int expectedSize, int expectedScale) {
        assertColumn(actualColumn, expectedNumber, expectedName, expectedType, expectedSize);
        Assert.assertEquals(expectedScale, actualColumn.getScale());
    }
}