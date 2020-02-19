package ru.xander.replicator.schema.oracle;

import org.junit.Ignore;
import org.junit.Test;
import ru.xander.replicator.TestUtils;
import ru.xander.replicator.schema.SchemaConfig;
import ru.xander.replicator.schema.SchemaConnection;
import ru.xander.replicator.schema.Table;

/**
 * @author Alexander Shakhov
 */
@Ignore
public class OracleSchemaTest {
    @Test
    public void getTable() {
        SchemaConfig schemaConfig = TestUtils.sourceSchemaOracle();
        try (SchemaConnection schemaConnection = new SchemaConnection(schemaConfig)) {
            Table table = schemaConnection.getSchema().getTable("D_EB_KD");
            TestUtils.printTable(table);
        }
    }
}