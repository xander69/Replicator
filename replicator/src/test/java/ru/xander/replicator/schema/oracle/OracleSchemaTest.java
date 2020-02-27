package ru.xander.replicator.schema.oracle;

import org.junit.Ignore;
import org.junit.Test;
import ru.xander.replicator.TestUtils;
import ru.xander.replicator.schema.Schema;
import ru.xander.replicator.schema.SchemaFactory;
import ru.xander.replicator.schema.Table;

/**
 * @author Alexander Shakhov
 */
@Ignore
public class OracleSchemaTest {
    @Test
    public void getTable() throws Exception {
        try (Schema schema = SchemaFactory.getInstance().create(TestUtils.sourceSchemaOracle())) {
            Table table = schema.getTable("SAMPLE_TABLE");
            TestUtils.printTable(table);
        }
    }
}