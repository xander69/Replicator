package ru.xander.replicator.action;

import org.junit.Ignore;
import org.junit.Test;
import ru.xander.replicator.TestUtils;

@Ignore
public class DropActionTest {
    @Test
    public void execute() {
        ReplicatorActions.drop()
                .execute("SAMPLE_TABLE", DropConfig.builder()
                        .schemaConfig(TestUtils.targetSchemaOracle())
                        .build());
    }
}