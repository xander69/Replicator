package ru.xander.replicator.action;

import org.junit.Ignore;
import org.junit.Test;
import ru.xander.replicator.TestUtils;

@Ignore
public class ReplicateActionTest {
    @Test
    public void execute() {
        ReplicatorActions.replicate()
                .execute("SAMPLE_TABLE",
                        ReplicateConfig.builder()
                                .sourceConfig(TestUtils.sourceSchemaOracle())
                                .targetConfig(TestUtils.targetSchemaOracle())
                                .updateImported(false)
                                .build());
    }
}