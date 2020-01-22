package ru.xander.replicator;

import org.junit.Test;
import ru.xander.replicator.oracle.OracleSchema;

public class ReplicatorTest {

    @Test
    public void replicate() {
    }

    @Test
    public void drop() {
    }

    @Test
    public void dump() {
        Schema source = new OracleSchema(SchemaOptionsFactory.createSourceOracle());
        Schema target = new OracleSchema(SchemaOptionsFactory.createTargetOracle());
        Replicator replicator = new Replicator(source, target, new TestReplicatorListener());
        replicator.dump("D_EB_KD", System.out);
    }
}