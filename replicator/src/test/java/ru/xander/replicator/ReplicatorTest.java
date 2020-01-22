package ru.xander.replicator;

import org.junit.Test;
import ru.xander.replicator.listener.ReplicatorListener;

public class ReplicatorTest {

    @Test
    public void replicate() {
    }

    @Test
    public void drop() {
    }

    @Test
    public void dump() {
        Schema source = SchemaFactory.create(SchemaOptionsFactory.createSourceOracle());
        Schema target = SchemaFactory.create(SchemaOptionsFactory.createTargetOracle());
        Replicator replicator = new Replicator(source, target, ReplicatorListener.stdout);
        replicator.dump("FX_RS_TYPEREPORT", System.out, new DumpOptions());
    }
}