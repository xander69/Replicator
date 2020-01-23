package ru.xander.replicator;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import ru.xander.replicator.listener.ReplicatorListener;

import java.io.File;

@Ignore
public class ReplicatorTest {

    private Schema source;
    private Schema target;
    private Replicator replicator;

    @Before
    public void setUp() {
        source = SchemaFactory.create(SchemaOptionsFactory.createSourceOracle());
        target = SchemaFactory.create(SchemaOptionsFactory.createTargetOracle());
        replicator = new Replicator(source, target, ReplicatorListener.stdout);
    }

    @After
    public void tearDown() throws Exception {
        source.close();
        target.close();
    }

    @Test
    public void replicate() {
    }

    @Test
    public void drop() {
    }

    @Test
    public void dump() {
        replicator.dump("FX_RS_TYPEREPORT", System.out, new DumpOptions());
    }

    @Test
    public void pump() {
        replicator.pump(new File("d:\\SQL\\dump.sql"));
    }
}