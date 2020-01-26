package ru.xander.replicator;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import ru.xander.replicator.compare.CompareResult;
import ru.xander.replicator.compare.CompareResultType;
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
        replicator.replicate("F_ASFK_RASHKASSAYEAR", false);
    }

    @Test
    public void drop() {
    }

    @Test
    public void compare() {
        final String tableName = "F_ASFK_RASHKASSAYEAR";
        CompareResult compareResult = replicator.compare(tableName);
        if (compareResult.getResultType() == CompareResultType.ABSENT_ON_SOURCE) {
            System.out.println("Table " + tableName + " absent on source");
        } else if (compareResult.getResultType() == CompareResultType.ABSENT_ON_TARGET) {
            System.out.println("Table " + tableName + " absent on target");
        } else if (compareResult.getResultType() == CompareResultType.EQUALS) {
            System.out.println("Table " + tableName + " equals");
        } else {
            compareResult.getDiffs().forEach(diff -> {
                System.out.println(diff.getKind() + ": "
                        + '\'' + diff.getSourceValue() + "', "
                        + '\'' + diff.getTargetValue() + '\'');
            });
        }
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