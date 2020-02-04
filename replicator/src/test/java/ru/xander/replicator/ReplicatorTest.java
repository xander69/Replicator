package ru.xander.replicator;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import ru.xander.replicator.compare.CompareResult;
import ru.xander.replicator.compare.CompareResultType;

import java.io.File;

/**
 * @author Alexander Shakhov
 */
@Ignore
public class ReplicatorTest {

    private Schema source;
    private Schema target;
    private Replicator replicator;

    @Before
    public void setUp() {
        source = SchemaFactory.create(SchemaOptionsFactory.createSourceOracle());
        target = SchemaFactory.create(SchemaOptionsFactory.createTargetOracle());
        replicator = new Replicator(source, target, new TestReplicatorListener());
    }

    @After
    public void tearDown() throws Exception {
        source.close();
        target.close();
    }

    @Test
    public void replicate() {
        replicator.replicate("SAMPLE_TABLE", true);
    }

    @Test
    public void drop() {
        replicator.drop("SAMPLE_TABLE");
    }

    @Test
    public void compare() {
        printCompareResult(replicator.compare("SAMPLE_TABLE"));
    }

    @Test
    public void dump() {
        DumpOptions dumpOptions = new DumpOptions();
        dumpOptions.setDumpDdl(true);
        dumpOptions.setDumpDml(true);
        replicator.dump("SAMPLE_TABLE", System.out, dumpOptions);
    }

    @Test
    public void pump() {
        replicator.pump(new File("d:\\SQL\\dump.sql"));
    }

    private void printCompareResult(CompareResult compareResult) {
        if (compareResult.getResultType() == CompareResultType.ABSENT_ON_SOURCE) {
            System.out.println("Absent on source");
        } else if (compareResult.getResultType() == CompareResultType.ABSENT_ON_TARGET) {
            System.out.println("Absent on target");
        } else if (compareResult.getResultType() == CompareResultType.EQUALS) {
            System.out.println("Equals");
        } else {
            compareResult.getDiffs().forEach(diff ->
                    System.out.println(diff.getKind() + ": "
                            + '\'' + diff.getSourceValue() + "', "
                            + '\'' + diff.getTargetValue() + '\''));
        }
    }
}