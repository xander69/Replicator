package ru.xander.replicator;

import org.junit.Ignore;
import org.junit.Test;
import ru.xander.replicator.compare.CompareResult;
import ru.xander.replicator.compare.CompareResultType;

/**
 * @author Alexander Shakhov
 */
@Ignore
public class ReplicatorTest {

    @Test
    public void replicate() {
        new Replicator().replicate("D_EB_KD", ReplicateConfig.builder()
                .sourceConfig(TestUtils.sourceSchemaOracle())
                .targetConfig(TestUtils.targetSchemaOracle())
                .updateImported(true)
                .build());
    }

    @Test
    public void drop() {
        new Replicator().drop("D_EB_KD", DropConfig.builder()
                .schemaConfig(TestUtils.targetSchemaOracle())
                .build());
    }

    @Test
    public void compare() {
//        printCompareResult(replicator.compare("F_ASFK_MEJBUDG_TSEL"));
    }

    @Test
    public void dump() {
//        DumpOptions dumpOptions = new DumpOptions();
//        dumpOptions.setDumpDdl(true);
//        dumpOptions.setDumpDml(true);
//        replicator.dump("SAMPLE_TABLE", System.out, dumpOptions);
    }

    @Test
    public void pump() {
//        replicator.pump(new File("d:\\SQL\\dump.sql"));
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