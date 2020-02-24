package ru.xander.replicator;

import org.junit.Ignore;
import org.junit.Test;
import ru.xander.replicator.compare.CompareResult;
import ru.xander.replicator.compare.CompareResultType;
import ru.xander.replicator.dump.DumpType;

import java.io.ByteArrayOutputStream;

/**
 * @author Alexander Shakhov
 */
@Ignore
public class ReplicatorTest {
    @Test
    public void tableList() {
        Replicator.tableList()
                .schemaConfig(TestUtils.sourceSchemaOracle())
                .like("%EXE%")
                .notLike("%$%")
                .configure()
                .execute()
                .forEach(System.out::println);
    }

    @Test
    public void replicate() {
        Replicator.replicate()
                .sourceConfig(TestUtils.sourceSchemaOracle())
                .targetConfig(TestUtils.targetSchemaOracle())
                .updateImported(false)
                .tables("SAMPLE_TABLE")
                .configure()
                .execute();
    }

    @Test
    public void drop() {
        Replicator.drop()
                .schemaConfig(TestUtils.targetSchemaOracle())
                .dropExported(true)
                .tables("SAMPLE_TABLE")
                .configure()
                .execute();
    }

    @Test
    public void compare() {
        Replicator.compare()
                .sourceConfig(TestUtils.sourceSchemaOracle())
                .targetConfig(TestUtils.targetSchemaOracle())
                .tables("SAMPLE_TABLE")
                .configure()
                .execute()
                .forEach(this::printCompareResult);
    }

    @Test
    public void dump() {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        Replicator.dump()
                .schemaConfig(TestUtils.sourceSchemaOracle())
                .outputStream(output)
                .dumpType(DumpType.SQL)
                .tableName("SAMPLE_TABLE")
                .dumpDdl(true)
                .dumpDml(true)
                .verboseEash(4)
                .commitEach(5)
                .configure()
                .execute();
        System.out.println(output.toString());
    }

    private void printCompareResult(String tableName, CompareResult compareResult) {
        System.out.println("======================================================");
        System.out.println("Compare table " + tableName);

        if (compareResult.getResultType() == CompareResultType.ABSENT_ON_SOURCE) {
            System.out.println("Absent on source");
        } else if (compareResult.getResultType() == CompareResultType.ABSENT_ON_TARGET) {
            System.out.println("Absent on target");
        } else if (compareResult.getResultType() == CompareResultType.EQUALS) {
            System.out.println("Equals");
        } else {
            compareResult.getDiffs().forEach(diff ->
                    System.out.println("\n" + diff.getKind() + ": "
                            + '\'' + diff.getSourceValue() + "', "
                            + '\'' + diff.getTargetValue() + "'\n"
                            + diff.getAlter()));
        }
    }
}