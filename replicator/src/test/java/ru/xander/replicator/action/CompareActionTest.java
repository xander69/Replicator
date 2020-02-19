package ru.xander.replicator.action;

import org.junit.Ignore;
import org.junit.Test;
import ru.xander.replicator.TestUtils;
import ru.xander.replicator.compare.CompareResult;
import ru.xander.replicator.compare.CompareResultType;

@Ignore
public class CompareActionTest {
    @Test
    public void execute() {
        CompareResult compareResult = ReplicatorActions.compare()
                .execute("SAMPLE_TABLE",
                        CompareConfig.builder()
                                .sourceConfig(TestUtils.sourceSchemaOracle())
                                .targetConfig(TestUtils.targetSchemaOracle())
                                .build());
        printCompareResult(compareResult);
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