package ru.xander.replicator.compare;

import java.util.List;

public class CompareResult {

    private final CompareResultType resultType;
    private final List<CompareDiff> diffs;

    public CompareResult(CompareResultType resultType, List<CompareDiff> diffs) {
        this.resultType = resultType;
        this.diffs = diffs;
    }

    public CompareResultType getResultType() {
        return resultType;
    }

    public List<CompareDiff> getDiffs() {
        return diffs;
    }

}
