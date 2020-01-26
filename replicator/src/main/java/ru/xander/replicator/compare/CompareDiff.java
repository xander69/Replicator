package ru.xander.replicator.compare;

public class CompareDiff {
    private final CompareKind kind;
    private final String sourceValue;
    private final String targetValue;

    public CompareDiff(CompareKind kind, String sourceValue, String targetValue) {
        this.kind = kind;
        this.sourceValue = sourceValue;
        this.targetValue = targetValue;
    }

    public CompareKind getKind() {
        return kind;
    }

    public String getSourceValue() {
        return sourceValue;
    }

    public String getTargetValue() {
        return targetValue;
    }
}
