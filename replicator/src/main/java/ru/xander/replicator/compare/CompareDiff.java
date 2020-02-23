package ru.xander.replicator.compare;

public class CompareDiff {
    private CompareKind kind;
    private String sourceValue;
    private String targetValue;
    private String alter;

    public CompareKind getKind() {
        return kind;
    }

    public void setKind(CompareKind kind) {
        this.kind = kind;
    }

    public String getSourceValue() {
        return sourceValue;
    }

    public void setSourceValue(String sourceValue) {
        this.sourceValue = sourceValue;
    }

    public String getTargetValue() {
        return targetValue;
    }

    public void setTargetValue(String targetValue) {
        this.targetValue = targetValue;
    }

    public String getAlter() {
        return alter;
    }

    public void setAlter(String alter) {
        this.alter = alter;
    }
}
