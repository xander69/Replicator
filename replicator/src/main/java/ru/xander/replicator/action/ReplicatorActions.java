package ru.xander.replicator.action;

public abstract class ReplicatorActions {
    public static ReplicateAction replicate() {
        return new ReplicateAction();
    }

    public static DropAction drop() {
        return new DropAction();
    }

    public static CompareAction compare() {
        return new CompareAction();
    }

    public static DumpAction dump() {
        return new DumpAction();
    }

    public static PumpAction pump() {
        return new PumpAction();
    }
}
