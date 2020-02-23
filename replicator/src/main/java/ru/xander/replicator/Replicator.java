package ru.xander.replicator;

import ru.xander.replicator.action.CompareActionConfigurer;
import ru.xander.replicator.action.DropActionConfigurer;
import ru.xander.replicator.action.DumpActionConfigurer;
import ru.xander.replicator.action.PumpActionConfigurer;
import ru.xander.replicator.action.ReplicateActionConfigurer;
import ru.xander.replicator.action.TableListActionConfigurer;

/**
 * @author Alexander Shakhov
 */
public final class Replicator {
    public static TableListActionConfigurer tableList() {
        return new TableListActionConfigurer();
    }

    public static ReplicateActionConfigurer replicate() {
        return new ReplicateActionConfigurer();
    }

    public static DropActionConfigurer drop() {
        return new DropActionConfigurer();
    }

    public static CompareActionConfigurer compare() {
        return new CompareActionConfigurer();
    }

    public static DumpActionConfigurer dump() {
        return new DumpActionConfigurer();
    }

    public static PumpActionConfigurer pump() {
        return new PumpActionConfigurer();
    }
}
