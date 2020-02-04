package ru.xander.replicator;

import ru.xander.replicator.listener.Alter;
import ru.xander.replicator.listener.Progress;
import ru.xander.replicator.listener.ReplicatorListener;

/**
 * @author Alexander Shakhov
 */
public class TestReplicatorListener implements ReplicatorListener {

    private static final String RED = "\u001B[31m";
    private static final String RESET = "\u001B[0m";
    private static final String BLUE = "\u001B[34m";
    private static final String BRIGHT_BLACK = "\u001B[90m";

    private final ReplicatorListener delegate = ReplicatorListener.stdout;

    @Override
    public void warning(String message) {
        System.out.print(RED);
        delegate.warning(message);
        System.out.print(RESET);
    }

    @Override
    public void alter(Alter event) {
        System.out.print(BLUE);
        delegate.alter(event);
        System.out.print(RESET);
    }

    @Override
    public void progress(Progress progress) {
        System.out.print(BRIGHT_BLACK);
        delegate.progress(progress);
        System.out.print(RESET);
    }
}
