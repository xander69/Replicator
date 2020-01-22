package ru.xander.replicator.listener;

public final class DummyListener implements ReplicatorListener {

    private static final DummyListener instance = new DummyListener();

    private DummyListener() {
    }

    @Override
    public void warning(String message) {
        // do nothing
    }

    @Override
    public void alter(Alter event) {
        // do nothing
    }

    public static DummyListener getInstance() {
        return instance;
    }
}
