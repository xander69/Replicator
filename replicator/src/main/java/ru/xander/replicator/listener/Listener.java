package ru.xander.replicator.listener;

public interface Listener {

    default void notify(String message) {
        // do nothing
    }

    default void error(Exception e, String sql) {

    }

    default void alter(Alter event) {
        // do nothing
    }
}
