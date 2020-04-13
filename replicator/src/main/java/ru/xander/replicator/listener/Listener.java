package ru.xander.replicator.listener;

/**
 * @author Alexander Shakhov
 */
public interface Listener {
    default void notify(String message) {
        // do nothing
    }

    default void warning(String message) {
        // do nothing
    }

    default void error(Exception e) {
        // do nothing
    }

    default void errorSql(Exception e, String sql) {
        // do nothing
    }

    default void alter(Alter event) {
        // do nothing
    }

    default void progress(Progress progress) {
        // do nothing
    }

    default void modify(Modify modify) {
        // do nothing
    }
}
