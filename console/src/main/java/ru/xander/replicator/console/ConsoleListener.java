package ru.xander.replicator.console;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.xander.replicator.listener.Alter;
import ru.xander.replicator.listener.Listener;

/**
 * @author Alexander Shakhov
 */
public class ConsoleListener implements Listener {

    private static final Logger log = LoggerFactory.getLogger(ConsoleListener.class);

    private final String schema;

    ConsoleListener(String schema) {
        this.schema = schema;
    }

    @Override
    public void warning(String message) {
        log.warn(message);
    }

    @Override
    public void alter(Alter event) {
        if (event.getExtra() == null) {
            log.info("<{}> {}: {}", schema, event.getType(), event.getObjectName());
        } else {
            log.info("<{}> {}: {} ({})", schema, event.getType(), event.getObjectName(), event.getExtra());
        }
    }

    @Override
    public void notify(String message) {
        log.info("<{}> {}", schema, message);
    }

    @Override
    public void error(Exception e, String sql) {
        if (sql == null) {
            log.error("<{}> {}", schema, e.getMessage());
        } else {
            log.error("<{}> {}\n{}", schema, e.getMessage(), sql);
        }
    }

    //    @Override
//    public void progress(Progress progress) {
//        long percent = (progress.getTotal() > 0) ? (long) (progress.getValue() / (double) progress.getTotal() * 100) : 0L;
//        if (progress.getMessage() == null) {
//            System.out.printf("%d/%d (%d %%)\n", progress.getValue(), progress.getTotal(), percent);
//        } else {
//            System.out.printf("%s: %d/%d (%d %%)\n", progress.getMessage(), progress.getValue(), progress.getTotal(), percent);
//        }
//    }
//
//    @Override
//    public void event(String message) {
//        log.info("<{}> {}", schema, message);
//    }
}
