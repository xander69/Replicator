package ru.xander.replicator;

import ru.xander.replicator.listener.Alter;
import ru.xander.replicator.listener.Listener;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author Alexander Shakhov
 */
public class TestListener implements Listener {

    private static final String RED = "\u001B[31m";
    private static final String RESET = "\u001B[0m";
    private static final String BLUE = "\u001B[34m";
    private static final String BRIGHT_BLACK = "\u001B[90m";

    private final DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    @Override
    public void notify(String message) {
        printTime();
        System.out.println(message);
    }

    @Override
    public void error(Exception e, String sql) {
        printTime();
        System.out.print(RED);
        System.out.print(e.getMessage());
        if (sql != null) {
            System.out.println();
            System.out.print(sql);
        }
        System.out.println(RESET);
    }

    @Override
    public void alter(Alter event) {
        printTime();
        System.out.print(BLUE);
        System.out.print(event.getType() + ": " + event.getTableName());
        if (event.getObjectName() != null) {
            System.out.print("." + event.getObjectName());
        }
        if (event.getExtra() != null) {
            System.out.print(" (" + event.getExtra() + ")");
        }
        System.out.println(RESET);
    }

    private void printTime() {
        System.out.print(dateFormat.format(new Date(System.currentTimeMillis())) + " - ");
    }
}
