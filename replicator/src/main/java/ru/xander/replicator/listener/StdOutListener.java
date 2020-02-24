package ru.xander.replicator.listener;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author Alexander Shakhov
 */
public class StdOutListener implements Listener {

    private static final String WHITE_AND_RED_BG = "\u001B[97;41m";
    private static final String RED = "\u001B[31m";
    private static final String RESET = "\u001B[0m";
    private static final String BLUE = "\u001B[34m";
    private static final String GREEN = "\u001B[32m";
    private static final String BRIGHT_BLACK = "\u001B[90m";

    private final DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    private final String name;

    public StdOutListener(String name) {
        this.name = name;
    }

    @Override
    public void notify(String message) {
        printMessage(message);
    }

    @Override
    public void warning(String message) {
        printMessage(RED + "Warning: " + message + RESET);
    }

    @Override
    public void error(Exception e, String sql) {
        String message = WHITE_AND_RED_BG + "Error: " + e.getMessage();
        if (sql != null) {
            message += RESET + "\n" + RED + sql;
        }
        message += RESET;
        printMessage(message);
    }

    @Override
    public void alter(Alter event) {
        String message = BLUE + event.getType() + ": " + event.getTableName();
        if (event.getObjectName() != null) {
            message += "." + event.getObjectName();
        }
        if (event.getExtra() != null) {
            message += " (" + event.getExtra() + ")";
        }
        message += RESET;
        if (event.getSql() != null) {
            message += "\n" + BRIGHT_BLACK + event.getSql() + RESET;
        }
        printMessage(message);
    }

    @Override
    public void progress(Progress progress) {
        int percent = (int) ((double) progress.getValue() / progress.getTotal() * 100.0);
        String message = String.format("%s%s (%d/%d, %d%%)%s", GREEN, progress.getMessage(), progress.getValue(), progress.getTotal(), percent, RESET);
        printMessage(message);
    }

    private synchronized void printMessage(String message) {
        System.out.println(dateFormat.format(new Date(System.currentTimeMillis())) + " <" + name + "> [" + Thread.currentThread().getName() + "] - " + message);
    }
}
