package ru.xander.replicator.listener;

import java.time.LocalDateTime;

public interface ReplicatorListener {

    default void warning(String message) {
        // do nothing
    }

    default void alter(Alter event) {
        // do nothing
    }

    ReplicatorListener stub = new ReplicatorListener() {
    };

    ReplicatorListener stdout = new ReplicatorListener() {
        @Override
        public void warning(String message) {
            System.out.println(LocalDateTime.now() + " - Warning: " + message);
        }

        @Override
        public void alter(Alter event) {
            System.out.print(LocalDateTime.now() + " - Alter: " + event.getEventType() + " for " + event.getObjectName());
            if (event.getExtra() != null) {
                System.out.print(" (" + event.getExtra() + ")");
            }
            System.out.println();
        }
    };
}
