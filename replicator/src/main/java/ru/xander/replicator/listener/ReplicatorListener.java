package ru.xander.replicator.listener;

import java.time.LocalDateTime;

public interface ReplicatorListener {

    default void warning(String message) {
        // do nothing
    }

    default void alter(Alter event) {
        // do nothing
    }

    default void progress(Progress progress) {
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

        @Override
        public void progress(Progress progress) {
            if (progress.getMessage() != null) {
                System.out.print(progress.getMessage() + ": ");
            }
            if (progress.getTotal() != 0) {
                long percent = (long) (progress.getValue() / (double) progress.getTotal() * 100);
                System.out.println(progress.getValue() + " / " + progress.getTotal() + " (" + percent + "%)");
            } else {
                System.out.println(progress.getValue() + " / " + progress.getTotal());
            }
        }
    };
}
