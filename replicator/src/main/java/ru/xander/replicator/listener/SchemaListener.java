package ru.xander.replicator.listener;

import java.time.LocalDateTime;

public interface SchemaListener {
    default void event(String message) {
        // do nothing
    }

    SchemaListener stub = new SchemaListener() {
    };

    SchemaListener stdout = new SchemaListener() {
        @Override
        public void event(String message) {
            System.out.println(LocalDateTime.now() + " - " + message);
        }
    };
}
