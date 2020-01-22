package ru.xander.replicator;

import ru.xander.replicator.listener.Alter;
import ru.xander.replicator.listener.ReplicatorListener;

import java.time.LocalDateTime;

public class TestReplicatorListener implements ReplicatorListener {

    @Override
    public void warning(String message) {
        System.out.println(LocalDateTime.now() + " - Warning: " + message);
    }

    @Override
    public void alter(Alter event) {
        System.out.println(LocalDateTime.now() + " - Alter: " + event.getEventType() + " for " + event.getObjectName());
    }
}
