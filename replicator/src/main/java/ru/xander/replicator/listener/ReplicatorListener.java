package ru.xander.replicator.listener;

public interface ReplicatorListener {

    void warning(String message);

    void alter(Alter event);

}
