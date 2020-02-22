package ru.xander.replicator.action;

public interface ActionConfigurer<T extends Action> {
    T configure();
}
