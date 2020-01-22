package ru.xander.replicator;

import java.util.Objects;

public class Replicator {

    private final Schema source;
    private final Schema target;

    public Replicator(Schema source, Schema target) {
        Objects.requireNonNull(source);
        Objects.requireNonNull(target);
        this.source = source;
        this.target = target;
    }

    public void update(String tableName) {
    }
}
