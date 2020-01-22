package ru.xander.replicator.schema;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class Ddl {
    private String table;
    private List<String> constraints;
    private List<String> indices;
    private List<String> triggers;
    private String sequence;

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public List<String> getConstraints() {
        if (constraints == null) {
            return Collections.emptyList();
        }
        return constraints;
    }

    public void addConstraints(String constraint) {
        if (constraints == null) {
            constraints = new LinkedList<>();
        }
        constraints.add(constraint);
    }

    public List<String> getIndices() {
        if (indices == null) {
            return Collections.emptyList();
        }
        return indices;
    }

    public void addIndex(String index) {
        if (indices == null) {
            indices = new LinkedList<>();
        }
        this.indices.add(index);
    }

    public List<String> getTriggers() {
        if (triggers == null) {
            return Collections.emptyList();
        }
        return triggers;
    }

    public void addTrigger(String trigger) {
        if (triggers == null) {
            triggers = new LinkedList<>();
        }
        triggers.add(trigger);
    }

    public String getSequence() {
        return sequence;
    }

    public void setSequence(String sequence) {
        this.sequence = sequence;
    }
}
