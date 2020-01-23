package ru.xander.replicator.listener;

public class Progress {
    private long value;
    private long total;
    private String message;

    public Progress() {
    }

    public Progress(long value, long total, String message) {
        this.value = value;
        this.total = total;
        this.message = message;
    }

    public long getValue() {
        return value;
    }

    public void setValue(long value) {
        this.value = value;
    }

    public long getTotal() {
        return total;
    }

    public void setTotal(long total) {
        this.total = total;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
