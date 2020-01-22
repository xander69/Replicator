package ru.xander.replicator.listener;

public class Alter {

    private AlterType eventType;
    private String objectName;
    private String extra;

    public Alter() {
    }

    public Alter(AlterType eventType, String objectName) {
        this.eventType = eventType;
        this.objectName = objectName;
    }

    public Alter(AlterType eventType, String objectName, String extra) {
        this.eventType = eventType;
        this.objectName = objectName;
        this.extra = extra;
    }

    public AlterType getEventType() {
        return eventType;
    }

    public void setEventType(AlterType eventType) {
        this.eventType = eventType;
    }

    public String getObjectName() {
        return objectName;
    }

    public void setObjectName(String objectName) {
        this.objectName = objectName;
    }

    public String getExtra() {
        return extra;
    }

    public void setExtra(String extra) {
        this.extra = extra;
    }
}
