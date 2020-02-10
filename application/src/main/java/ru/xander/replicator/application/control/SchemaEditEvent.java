package ru.xander.replicator.application.control;

import javafx.event.Event;
import lombok.Getter;
import ru.xander.replicator.application.entity.SchemaEntity;

@Getter
public class SchemaEditEvent extends Event {

    private final SchemaAction action;
    private final SchemaEntity schemaEntity;

    public SchemaEditEvent(SchemaAction action, SchemaEntity schemaEntity) {
        super(null);
        this.action = action;
        this.schemaEntity = schemaEntity;
    }

    public enum SchemaAction {
        SAVE,
        EDIT,
        DELETE
    }
}
