package ru.xander.replicator.application.controller;

import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import ru.xander.replicator.application.entity.SchemaEntity;
import ru.xander.replicator.application.entity.SchemaVendor;

public class State {

    private final ObservableList<SchemaEntity> schemas = FXCollections.observableArrayList();

    private static final State instance = new State();

    private State() {
    }

    public ObservableList<SchemaEntity> getSchemas() {
        return schemas;
    }

    public void addSchema(SchemaEntity schemaEntity) {
        schemaEntity.setId((long) schemas.size());
        schemas.add(schemaEntity);
    }

    public SchemaEntity getSchema(long id) {
        return schemas.stream().filter(s -> s.getId().equals(id)).findFirst().orElse(null);
    }

    public void delSchema(SchemaEntity schemaEntity) {
        schemas.remove(schemaEntity);
    }

    public static State getInstance() {
        return instance;
    }

    public static void initialize() {
        SchemaEntity schemaEntity = new SchemaEntity();
        schemaEntity.setId(1L);
        schemaEntity.setName("TEST SCHEMA");
        schemaEntity.setVendor(SchemaVendor.ORACLE);
        schemaEntity.setJdbcDriver(SchemaVendor.ORACLE.getJdbcDriver());
        schemaEntity.setJdbcUrl("jdbc:oracle:thin@mz2:1521:oradi");
        schemaEntity.setUsername("DV");
        schemaEntity.setPassword("DV");
        instance.schemas.add(schemaEntity);
    }
}
