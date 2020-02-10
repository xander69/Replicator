package ru.xander.replicator.application.entity;

import javafx.beans.property.ObjectProperty;
import javafx.beans.property.SimpleObjectProperty;
import javafx.beans.property.SimpleStringProperty;
import javafx.beans.property.StringProperty;
import javafx.scene.image.Image;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Transient;

@Entity
//@Access(AccessType.PROPERTY)
@Table(name = "rep_schema")
@Data
@NoArgsConstructor
@ToString(of = {"id", "name", "vendor"})
public class SchemaEntity {
    @Id
    @GeneratedValue
    private Long id;
    private String name;
    @Transient
    private StringProperty nameProperty = new SimpleStringProperty(this, "name");
    @Enumerated(EnumType.STRING)
    private SchemaVendor vendor;
    @Transient
    private ObjectProperty<Image> vendorProperty = new SimpleObjectProperty<>(this, "vendor");
    private String jdbcDriver;
    private String jdbcUrl;
    private String username;
    private String password;
    private String workSchema;

    public String getName() {
        return nameProperty.get();
    }

    public void setName(String name) {
        this.nameProperty.set(name);
    }

    public StringProperty nameProperty() {
        return nameProperty;
    }

    public SchemaVendor getVendor() {
        return vendor;
    }

    public void setVendor(SchemaVendor vendor) {
        this.vendor = vendor;
        this.vendorProperty.set(vendor.getIcon24());
    }

    public ObjectProperty<Image> vendorProperty() {
        return vendorProperty;
    }
}
