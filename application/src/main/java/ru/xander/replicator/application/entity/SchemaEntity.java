package ru.xander.replicator.application.entity;

import lombok.Data;
import lombok.NoArgsConstructor;
import ru.xander.replicator.schema.VendorType;

import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name = "rep_schema")
@Data
@NoArgsConstructor
public class SchemaEntity {
    @Id
    private Long id;
    private String name;
    @Enumerated(EnumType.STRING)
    private VendorType vendor;
    private String jdbcDriver;
    private String jdbcUrl;
    private String username;
    private String password;
    private String workSchema;
}
