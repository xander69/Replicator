package ru.xander.replicator.application.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import ru.xander.replicator.application.entity.SchemaEntity;

public interface SchemaEntityRepo extends JpaRepository<SchemaEntity, Long> {
}
