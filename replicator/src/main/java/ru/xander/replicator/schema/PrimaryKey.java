package ru.xander.replicator.schema;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * @author Alexander Shakhov
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class PrimaryKey extends Constraint {
}
