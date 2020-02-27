package ru.xander.replicator.action;

import ru.xander.replicator.exception.ReplicatorException;
import ru.xander.replicator.schema.Schema;
import ru.xander.replicator.schema.SchemaConfig;
import ru.xander.replicator.schema.SchemaFactory;

import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

public interface Action {
    default void withSchema(SchemaConfig schemaConfig, Consumer<Schema> consumer) {
        Schema schema = null;
        try {
            schema = SchemaFactory.getInstance().create(schemaConfig);
            consumer.accept(schema);
        } finally {
            silentClose(schema);
        }
    }

    default <R> R withSchemaAndReturn(SchemaConfig schemaConfig, Function<Schema, R> function) {
        Schema schema = null;
        try {
            schema = SchemaFactory.getInstance().create(schemaConfig);
            return function.apply(schema);
        } finally {
            silentClose(schema);
        }
    }

    default void withTwoSchemas(SchemaConfig sourceConfig, SchemaConfig targetConfig, BiConsumer<Schema, Schema> consumer) {
        Schema source = null;
        Schema target = null;
        try {
            source = SchemaFactory.getInstance().create(sourceConfig);
            target = SchemaFactory.getInstance().create(targetConfig);
            consumer.accept(source, target);
        } finally {
            silentClose(source);
            silentClose(target);
        }
    }

    default <R> R withTwoSchemasAndReturn(SchemaConfig sourceConfig, SchemaConfig targetConfig, BiFunction<Schema, Schema, R> function) {
        Schema source = null;
        Schema target = null;
        try {
            source = SchemaFactory.getInstance().create(sourceConfig);
            target = SchemaFactory.getInstance().create(targetConfig);
            return function.apply(source, target);
        } finally {
            silentClose(source);
            silentClose(target);
        }
    }

    default void silentClose(Schema schema) {
        try {
            if (schema != null) {
                schema.close();
            }
        } catch (Exception e) {
            throw new ReplicatorException(e.getMessage(), e);
        }
    }
}
