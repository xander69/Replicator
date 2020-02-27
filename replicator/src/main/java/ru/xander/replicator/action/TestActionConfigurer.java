package ru.xander.replicator.action;

import ru.xander.replicator.schema.SchemaConfig;

/**
 * @author Alexander Shakhov
 */
public class TestActionConfigurer implements ActionConfigurer<TestAction> {

    /**
     * Конфигурация схемы.
     */
    private SchemaConfig schemaConfig;

    public TestActionConfigurer schemaConfig(SchemaConfig schemaConfig) {
        this.schemaConfig = schemaConfig;
        return this;
    }

    @Override
    public TestAction configure() {
        return new TestAction(schemaConfig);
    }
}
