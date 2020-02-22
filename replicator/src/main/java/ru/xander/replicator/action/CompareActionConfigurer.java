package ru.xander.replicator.action;

import ru.xander.replicator.schema.SchemaConfig;

/**
 * @author Alexander Shakhov
 */
public class CompareActionConfigurer implements ActionConfigurer<CompareAction> {

    /**
     * Конфигурация схемы-источника.
     */
    private SchemaConfig sourceConfig;

    /**
     * Конфигурация схемы-приемника.
     */
    private SchemaConfig targetConfig;

    /**
     * Список таблиц для сравнения.
     */
    private String[] tables;

    public CompareActionConfigurer sourceConfig(SchemaConfig sourceConfig) {
        this.sourceConfig = sourceConfig;
        return this;
    }

    public CompareActionConfigurer targetConfig(SchemaConfig targetConfig) {
        this.targetConfig = targetConfig;
        return this;
    }

    public CompareActionConfigurer tables(String... tables) {
        this.tables = tables;
        return this;
    }

    @Override
    public CompareAction configure() {
        return new CompareAction(sourceConfig, targetConfig, tables);
    }
}
