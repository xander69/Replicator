package ru.xander.replicator.action;

import ru.xander.replicator.schema.SchemaConfig;

/**
 * @author Alexander Shakhov
 */
public class ReplicateActionConfigurer implements ActionConfigurer<ReplicateAction> {

    private static final boolean DEFAULT_UPDATE_IMPORTED = false;

    /**
     * Конфигурация схемы-источника.
     */
    private SchemaConfig sourceConfig;

    /**
     * Конфигурация схемы-приемника.
     */
    private SchemaConfig targetConfig;

    /**
     * Обновлять зависимости.
     */
    private boolean updateImported = DEFAULT_UPDATE_IMPORTED;

    /**
     * Список таблиц для репликации.
     */
    private String[] tables;

    public ReplicateActionConfigurer sourceConfig(SchemaConfig sourceConfig) {
        this.sourceConfig = sourceConfig;
        return this;
    }

    public ReplicateActionConfigurer targetConfig(SchemaConfig targetConfig) {
        this.targetConfig = targetConfig;
        return this;
    }

    public ReplicateActionConfigurer updateImported(boolean updateImported) {
        this.updateImported = updateImported;
        return this;
    }

    public ReplicateActionConfigurer tables(String... tables) {
        this.tables = tables;
        return this;
    }

    @Override
    public ReplicateAction configure() {
        return new ReplicateAction(sourceConfig, targetConfig, updateImported, tables);
    }
}
