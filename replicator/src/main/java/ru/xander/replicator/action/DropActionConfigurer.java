package ru.xander.replicator.action;

import ru.xander.replicator.schema.SchemaConfig;

/**
 * @author Alexander Shakhov
 */
public class DropActionConfigurer implements ActionConfigurer<DropAction> {

    private static final boolean DEFAULT_DROP_EXPORTED = true;

    /**
     * Конфигурация схемы.
     */
    private SchemaConfig schemaConfig;

    /**
     * Удалять зависимые таблицы. Если false - удаляться будут только констрейнты.
     */
    private boolean dropExported = DEFAULT_DROP_EXPORTED;

    /**
     * Список таблиц для удаления.
     */
    private String[] tables;

    public DropActionConfigurer schemaConfig(SchemaConfig schemaConfig) {
        this.schemaConfig = schemaConfig;
        return this;
    }

    public DropActionConfigurer dropExported(boolean dropExported) {
        this.dropExported = dropExported;
        return this;
    }

    public DropActionConfigurer tables(String... tables) {
        this.tables = tables;
        return this;
    }

    @Override
    public DropAction configure() {
        return new DropAction(schemaConfig, dropExported, tables);
    }
}
