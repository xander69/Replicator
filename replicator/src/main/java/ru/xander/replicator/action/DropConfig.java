package ru.xander.replicator.action;

import ru.xander.replicator.schema.SchemaConfig;

/**
 * @author Alexander Shakhov
 */
public class DropConfig {

    private static final boolean DEFAULT_DROP_EXPORTED = true;

    /**
     * Конфигурация схемы.
     */
    private SchemaConfig schemaConfig;
    /**
     * Удалять зависимые таблицы. Если false - удаляться будут только констрейнты.
     */
    private boolean dropExported = DEFAULT_DROP_EXPORTED;

    public SchemaConfig getSchemaConfig() {
        return schemaConfig;
    }

    public void setSchemaConfig(SchemaConfig schemaConfig) {
        this.schemaConfig = schemaConfig;
    }

    public boolean isDropExported() {
        return dropExported;
    }

    public void setDropExported(boolean dropExported) {
        this.dropExported = dropExported;
    }

    public static DropConfigBuilder builder() {
        return new DropConfigBuilder();
    }

    public static class DropConfigBuilder extends DropConfig {
        private DropConfigBuilder() {
        }

        public DropConfigBuilder schemaConfig(SchemaConfig schemaConfig) {
            this.setSchemaConfig(schemaConfig);
            return this;
        }

        public DropConfigBuilder dropExported(boolean dropExported) {
            this.setDropExported(dropExported);
            return this;
        }

        public DropConfig build() {
            DropConfig replicateConfig = new DropConfig();
            replicateConfig.setSchemaConfig(this.getSchemaConfig());
            replicateConfig.setDropExported(this.isDropExported());
            return replicateConfig;
        }
    }
}
