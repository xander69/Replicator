package ru.xander.replicator;

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

    public static class DropConfigBuilder {
        private SchemaConfig schemaConfig;
        private boolean dropExported = DEFAULT_DROP_EXPORTED;

        private DropConfigBuilder() {
        }

        public DropConfigBuilder schemaConfig(SchemaConfig schemaConfig) {
            this.schemaConfig = schemaConfig;
            return this;
        }

        public DropConfigBuilder dropExported(boolean dropExported) {
            this.dropExported = dropExported;
            return this;
        }

        public DropConfig build() {
            DropConfig replicateConfig = new DropConfig();
            replicateConfig.setSchemaConfig(schemaConfig);
            replicateConfig.setDropExported(dropExported);
            return replicateConfig;
        }
    }
}
