package ru.xander.replicator;

/**
 * @author Alexander Shakhov
 */
public class ReplicateConfig {

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

    public SchemaConfig getSourceConfig() {
        return sourceConfig;
    }

    public void setSourceConfig(SchemaConfig sourceConfig) {
        this.sourceConfig = sourceConfig;
    }

    public SchemaConfig getTargetConfig() {
        return targetConfig;
    }

    public void setTargetConfig(SchemaConfig targetConfig) {
        this.targetConfig = targetConfig;
    }

    public boolean isUpdateImported() {
        return updateImported;
    }

    public void setUpdateImported(boolean updateImported) {
        this.updateImported = updateImported;
    }

    public static ReplicateConfigBuilder builder() {
        return new ReplicateConfigBuilder();
    }

    public static class ReplicateConfigBuilder {
        private SchemaConfig sourceConfig;
        private SchemaConfig targetConfig;
        private boolean updateImported = DEFAULT_UPDATE_IMPORTED;

        private ReplicateConfigBuilder() {
        }

        public ReplicateConfigBuilder sourceConfig(SchemaConfig sourceConfig) {
            this.sourceConfig = sourceConfig;
            return this;
        }

        public ReplicateConfigBuilder targetConfig(SchemaConfig targetConfig) {
            this.targetConfig = targetConfig;
            return this;
        }

        public ReplicateConfigBuilder updateImported(boolean updateImported) {
            this.updateImported = updateImported;
            return this;
        }

        public ReplicateConfig build() {
            ReplicateConfig replicateConfig = new ReplicateConfig();
            replicateConfig.setSourceConfig(sourceConfig);
            replicateConfig.setTargetConfig(targetConfig);
            replicateConfig.setUpdateImported(updateImported);
            return replicateConfig;
        }
    }
}
