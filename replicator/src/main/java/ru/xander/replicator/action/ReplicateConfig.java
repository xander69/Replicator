package ru.xander.replicator.action;

import ru.xander.replicator.schema.SchemaConfig;

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

    public static class ReplicateConfigBuilder extends ReplicateConfig {
        private ReplicateConfigBuilder() {
        }

        public ReplicateConfigBuilder sourceConfig(SchemaConfig sourceConfig) {
            this.setSourceConfig(sourceConfig);
            return this;
        }

        public ReplicateConfigBuilder targetConfig(SchemaConfig targetConfig) {
            this.setTargetConfig(targetConfig);
            return this;
        }

        public ReplicateConfigBuilder updateImported(boolean updateImported) {
            this.setUpdateImported(updateImported);
            return this;
        }

        public ReplicateConfig build() {
            ReplicateConfig replicateConfig = new ReplicateConfig();
            replicateConfig.setSourceConfig(this.getSourceConfig());
            replicateConfig.setTargetConfig(this.getTargetConfig());
            replicateConfig.setUpdateImported(this.isUpdateImported());
            return replicateConfig;
        }
    }
}
