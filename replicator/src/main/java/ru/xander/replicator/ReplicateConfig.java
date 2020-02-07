package ru.xander.replicator;

import ru.xander.replicator.listener.Listener;

/**
 * @author Alexander Shakhov
 */
public class ReplicateConfig {

    /**
     * Слушатель событий.
     */
    private Listener listener;
    /**
     * Конфигурация схемы-источника.
     */
    private SchemaConfig sourceConfig;
    /**
     * Конфигурация схемы-приемника.
     */
    private SchemaConfig targetConfig;

    public Listener getListener() {
        return listener;
    }

    public void setListener(Listener listener) {
        this.listener = listener;
    }

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

    public static ReplicateConfigBuilder builder() {
        return new ReplicateConfigBuilder();
    }

    public static class ReplicateConfigBuilder {
        private Listener listener;
        private SchemaConfig sourceConfig;
        private SchemaConfig targetConfig;

        private ReplicateConfigBuilder() {
        }

        public ReplicateConfigBuilder listener(Listener listener) {
            this.listener = listener;
            return this;
        }

        public ReplicateConfigBuilder sourceConfig(SchemaConfig sourceConfig) {
            this.sourceConfig = sourceConfig;
            return this;
        }

        public ReplicateConfigBuilder targetConfig(SchemaConfig targetConfig) {
            this.targetConfig = targetConfig;
            return this;
        }

        public ReplicateConfig build() {
            ReplicateConfig replicateConfig = new ReplicateConfig();
            replicateConfig.setListener(listener);
            replicateConfig.setSourceConfig(sourceConfig);
            replicateConfig.setTargetConfig(targetConfig);
            return replicateConfig;
        }
    }
}
