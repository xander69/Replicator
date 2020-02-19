package ru.xander.replicator.action;

import ru.xander.replicator.schema.SchemaConfig;

/**
 * @author Alexander Shakhov
 */
public class CompareConfig {

    /**
     * Конфигурация схемы-источника.
     */
    private SchemaConfig sourceConfig;
    /**
     * Конфигурация схемы-приемника.
     */
    private SchemaConfig targetConfig;

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

    public static CompareConfigBuilder builder() {
        return new CompareConfigBuilder();
    }

    public static class CompareConfigBuilder extends CompareConfig {
        private CompareConfigBuilder() {
        }

        public CompareConfigBuilder sourceConfig(SchemaConfig sourceConfig) {
            this.setSourceConfig(sourceConfig);
            return this;
        }

        public CompareConfigBuilder targetConfig(SchemaConfig targetConfig) {
            this.setTargetConfig(targetConfig);
            return this;
        }

        public CompareConfig build() {
            CompareConfig compareConfig = new CompareConfig();
            compareConfig.setSourceConfig(this.getSourceConfig());
            compareConfig.setTargetConfig(this.getTargetConfig());
            return compareConfig;
        }
    }
}
