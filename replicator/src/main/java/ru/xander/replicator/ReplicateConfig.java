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
    /**
     * Выполнять репликацию структуры.
     */
    private boolean replicateDdl = true;
    /**
     * Выполнять репликацию данных.
     */
    private boolean replicateDml = false;
    /**
     * Реплицировать зависимые таблицы.
     */
    private boolean replicateImported = false;
    /**
     * Обновлять зависимости.
     */
    private boolean updateExported = true;

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

    public boolean isReplicateDdl() {
        return replicateDdl;
    }

    public void setReplicateDdl(boolean replicateDdl) {
        this.replicateDdl = replicateDdl;
    }

    public boolean isReplicateDml() {
        return replicateDml;
    }

    public void setReplicateDml(boolean replicateDml) {
        this.replicateDml = replicateDml;
    }

    public boolean isReplicateImported() {
        return replicateImported;
    }

    public void setReplicateImported(boolean replicateImported) {
        this.replicateImported = replicateImported;
    }

    public boolean isUpdateExported() {
        return updateExported;
    }

    public void setUpdateExported(boolean updateExported) {
        this.updateExported = updateExported;
    }
}
