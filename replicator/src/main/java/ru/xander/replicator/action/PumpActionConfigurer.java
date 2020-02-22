package ru.xander.replicator.action;

import ru.xander.replicator.schema.SchemaConfig;

import java.io.File;

/**
 * @author Alexander Shakhov
 */
public class PumpActionConfigurer implements ActionConfigurer<PumpAction> {

    /**
     * Конфигурация схемы.
     */
    private SchemaConfig schemaConfig;

    /**
     * Список файлов, содержащих скрипт дампа.
     */
    private File[] scriptFiles;

    public PumpActionConfigurer schemaConfig(SchemaConfig schemaConfig) {
        this.schemaConfig = schemaConfig;
        return this;
    }

    public PumpActionConfigurer scriptFile(File... scriptFiles) {
        this.scriptFiles = scriptFiles;
        return this;
    }

    @Override
    public PumpAction configure() {
        return new PumpAction(schemaConfig, scriptFiles);
    }
}
