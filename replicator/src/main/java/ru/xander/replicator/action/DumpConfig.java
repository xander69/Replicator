package ru.xander.replicator.action;

import ru.xander.replicator.schema.SchemaConfig;

import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * @author Alexander Shakhov
 */
public class DumpConfig {

    /**
     * Конфигурация схемы.
     */
    private SchemaConfig schemaConfig;
    /**
     * Выходной поток для результата.
     */
    private OutputStream outputStream;
    /**
     * Сохранять DDL в дамп
     */
    private boolean dumpDdl = true;
    /**
     * Сохранять данные в дамп
     */
    private boolean dumpDml = true;
    /**
     * Кодировка выходного файла
     */
    private Charset charset = StandardCharsets.UTF_8;
    /**
     * Количество записей, после которого будет генерироваться событие прогресса
     */
    private long verboseEach = 1000L;
    /**
     * Количество записей, после которого будет выполняться коммит.
     * Если 0, то коммит будет только в конце.
     */
    private long commitEach = 1000L;

    public SchemaConfig getSchemaConfig() {
        return schemaConfig;
    }

    public void setSchemaConfig(SchemaConfig schemaConfig) {
        this.schemaConfig = schemaConfig;
    }

    public OutputStream getOutputStream() {
        return outputStream;
    }

    public void setOutputStream(OutputStream outputStream) {
        this.outputStream = outputStream;
    }

    public boolean isDumpDdl() {
        return dumpDdl;
    }

    public void setDumpDdl(boolean dumpDdl) {
        this.dumpDdl = dumpDdl;
    }

    public boolean isDumpDml() {
        return dumpDml;
    }

    public void setDumpDml(boolean dumpDml) {
        this.dumpDml = dumpDml;
    }

    public Charset getCharset() {
        return charset;
    }

    public void setCharset(Charset charset) {
        this.charset = charset;
    }

    public long getVerboseEach() {
        return verboseEach;
    }

    public void setVerboseEach(long verboseEach) {
        this.verboseEach = verboseEach;
    }

    public long getCommitEach() {
        return commitEach;
    }

    public void setCommitEach(long commitEach) {
        this.commitEach = commitEach;
    }

    public static DumpConfigBuilder builder() {
        return new DumpConfigBuilder();
    }

    public static class DumpConfigBuilder extends DumpConfig {
        private DumpConfigBuilder() {
        }

        public DumpConfigBuilder schemaConfig(SchemaConfig schemaConfig) {
            this.setSchemaConfig(schemaConfig);
            return this;
        }

        public DumpConfigBuilder outputStream(OutputStream outputStream) {
            this.setOutputStream(outputStream);
            return this;
        }

        public DumpConfigBuilder dumpDdl(boolean dumlDdl) {
            this.setDumpDdl(dumlDdl);
            return this;
        }

        public DumpConfigBuilder dumpDml(boolean dumlDml) {
            this.setDumpDml(dumlDml);
            return this;
        }

        public DumpConfigBuilder charset(Charset charset) {
            this.setCharset(charset);
            return this;
        }

        public DumpConfigBuilder verboseEash(long verboseEash) {
            this.setVerboseEach(verboseEash);
            return this;
        }

        public DumpConfigBuilder commitEach(long commitEach) {
            this.setCommitEach(commitEach);
            return this;
        }

        public DumpConfig build() {
            DumpConfig config = new DumpConfig();
            config.setSchemaConfig(this.getSchemaConfig());
            config.setOutputStream(this.getOutputStream());
            config.setDumpDdl(this.isDumpDdl());
            config.setDumpDml(this.isDumpDml());
            config.setCharset(this.getCharset());
            config.setVerboseEach(this.getVerboseEach());
            config.setCommitEach(this.getCommitEach());
            return config;
        }
    }
}
