package ru.xander.replicator.action;

import ru.xander.replicator.dump.DumpOptions;
import ru.xander.replicator.schema.SchemaConfig;

import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * @author Alexander Shakhov
 */
public class DumpActionConfigurer implements ActionConfigurer<DumpAction> {

    private static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;
    private static final long DEFAULT_VERBOSE_EACH = 1000L;
    private static final long DEFAULT_COMMIT_EACH = 1000L;

    /**
     * Конфигурация схемы.
     */
    private SchemaConfig schemaConfig;

    /**
     * Выходной поток для результата.
     */
    private OutputStream outputStream;

    /**
     * Сохранять DDL в дамп.
     */
    private boolean dumpDdl = true;

    /**
     * Сохранять данные в дамп.
     */
    private boolean dumpDml = true;

    /**
     * Кодировка выходного файла.
     */
    private Charset charset = DEFAULT_CHARSET;

    /**
     * Количество записей, после которого будет генерироваться событие прогресса.
     */
    private long verboseEach = DEFAULT_VERBOSE_EACH;

    /**
     * Количество записей, после которого будет выполняться коммит.
     * Если 0, то коммит будет только в конце.
     */
    private long commitEach = DEFAULT_COMMIT_EACH;

    /**
     * Имя таблицы, для которой следует снять дамп.
     */
    private String tableName;

    public DumpActionConfigurer schemaConfig(SchemaConfig schemaConfig) {
        this.schemaConfig = schemaConfig;
        return this;
    }

    public DumpActionConfigurer outputStream(OutputStream outputStream) {
        this.outputStream = outputStream;
        return this;
    }

    public DumpActionConfigurer dumpDdl(boolean dumlDdl) {
        this.dumpDdl = dumlDdl;
        return this;
    }

    public DumpActionConfigurer dumpDml(boolean dumlDml) {
        this.dumpDml = dumlDml;
        return this;
    }

    public DumpActionConfigurer charset(Charset charset) {
        this.charset = charset;
        return this;
    }

    public DumpActionConfigurer verboseEash(long verboseEash) {
        this.verboseEach = verboseEash;
        return this;
    }

    public DumpActionConfigurer commitEach(long commitEach) {
        this.commitEach = commitEach;
        return this;
    }

    public DumpActionConfigurer tableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    @Override
    public DumpAction configure() {
        DumpOptions options = new DumpOptions();
        options.setDumpDdl(dumpDdl);
        options.setDumpDml(dumpDml);
        options.setCharset(charset);
        options.setVerboseEach(verboseEach);
        options.setCommitEach(commitEach);
        return new DumpAction(schemaConfig, outputStream, options, tableName);
    }
}
