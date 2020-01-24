package ru.xander.replicator;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class DumpOptions {

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
}
