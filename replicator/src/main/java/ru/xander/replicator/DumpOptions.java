package ru.xander.replicator;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class DumpOptions {

    private boolean dumpDdl = true;
    private boolean dumpDml = true;
    private Charset charset = StandardCharsets.UTF_8;
    private long verboseStep = 1000L;

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

    public long getVerboseStep() {
        return verboseStep;
    }

    public void setVerboseStep(long verboseStep) {
        this.verboseStep = verboseStep;
    }
}
