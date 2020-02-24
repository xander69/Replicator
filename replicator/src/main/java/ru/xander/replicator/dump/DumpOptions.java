package ru.xander.replicator.dump;

import java.io.OutputStream;
import java.nio.charset.Charset;

/**
 * @author Alexander Shakhov
 */
public class DumpOptions {
    private OutputStream outputStream;
    private boolean dumpDdl;
    private boolean dumpDml;
    private Charset charset;
    private long verboseEach;
    private long commitEach;

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
}
