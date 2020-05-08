package ru.xander.replicator.compare;

/**
 * @author Alexander Shakhov
 */
public class CompareOptions {

    private boolean skipComments;
    private boolean skipDefaults;

    public boolean isSkipComments() {
        return skipComments;
    }

    public void setSkipComments(boolean skipComments) {
        this.skipComments = skipComments;
    }

    public boolean isSkipDefaults() {
        return skipDefaults;
    }

    public void setSkipDefaults(boolean skipDefaults) {
        this.skipDefaults = skipDefaults;
    }
}
