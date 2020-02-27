package ru.xander.replicator.schema;

/**
 * @author Alexander Shakhov
 */
public class SchemaConnectionTest {

    private final boolean valid;
    private final String errorMessage;

    public SchemaConnectionTest(boolean valid, String errorMessage) {
        this.valid = valid;
        this.errorMessage = errorMessage;
    }

    public boolean isValid() {
        return valid;
    }

    public String getErrorMessage() {
        return errorMessage;
    }
}
