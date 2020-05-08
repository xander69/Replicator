package ru.xander.replicator.action;

import ru.xander.replicator.compare.CompareOptions;
import ru.xander.replicator.schema.SchemaConfig;

/**
 * @author Alexander Shakhov
 */
public class CompareActionConfigurer implements ActionConfigurer<CompareAction> {

    private static final boolean DEFAULT_SKIP_COMMENTS = false;
    private static final boolean DEFAULT_SKIP_DEFAULTS = false;
    /**
     * Конфигурация схемы-источника.
     */
    private SchemaConfig sourceConfig;

    /**
     * Конфигурация схемы-приемника.
     */
    private SchemaConfig targetConfig;

    /**
     * Список таблиц для сравнения.
     */
    private String[] tables;

    /**
     * Если true, комментарии для колонок не будут сравниваться.
     */
    private boolean skipComments = DEFAULT_SKIP_COMMENTS;

    /**
     * Если true, значения по умолчанию для колонок не будут сравниваться.
     */
    private boolean skipDefaults = DEFAULT_SKIP_DEFAULTS;

    public CompareActionConfigurer sourceConfig(SchemaConfig sourceConfig) {
        this.sourceConfig = sourceConfig;
        return this;
    }

    public CompareActionConfigurer targetConfig(SchemaConfig targetConfig) {
        this.targetConfig = targetConfig;
        return this;
    }

    public CompareActionConfigurer tables(String... tables) {
        this.tables = tables;
        return this;
    }

    public CompareActionConfigurer skipComments(boolean skipComments) {
        this.skipComments = skipComments;
        return this;
    }

    public CompareActionConfigurer skipDefaults(boolean skipDefaults) {
        this.skipDefaults = skipDefaults;
        return this;
    }

    @Override
    public CompareAction configure() {
        CompareOptions options = new CompareOptions();
        options.setSkipComments(skipComments);
        options.setSkipDefaults(skipDefaults);
        return new CompareAction(sourceConfig, targetConfig, tables, options);
    }
}
