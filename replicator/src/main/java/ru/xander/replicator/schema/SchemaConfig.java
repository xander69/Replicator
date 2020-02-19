package ru.xander.replicator.schema;

import ru.xander.replicator.listener.Listener;

/**
 * @author Alexander Shakhov
 */
public class SchemaConfig {

    private String jdbcDriver;
    private String jdbcUrl;
    private String username;
    private String password;
    private String workSchema;
    private Listener listener;

    public String getJdbcDriver() {
        return jdbcDriver;
    }

    public void setJdbcDriver(String jdbcDriver) {
        this.jdbcDriver = jdbcDriver;
    }

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public void setJdbcUrl(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getWorkSchema() {
        return workSchema;
    }

    public void setWorkSchema(String workSchema) {
        this.workSchema = workSchema;
    }

    public Listener getListener() {
        return listener;
    }

    public void setListener(Listener listener) {
        this.listener = listener;
    }

    public static SchemaConfigBuilder builder() {
        return new SchemaConfigBuilder();
    }

    public static class SchemaConfigBuilder extends SchemaConfig {

        private SchemaConfigBuilder() {
        }

        public SchemaConfigBuilder jdbcDriver(String jdbcDriver) {
            this.setJdbcDriver(jdbcDriver);
            return this;
        }

        public SchemaConfigBuilder jdbcUrl(String jdbcUrl) {
            this.setJdbcUrl(jdbcUrl);
            return this;
        }

        public SchemaConfigBuilder username(String username) {
            this.setUsername(username);
            return this;
        }

        public SchemaConfigBuilder password(String password) {
            this.setPassword(password);
            return this;
        }

        public SchemaConfigBuilder workSchema(String workSchema) {
            this.setWorkSchema(workSchema);
            return this;
        }

        public SchemaConfigBuilder listener(Listener listener) {
            this.setListener(listener);
            return this;
        }

        public SchemaConfig build() {
            SchemaConfig schemaConfig = new SchemaConfig();
            schemaConfig.setJdbcDriver(this.getJdbcDriver());
            schemaConfig.setJdbcUrl(this.getJdbcUrl());
            schemaConfig.setUsername(this.getUsername());
            schemaConfig.setPassword(this.getPassword());
            schemaConfig.setWorkSchema(this.getWorkSchema());
            schemaConfig.setListener(this.getListener());
            return schemaConfig;
        }
    }
}
