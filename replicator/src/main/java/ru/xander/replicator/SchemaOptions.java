package ru.xander.replicator;

public class SchemaOptions {

    private String jdbcDriver;
    private String jdbcUrl;
    private String username;
    private String password;
    private String workSchema;

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
}
