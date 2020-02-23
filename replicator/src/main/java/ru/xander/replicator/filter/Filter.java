package ru.xander.replicator.filter;

/**
 * @author Alexander Shakhov
 */
public class Filter {

    private FilterType type;
    private String value;

    public Filter() {
    }

    public Filter(FilterType type, String value) {
        this.type = type;
        this.value = value;
    }

    public FilterType getType() {
        return type;
    }

    public void setType(FilterType type) {
        this.type = type;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
