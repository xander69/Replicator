package ru.xander.replicator.action;

import ru.xander.replicator.filter.Filter;
import ru.xander.replicator.filter.FilterType;
import ru.xander.replicator.schema.SchemaConfig;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Alexander Shakhov
 */
public class TableListActionConfigurer implements ActionConfigurer<TableListAction> {

    private SchemaConfig schemaConfig;
    private List<Filter> filterList;

    public TableListActionConfigurer schemaConfig(SchemaConfig schemaConfig) {
        this.schemaConfig = schemaConfig;
        return this;
    }

    public TableListActionConfigurer filterList(List<Filter> filterList) {
        this.filterList = filterList;
        return this;
    }

    public TableListActionConfigurer like(String value) {
        getFilterList().add(new Filter(FilterType.LIKE, value));
        return this;
    }

    public TableListActionConfigurer notLike(String value) {
        getFilterList().add(new Filter(FilterType.NOT_LIKE, value));
        return this;
    }

    private List<Filter> getFilterList() {
        if (filterList == null) {
            filterList = new ArrayList<>();
        }
        return filterList;
    }

    @Override
    public TableListAction configure() {
        return new TableListAction(schemaConfig, filterList);
    }
}
