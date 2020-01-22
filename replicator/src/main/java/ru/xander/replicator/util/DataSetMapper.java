package ru.xander.replicator.util;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;

public interface DataSetMapper {
    void map(ResultSet rs) throws SQLException, IOException;
}
