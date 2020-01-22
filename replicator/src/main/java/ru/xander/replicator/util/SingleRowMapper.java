package ru.xander.replicator.util;

import java.sql.ResultSet;
import java.sql.SQLException;

public interface SingleRowMapper<T> {
    T map(ResultSet rs) throws SQLException;
}
