package ru.xander.replicator.util;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author Alexander Shakhov
 */
public interface RowMapper<T> {
    T map(ResultSet rs) throws SQLException;
}
