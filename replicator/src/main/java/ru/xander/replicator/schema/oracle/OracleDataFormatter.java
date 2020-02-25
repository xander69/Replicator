package ru.xander.replicator.schema.oracle;

import ru.xander.replicator.exception.SchemaException;
import ru.xander.replicator.schema.Column;
import ru.xander.replicator.schema.DataFormatter;
import ru.xander.replicator.util.StringUtils;

import java.io.IOException;
import java.io.Reader;
import java.sql.Clob;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.SimpleDateFormat;

/**
 * @author Alexander Shakhov
 */
class OracleDataFormatter implements DataFormatter {

    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static final SimpleDateFormat timestampFormat = new SimpleDateFormat("yyyy-MM.dd HH:mm:ss.SSS");

    @Override
    public String formatBoolean(Object value, Column column) {
        return (Boolean) value ? "1" : "0";
    }

    @Override
    public String formatInteger(Object value, Column column) {
        return String.valueOf(value);
    }

    @Override
    public String formatFloat(Object value, Column column) {
        DecimalFormatSymbols formatSymbols = DecimalFormatSymbols.getInstance();
        formatSymbols.setDecimalSeparator('.');
        DecimalFormat decimalFormat = new DecimalFormat("##0.0" + StringUtils.repeat('#', column.getScale() - 1));
        decimalFormat.setDecimalFormatSymbols(formatSymbols);
        return decimalFormat.format(value);
    }

    @Override
    public String formatSerial(Object value, Column column) {
        return String.valueOf(value);
    }

    @Override
    public String formatChar(Object value, Column column) {
        return quoteString(String.valueOf(value));
    }

    @Override
    public String formatString(Object value, Column column) {
        return quoteString(String.valueOf(value));
    }

    @Override
    public String formatDate(Object value, Column column) {
        return "TO_DATE('" + dateFormat.format(value) + "', 'YYYY-MM-DD HH24:MI:SS')";
    }

    @Override
    public String formatTime(Object value, Column column) {
        return "TO_DATE('" + dateFormat.format(value) + "', 'YYYY-MM-DD HH24:MI:SS')";
    }

    @Override
    public String formatTimestamp(Object value, Column column) {
        return "TO_TIMESTAMP('" + timestampFormat.format(value) + "', 'YYYY-MM-DD HH24:MI:SS.FF3')";
    }

    @Override
    public String formatClob(Object value, Column column) {
        String clob = readClob((Clob) value);
        if (clob.length() <= 2000) {
            return quoteString(clob);
        }
        String[] parts = StringUtils.cutString(clob, 2000);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < parts.length; i++) {
            sb.append("TO_CLOB(").append(quoteString(parts[i])).append(")");
            if (i < (parts.length - 1)) {
                sb.append(" || ");
            }
        }
        return sb.toString();
    }

    @Override
    public String formatBlob(Object value, Column column) {
        //TODO: blob не поддерживается
        return "NULL";
    }

    private static String quoteString(String string) {
        return '\'' + string
                .replace("'", "''")
                .replace("\n", "'||CHR(10)||'")
                .replace("\r", "'||CHR(13)||'")
                + '\'';
    }

    private static String readClob(Clob clob) {
        StringBuilder value = new StringBuilder();
        try (Reader reader = clob.getCharacterStream()) {
            char[] buffer = new char[4096];
            int len;
            while ((len = reader.read(buffer)) != -1) {
                value.append(buffer, 0, len);
            }
        } catch (SQLException | IOException e) {
            String errorMessage = "Cannot read CLOB value: " + e.getMessage();
            throw new SchemaException(errorMessage, e);
        }
        return value.toString();
    }
}
