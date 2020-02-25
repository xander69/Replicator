package ru.xander.replicator.dump;

import ru.xander.replicator.exception.ReplicatorException;

import javax.xml.bind.DatatypeConverter;
import java.sql.Blob;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public abstract class DumpUtils {

    private static final DateFormat dateFormat = new SimpleDateFormat("yyyy'-'MM'-'dd'T'HH':'mm':'ss'.'SSS");

    public static String dateToString(Date date) {
        if (date == null) {
            return null;
        }
        return dateFormat.format(date);
    }

    public static byte[] blobToBytes(Blob blob) {
        try {
            return blob.getBytes(0, (int) blob.length());
        } catch (SQLException e) {
            throw new ReplicatorException("Cannot convert BLOB-value to BASE64: " + e.getMessage(), e);
        }
    }

    public static String blobToBase64(Blob blob) {
        byte[] bytes = blobToBytes(blob);
        return DatatypeConverter.printBase64Binary(bytes);
    }
}
