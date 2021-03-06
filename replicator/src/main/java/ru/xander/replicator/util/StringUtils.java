package ru.xander.replicator.util;

import java.util.Objects;

/**
 * @author Alexander Shakhov
 */
public abstract class StringUtils {

    private static final String COLUMNS_DELIMITER = ",";

    private StringUtils() {
        throw new IllegalStateException("Utility class");
    }

    public static boolean isEmpty(String str) {
        return (str == null) || str.isEmpty();
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    public static boolean equalsStringIgnoreWhiteSpace(String s1, String s2) {
        if (s1 != null) {
            s1 = s1.replace('\n', ' ').trim();
        }
        if (s2 != null) {
            s2 = s2.replace('\n', ' ').trim();
        }
        return Objects.equals(s1, s2);
    }

    public static String[] splitColumns(String columns) {
        String[] array = columns.split(COLUMNS_DELIMITER);
        for (int i = 0; i < array.length; i++) {
            array[i] = array[i].trim();
        }
        return array;
    }

    public static String joinColumns(String[] columns) {
        return String.join(COLUMNS_DELIMITER, columns);
    }

    public static boolean arrayContains(String[] array, String str) {
        for (String s : array) {
            if (s.equals(str)) {
                return true;
            }
        }
        return false;
    }

    public static String repeat(char c, int count) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < count; i++) {
            sb.append(c);
        }
        return sb.toString();
    }

    public static String[] cutString(String s, final int partSize) {
        if (partSize <= 0) {
            throw new IllegalArgumentException("Part size must be greatest than 0");
        }
        if (s.length() <= partSize) {
            return new String[]{s};
        }
        int partsCount = (int) Math.ceil(s.length() / (double) partSize);
        String[] parts = new String[partsCount];
        for (int i = 0; i < partsCount; i++) {
            if ((i + 1) == partsCount) {
                parts[i] = s.substring(i * partSize);
            } else {
                parts[i] = s.substring(i * partSize, (i + 1) * partSize);
            }
        }
        return parts;
    }
}
