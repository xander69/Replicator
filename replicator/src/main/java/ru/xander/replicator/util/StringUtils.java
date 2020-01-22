package ru.xander.replicator.util;

import java.util.Objects;

public class StringUtils {
    public static boolean isEmpty(String str) {
        return (str == null) || str.isEmpty();
    }

    public static boolean equalsStringIgnoreWhiteSpace(String s1, String s2) {
        if (s1 != null) {
            s1 = s1.replace('\n', ' ').trim();
        }
        if (s2 != null) {
            s2 = s2.replace('\n', ' ').trim();
        }
        return Objects.equals(s1, s2);
    }
}
