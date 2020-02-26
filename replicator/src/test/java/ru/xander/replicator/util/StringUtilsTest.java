package ru.xander.replicator.util;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author Alexander Shakhov
 */
public class StringUtilsTest {
    @Test
    public void cutString() {
        Assert.assertArrayEquals(new String[]{""}, StringUtils.cutString("", 1));
        Assert.assertArrayEquals(new String[]{"a"}, StringUtils.cutString("a", 1));
        Assert.assertArrayEquals(new String[]{"a", "b", "c"}, StringUtils.cutString("abc", 1));
        Assert.assertArrayEquals(new String[]{"abc", "def", "g"}, StringUtils.cutString("abcdefg", 3));
    }
}