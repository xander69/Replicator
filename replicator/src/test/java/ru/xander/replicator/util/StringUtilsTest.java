package ru.xander.replicator.util;

import org.junit.Assert;
import org.junit.Test;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;

public class StringUtilsTest {
    @Test
    public void cutString() {
        Assert.assertArrayEquals(new String[]{""}, StringUtils.cutString("", 1));
        Assert.assertArrayEquals(new String[]{"a"}, StringUtils.cutString("a", 1));
        Assert.assertArrayEquals(new String[]{"a", "b", "c"}, StringUtils.cutString("abc", 1));
        Assert.assertArrayEquals(new String[]{"abc", "def", "g"}, StringUtils.cutString("abcdefg", 3));
    }

    public static void main(String[] args) {
        DecimalFormatSymbols formatSymbols = DecimalFormatSymbols.getInstance();
        formatSymbols.setDecimalSeparator('.');
        DecimalFormat decimalFormat = new DecimalFormat("##0.0#########");
        decimalFormat.setDecimalFormatSymbols(formatSymbols);
        System.out.println(decimalFormat.format(1234567.123412));
    }
}