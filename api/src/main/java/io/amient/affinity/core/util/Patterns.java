package io.amient.affinity.core.util;

import java.util.regex.Pattern;

public class Patterns {

    public static Pattern literal = Pattern.compile("[\\p{L},\\p{N},\\n,\\r,\\.,:,;,%,\\p{Sc},\\?,\\!,\\-,\\+,\\*,\\/,\\(,\\),\\[,\\],\\=,\\w]+");

    public static Pattern nominal = Pattern.compile("[\\p{L}]+");

    public static Pattern numeral = Pattern.compile("[\\p{N}]+");

    public static Pattern decimal = Pattern.compile("[0-9]+");

    public static Pattern alphanum = Pattern.compile("[\\p{L},\\p{N},\\-,_]+");

    public static boolean is(Pattern pattern, String input) {
        return pattern.matcher(input).matches();
    }

}
