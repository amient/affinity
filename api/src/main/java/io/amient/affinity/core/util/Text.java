package io.amient.affinity.core.util;

import java.util.regex.Pattern;

public class Text {

    final public static String fulltextSet = "\\p{L}\\p{M}\\p{N}\\p{Z}\\p{P}\\p{S}\\r\\n\\t\\s";
    final public static Pattern fulltext = Pattern.compile("^["+ fulltextSet +"]+$");
    final public static Pattern fulltextFilter = Pattern.compile("[^"+ fulltextSet +"]");

    final public static String literalSet = "\\p{L}\\p{M}\\p{N}\\p{P}\\p{S}";
    final public static Pattern literal = Pattern.compile("^["+ literalSet +"]+$");
    final public static Pattern literalFilter = Pattern.compile("[^"+ literalSet +"]");

    final public static String plaintextSet = "\\p{L}\\p{M}\\p{N}\\p{Z}\\p{P}\\p{Sc}";
    final public static Pattern plaintext = Pattern.compile("^["+ plaintextSet +"]+$");
    final public static Pattern plaintextFilter = Pattern.compile("[^"+ plaintextSet +"]");

    final public static String alphanumSet = "\\p{L}\\p{M}\\p{N}\\-_";
    final public static Pattern alphanum = Pattern.compile("^["+ alphanumSet +"]+$");
    final public static Pattern alphanumFilter = Pattern.compile("[^"+ alphanumSet +"]");

    final public static String nominalSet = "\\p{L}\\p{M}";
    final public static Pattern nominal = Pattern.compile("^["+ nominalSet +"]+$");
    final public static Pattern nominalFilter = Pattern.compile("[^"+ nominalSet +"]");

    final public static String numeralSet = "\\p{N}";
    final public static Pattern numeral = Pattern.compile("^["+ numeralSet +"]+$");
    final public static Pattern numeralFilter = Pattern.compile("[^"+ numeralSet +"]");

    final public static String decimalSet = "0-9";
    final public static Pattern decimal = Pattern.compile("^["+ decimalSet +"]+$");
    final public static Pattern decimalFilter = Pattern.compile("[^"+ decimalSet +"]");

    final public static String controlSet = "\\p{C}";
    final public static Pattern control = Pattern.compile("^["+ controlSet +"]+$");
    final public static Pattern controlFilter = Pattern.compile("[^"+ controlSet +"]");

    public static String apply(Pattern pattern, String input) {
        assert(!pattern.pattern().startsWith("^"));
        return pattern.matcher(input).replaceAll("");
    }

    public static boolean is(Pattern pattern, String input) {
        assert(pattern.pattern().startsWith("^"));
        return pattern.matcher(input).matches();
    }

    public static void require(String field, Pattern pattern, String input) {
        if (!is(pattern, input)) {
            String error;
            if (pattern == fulltext) {
                error ="Control characters are not allowed in " + field;
            } else if (pattern == literal) {
                error ="Whitespace and line feed characters are not allowed in " + field;
            } else if (pattern == plaintext) {
                error ="Controls, special characters, tabs and line feed characters are not allowed in " + field;
            } else if (pattern == alphanum) {
                error ="Only letters, accents and numbers are allowed in" + field;
            } else if (pattern == numeral) {
                error ="Only numerals are allowed in " + field;
            } else if (pattern == decimal) {
                error ="Only decimal number characters are allowed in " + field;
            } else {
                error = "Illegal characters detected in " + field;
            }
            throw new IllegalArgumentException(error);
        }
    }

}
