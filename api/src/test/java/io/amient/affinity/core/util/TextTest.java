package io.amient.affinity.core.util;

import org.junit.Test;

public class TextTest {

    @Test
    public void textFilter() {
        String input = "(Hello1١),\t[World'2٢], {\"ľšť\"},!£@$%5\n<some hack/>" + (char)0;
        assert(Text.apply(Text.fulltextFilter, input).equals("(Hello1١),\t[World'2٢], {\"ľšť\"},!£@$%5\n<some hack/>"));
        assert(Text.apply(Text.literalFilter, input).equals("(Hello1١),[World'2٢],{\"ľšť\"},!£@$%5<somehack/>"));
        assert(Text.apply(Text.plaintextFilter, input).equals("(Hello1١),[World'2٢], {\"ľšť\"},!£@$%5some hack/"));
        assert(Text.apply(Text.alphanumFilter, input).equals("Hello1١World2٢ľšť5somehack"));
        assert(Text.apply(Text.nominalFilter, input).equals("HelloWorldľšťsomehack"));
        assert(Text.apply(Text.numeralFilter, input).equals("1١2٢5"));
        assert(Text.apply(Text.decimalFilter, input).equals("125"));
        assert(Text.apply(Text.controlFilter, input).equals("\t\n" + (char)0));
    }

    @Test(expected = RuntimeException.class)
    public void applyingMatchPatternShouldThrowException() {
        Text.apply(Text.plaintext, "abcd 123");
    }

    @Test(expected = RuntimeException.class)
    public void requiringFilterPatternShouldThrowException() {
        Text.require("x", Text.plaintextFilter, "abcd 123");
    }
    @Test
    public void fulltextPatternMatchesUnicodeText() {
        assert(Text.is(Text.fulltext, " <>~.,|\\/-+/*#()[]{}_\"\r\n\t:;!?^&@%¢$£1234567890١٢٣٤٥٦٧٨٩a\nAľľščťžýáíé\rτσιαιγολοχβςαν\n\rنيرحبال\r\nтераб"));
        assert(!Text.is(Text.fulltext,"" + (char)0));
        assert(!Text.is(Text.fulltext,"" + (char)1));
        assert(!Text.is(Text.fulltext,"" + (char)2));
        assert(!Text.is(Text.fulltext,"" + (char)3));
    }

    @Test
    public void literalPatternMatchesUnicodeTextWithoutWhiteSpace() {
        String input = "()[]{}<>\"'.,-+*\\/#_:;!?&@%¢$£1234567890١٢٣٤٥٦٧٨٩aAľľščťžýáíéτσιαιγολοχβςανنيرحبالтераб";
        assert( Text.is(Text.literal, input));
        assert( Text.apply(Text.literalFilter, input).equals(input));
        assert(!Text.is(Text.literal,"\r"));
        assert(!Text.is(Text.literal,"\n"));
        assert(!Text.is(Text.literal,"\t"));
        assert(!Text.is(Text.literal," "));
    }

    @Test
    public void nominalPatternMatchesUnicodeLettersAndNumbersOnly() {
        assert( Text.is(Text.nominal,"Aľľščťžýáíéτσιαιγολοχβςανтераб"));
        assert(!Text.is(Text.nominal,"1"));
        assert(!Text.is(Text.nominal,"2"));
        assert(!Text.is(Text.nominal,"3"));
        assert(!Text.is(Text.nominal,"4"));
        assert(!Text.is(Text.nominal,"5"));
        assert(!Text.is(Text.nominal,"6"));
        assert(!Text.is(Text.nominal,"7"));
        assert(!Text.is(Text.nominal,"8"));
        assert(!Text.is(Text.nominal,"9"));
        assert(!Text.is(Text.nominal,"0"));
        assert(!Text.is(Text.nominal,"١"));
        assert(!Text.is(Text.nominal,"٢"));
        assert(!Text.is(Text.nominal,"٣"));
        assert(!Text.is(Text.nominal,"٤"));
        assert(!Text.is(Text.nominal,"٥"));
        assert(!Text.is(Text.nominal,"٦"));
        assert(!Text.is(Text.nominal,"٧"));
        assert(!Text.is(Text.nominal,"٨"));
        assert(!Text.is(Text.nominal,"٩"));
        assert(!Text.is(Text.nominal,"-"));
        assert(!Text.is(Text.nominal,"_"));
        assert(!Text.is(Text.nominal,"$"));
        assert(!Text.is(Text.nominal,"£"));
        assert(!Text.is(Text.nominal,"."));
        assert(!Text.is(Text.nominal,","));
        assert(!Text.is(Text.nominal,"+"));
        assert(!Text.is(Text.nominal,"/"));
        assert(!Text.is(Text.nominal,"*"));
        assert(!Text.is(Text.nominal,"("));
        assert(!Text.is(Text.nominal,"["));
        assert(!Text.is(Text.nominal,":"));
        assert(!Text.is(Text.nominal,";"));
        assert(!Text.is(Text.nominal,"!"));
        assert(!Text.is(Text.nominal,"?"));
        assert(!Text.is(Text.nominal,"#"));
        assert(!Text.is(Text.nominal,"<"));
        assert(!Text.is(Text.nominal,">"));
        assert(!Text.is(Text.nominal,"~"));
        assert(!Text.is(Text.nominal,"\""));
        assert(!Text.is(Text.nominal,"|"));
        assert(!Text.is(Text.nominal,"\\"));
        assert(!Text.is(Text.nominal,"{"));
        assert(!Text.is(Text.nominal,"}"));
    }

    @Test
    public void numeralsMatchesUnicodeNumbersOnly() {
        assert(Text.is(Text.numeral,"1234567890١٢٣٤٥٦٧٨٩"));
        assert(!Text.is(Text.numeral, "a"));
        assert(!Text.is(Text.numeral,"$"));
        assert(!Text.is(Text.numeral,"£"));
        assert(!Text.is(Text.numeral,"."));
        assert(!Text.is(Text.numeral,","));
        assert(!Text.is(Text.numeral,"+"));
        assert(!Text.is(Text.numeral,"-"));
        assert(!Text.is(Text.numeral,"/"));
        assert(!Text.is(Text.numeral,"*"));
        assert(!Text.is(Text.numeral,"("));
        assert(!Text.is(Text.numeral,"["));
        assert(!Text.is(Text.numeral,":"));
        assert(!Text.is(Text.numeral,";"));
        assert(!Text.is(Text.numeral,"!"));
        assert(!Text.is(Text.numeral,"?"));
        assert(!Text.is(Text.numeral,"#"));
        assert(!Text.is(Text.numeral,"<"));
        assert(!Text.is(Text.numeral,">"));
        assert(!Text.is(Text.numeral,"~"));
    }

    @Test
    public void alphaNumPatternMatchesUnicodeLettersAndNumbersOnly() {
        assert(Text.is(Text.alphanum,"_-1234567890aAľľščťžýáíéτσιαιγολοχβςανтераб"));
        assert(!Text.is(Text.alphanum,"$"));
        assert(!Text.is(Text.alphanum,"£"));
        assert(!Text.is(Text.alphanum,"."));
        assert(!Text.is(Text.alphanum,","));
        assert(!Text.is(Text.alphanum,"+"));
        assert(!Text.is(Text.alphanum,"/"));
        assert(!Text.is(Text.alphanum,"*"));
        assert(!Text.is(Text.alphanum,"("));
        assert(!Text.is(Text.alphanum,"["));
        assert(!Text.is(Text.alphanum,":"));
        assert(!Text.is(Text.alphanum,";"));
        assert(!Text.is(Text.alphanum,"!"));
        assert(!Text.is(Text.alphanum,"?"));
        assert(!Text.is(Text.alphanum,"#"));
        assert(!Text.is(Text.alphanum,"<"));
        assert(!Text.is(Text.alphanum,">"));
        assert(!Text.is(Text.alphanum,"~"));
        assert(!Text.is(Text.alphanum,"\""));
        assert(!Text.is(Text.alphanum,"|"));
        assert(!Text.is(Text.alphanum,"\\"));
        assert(!Text.is(Text.alphanum,"{"));
        assert(!Text.is(Text.alphanum,"}"));
    }

}
