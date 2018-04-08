package io.amient.affinity.core.util;

import org.junit.Test;

public class PatternsTest {
    @Test
    public void textualPatternMatchesUnicodeText() {
        assert(Patterns.is(Patterns.textual, " .,-+/*()[]_:;!?^&@%¢$£1234567890١٢٣٤٥٦٧٨٩a\nAľľščťžýáíé\rτσιαιγολοχβςαν\n\rنيرحبال\r\nтераб"));
        assert(!Patterns.is(Patterns.textual,"#"));
        assert(!Patterns.is(Patterns.textual,"<"));
        assert(!Patterns.is(Patterns.textual,">"));
        assert(!Patterns.is(Patterns.textual,"~"));
        assert(!Patterns.is(Patterns.textual,"\""));
        assert(!Patterns.is(Patterns.textual,"|"));
        assert(!Patterns.is(Patterns.textual,"\\"));
        assert(!Patterns.is(Patterns.textual,"{"));
        assert(!Patterns.is(Patterns.textual,"}"));
    }

    @Test
    public void literalPatternMatchesUnicodeTextWithoutWhiteSpace() {
        assert( Patterns.is(Patterns.literal, ".,-+/*()[]_:;!?^&@%=¢$£1234567890١٢٣٤٥٦٧٨٩aAľľščťžýáíéτσιαιγολοχβςανنيرحبالтераб"));
        assert(!Patterns.is(Patterns.literal,"\r"));
        assert(!Patterns.is(Patterns.literal,"\n"));
        assert(!Patterns.is(Patterns.literal," "));
        assert(!Patterns.is(Patterns.literal,"#"));
        assert(!Patterns.is(Patterns.literal,"<"));
        assert(!Patterns.is(Patterns.literal,">"));
        assert(!Patterns.is(Patterns.literal,"~"));
        assert(!Patterns.is(Patterns.literal,"\""));
        assert(!Patterns.is(Patterns.literal,"|"));
        assert(!Patterns.is(Patterns.literal,"\\"));
        assert(!Patterns.is(Patterns.literal,"{"));
        assert(!Patterns.is(Patterns.literal,"}"));
    }

    @Test
    public void nominalPatternMatchesUnicodeLettersAndNumbersOnly() {
        assert( Patterns.is(Patterns.nominal,"Aľľščťžýáíéτσιαιγολοχβςανтераб"));
        assert(!Patterns.is(Patterns.nominal,"1"));
        assert(!Patterns.is(Patterns.nominal,"2"));
        assert(!Patterns.is(Patterns.nominal,"3"));
        assert(!Patterns.is(Patterns.nominal,"4"));
        assert(!Patterns.is(Patterns.nominal,"5"));
        assert(!Patterns.is(Patterns.nominal,"6"));
        assert(!Patterns.is(Patterns.nominal,"7"));
        assert(!Patterns.is(Patterns.nominal,"8"));
        assert(!Patterns.is(Patterns.nominal,"9"));
        assert(!Patterns.is(Patterns.nominal,"0"));
        assert(!Patterns.is(Patterns.nominal,"١"));
        assert(!Patterns.is(Patterns.nominal,"٢"));
        assert(!Patterns.is(Patterns.nominal,"٣"));
        assert(!Patterns.is(Patterns.nominal,"٤"));
        assert(!Patterns.is(Patterns.nominal,"٥"));
        assert(!Patterns.is(Patterns.nominal,"٦"));
        assert(!Patterns.is(Patterns.nominal,"٧"));
        assert(!Patterns.is(Patterns.nominal,"٨"));
        assert(!Patterns.is(Patterns.nominal,"٩"));
        assert(!Patterns.is(Patterns.nominal,"-"));
        assert(!Patterns.is(Patterns.nominal,"_"));
        assert(!Patterns.is(Patterns.nominal,"$"));
        assert(!Patterns.is(Patterns.nominal,"£"));
        assert(!Patterns.is(Patterns.nominal,"."));
        assert(!Patterns.is(Patterns.nominal,","));
        assert(!Patterns.is(Patterns.nominal,"+"));
        assert(!Patterns.is(Patterns.nominal,"/"));
        assert(!Patterns.is(Patterns.nominal,"*"));
        assert(!Patterns.is(Patterns.nominal,"("));
        assert(!Patterns.is(Patterns.nominal,"["));
        assert(!Patterns.is(Patterns.nominal,":"));
        assert(!Patterns.is(Patterns.nominal,";"));
        assert(!Patterns.is(Patterns.nominal,"!"));
        assert(!Patterns.is(Patterns.nominal,"?"));
        assert(!Patterns.is(Patterns.nominal,"#"));
        assert(!Patterns.is(Patterns.nominal,"<"));
        assert(!Patterns.is(Patterns.nominal,">"));
        assert(!Patterns.is(Patterns.nominal,"~"));
        assert(!Patterns.is(Patterns.nominal,"\""));
        assert(!Patterns.is(Patterns.nominal,"|"));
        assert(!Patterns.is(Patterns.nominal,"\\"));
        assert(!Patterns.is(Patterns.nominal,"{"));
        assert(!Patterns.is(Patterns.nominal,"}"));
    }

    @Test
    public void numeralsMatchesUnicodeNumbersOnly() {
        assert(Patterns.is(Patterns.numeral,"1234567890١٢٣٤٥٦٧٨٩"));
        assert(!Patterns.is(Patterns.numeral, "a"));
        assert(!Patterns.is(Patterns.numeral,"$"));
        assert(!Patterns.is(Patterns.numeral,"£"));
        assert(!Patterns.is(Patterns.numeral,"."));
        assert(!Patterns.is(Patterns.numeral,","));
        assert(!Patterns.is(Patterns.numeral,"+"));
        assert(!Patterns.is(Patterns.numeral,"-"));
        assert(!Patterns.is(Patterns.numeral,"/"));
        assert(!Patterns.is(Patterns.numeral,"*"));
        assert(!Patterns.is(Patterns.numeral,"("));
        assert(!Patterns.is(Patterns.numeral,"["));
        assert(!Patterns.is(Patterns.numeral,":"));
        assert(!Patterns.is(Patterns.numeral,";"));
        assert(!Patterns.is(Patterns.numeral,"!"));
        assert(!Patterns.is(Patterns.numeral,"?"));
        assert(!Patterns.is(Patterns.numeral,"#"));
        assert(!Patterns.is(Patterns.numeral,"<"));
        assert(!Patterns.is(Patterns.numeral,">"));
        assert(!Patterns.is(Patterns.numeral,"~"));
    }

    @Test
    public void alphaNumPatternMatchesUnicodeLettersAndNumbersOnly() {
        assert(Patterns.is(Patterns.alphanum,"_-1234567890aAľľščťžýáíéτσιαιγολοχβςανтераб"));
        assert(!Patterns.is(Patterns.alphanum,"$"));
        assert(!Patterns.is(Patterns.alphanum,"£"));
        assert(!Patterns.is(Patterns.alphanum,"."));
        assert(!Patterns.is(Patterns.alphanum,","));
        assert(!Patterns.is(Patterns.alphanum,"+"));
        assert(!Patterns.is(Patterns.alphanum,"/"));
        assert(!Patterns.is(Patterns.alphanum,"*"));
        assert(!Patterns.is(Patterns.alphanum,"("));
        assert(!Patterns.is(Patterns.alphanum,"["));
        assert(!Patterns.is(Patterns.alphanum,":"));
        assert(!Patterns.is(Patterns.alphanum,";"));
        assert(!Patterns.is(Patterns.alphanum,"!"));
        assert(!Patterns.is(Patterns.alphanum,"?"));
        assert(!Patterns.is(Patterns.alphanum,"#"));
        assert(!Patterns.is(Patterns.alphanum,"<"));
        assert(!Patterns.is(Patterns.alphanum,">"));
        assert(!Patterns.is(Patterns.alphanum,"~"));
        assert(!Patterns.is(Patterns.alphanum,"\""));
        assert(!Patterns.is(Patterns.alphanum,"|"));
        assert(!Patterns.is(Patterns.alphanum,"\\"));
        assert(!Patterns.is(Patterns.alphanum,"{"));
        assert(!Patterns.is(Patterns.alphanum,"}"));
    }

}
