package org.atlasapi.generation.parsing;
import static org.junit.Assert.assertEquals;

import org.atlasapi.generation.parsing.JavadocParser;
import org.atlasapi.generation.parsing.StandardJavadocParser;
import org.junit.Test;



public class StandardJavadocParserTest {
    
    private final JavadocParser parser = new StandardJavadocParser();

    @Test
    public void testStripsNewlinesFromDocStrings() {
        String stringWithNewlines = "abc\n123\n";
        
        String parsed = parser.parse(stringWithNewlines);
        
        assertEquals("abc123", parsed);
    }

    @Test
    public void testLeavesNonNewlinesUntouched() {
        String stringWithNewlines = "abc123";
        
        String parsed = parser.parse(stringWithNewlines);
        
        assertEquals(stringWithNewlines, parsed);
    }
}
