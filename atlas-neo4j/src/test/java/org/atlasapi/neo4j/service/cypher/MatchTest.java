package org.atlasapi.neo4j.service.cypher;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class MatchTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private Pattern pattern;

    @Before
    public void setUp() throws Exception {
        pattern = Pattern.pattern(
                Node.node().name("a"),
                Edge.edge(EdgeDirection.RIGHT),
                Node.node().name("b")
        );
    }

    @Test
    public void matchWithNoPatternsFailsValidation() throws Exception {
        exception.expect(IllegalArgumentException.class);
        Match.match()
                .build();
    }

    @Test
    public void matchWithSinglePattern() throws Exception {
        String match = Match.match(pattern)
                .build();

        assertThat(match, is("MATCH (a)-->(b)"));
    }

    @Test
    public void matchWithMultiplePatterns() throws Exception {
        String match = Match.match(pattern, pattern)
                .build();

        assertThat(match, is("MATCH (a)-->(b), (a)-->(b)"));
    }

    @Test
    public void optionalMatch() throws Exception {
        String match = Match.matchOptional(pattern)
                .build();

        assertThat(match, is("OPTIONAL MATCH (a)-->(b)"));
    }
}
