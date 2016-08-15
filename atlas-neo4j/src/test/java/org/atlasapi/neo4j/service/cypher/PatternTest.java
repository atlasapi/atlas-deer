package org.atlasapi.neo4j.service.cypher;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class PatternTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private Node node;
    private Edge edge;

    @Before
    public void setUp() throws Exception {
        node = Node.node()
                .name("node");
        edge = Edge.edge(EdgeDirection.RIGHT)
                .name("edge");
    }

    @Test
    public void patternWithTwoNodesTogetherFailsValidation() throws Exception {
        exception.expect(IllegalArgumentException.class);
        Pattern.pattern(node, edge, node, node)
                .build();
    }

    @Test
    public void patternWithTwoEdgesTogetherFailsValidation() throws Exception {
        exception.expect(IllegalArgumentException.class);
        Pattern.pattern(node, edge, edge, node)
                .build();
    }

    @Test
    public void emptyPatternFailsValidation() throws Exception {
        exception.expect(IllegalArgumentException.class);
        Pattern.pattern()
                .build();
    }

    @Test
    public void patternWithSingleNode() throws Exception {
        String pattern = Pattern.pattern(node)
                .build();

        assertThat(pattern, is("(node)"));
    }

    @Test
    public void patternWithNodeEdgeNode() throws Exception {
        String pattern = Pattern.pattern(node, edge, node)
                .build();

        assertThat(pattern, is("(node)-[edge]->(node)"));
    }
}
