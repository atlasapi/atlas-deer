package org.atlasapi.neo4j.service.cypher;

import org.junit.Test;

import static org.atlasapi.neo4j.service.cypher.EdgeDirection.BIDIRECTIONAL;
import static org.atlasapi.neo4j.service.cypher.EdgeDirection.LEFT;
import static org.atlasapi.neo4j.service.cypher.EdgeDirection.RIGHT;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class EdgeTest {

    @Test
    public void createEdge() throws Exception {
        String edge = Edge.edge(BIDIRECTIONAL)
                .name("name")
                .label("Label")
                .property("booleanProperty", false)
                .property("numericProperty", 5.0F)
                .property("stringProperty", "value")
                .build();

        assertThat(edge,
                is("-[name:Label {"
                        + "booleanProperty: false, "
                        + "numericProperty: 5.0, "
                        + "stringProperty: \"value\""
                        + "}]-"));
    }

    @Test
    public void createEdgeWithoutProperties() throws Exception {
        String edge = Edge.edge(BIDIRECTIONAL)
                .name("name")
                .label("Label")
                .build();

        assertThat(edge, is("-[name:Label]-"));
    }

    @Test
    public void createEdgeWithoutLabel() throws Exception {
        String edge = Edge.edge(BIDIRECTIONAL)
                .name("name")
                .property("booleanProperty", false)
                .property("numericProperty", 5.0F)
                .property("stringProperty", "value")
                .build();

        assertThat(edge,
                is("-[name {"
                        + "booleanProperty: false, "
                        + "numericProperty: 5.0, "
                        + "stringProperty: \"value\""
                        + "}]-"));
    }

    @Test
    public void createEdgeWithoutName() throws Exception {
        String edge = Edge.edge(BIDIRECTIONAL)
                .label("Label")
                .property("booleanProperty", false)
                .property("numericProperty", 5.0F)
                .property("stringProperty", "value")
                .build();

        assertThat(edge,
                is("-[:Label {"
                        + "booleanProperty: false, "
                        + "numericProperty: 5.0, "
                        + "stringProperty: \"value\""
                        + "}]-"));
    }

    @Test
    public void createMinimalEdge() throws Exception {
        String edge = Edge.edge(BIDIRECTIONAL)
                .build();

        assertThat(edge, is("--"));
    }

    @Test
    public void createLeftDirectionalMinimalEdge() throws Exception {
        String edge = Edge.edge(LEFT)
                .build();

        assertThat(edge, is("<--"));
    }

    @Test
    public void createRightDirectionalMinimalEdge() throws Exception {
        String edge = Edge.edge(RIGHT)
                .build();

        assertThat(edge, is("-->"));
    }

    @Test
    public void createEdgeWithPropertyParameter() throws Exception {
        String edge = Edge.edge(BIDIRECTIONAL)
                .name("name")
                .propertyParameter("property", "parameter")
                .build();

        assertThat(edge, is("-[name {property: {parameter}}]-"));
    }

    @Test
    public void createLeftDirectionalEdge() throws Exception {
        String edge = Edge.edge(LEFT)
                .name("name")
                .label("Label")
                .build();

        assertThat(edge, is("<-[name:Label]-"));
    }

    @Test
    public void createRightDirectionalEdge() throws Exception {
        String edge = Edge.edge(RIGHT)
                .name("name")
                .label("Label")
                .build();

        assertThat(edge, is("-[name:Label]->"));
    }

    @Test
    public void createBoundedEdge() throws Exception {
        String edge = Edge.boundedPatternEdge(RIGHT, 1, 5)
                .name("name")
                .build();

        assertThat(edge, is("-[name*1..5]->"));
    }

    @Test
    public void createLeftBoundedEdge() throws Exception {
        String edge = Edge.leftBoundedPatternEdge(RIGHT, 1)
                .name("name")
                .build();

        assertThat(edge, is("-[name*1..]->"));
    }

    @Test
    public void createRightBoundedEdge() throws Exception {
        String edge = Edge.rightBoundedPatternEdge(RIGHT, 5)
                .name("name")
                .build();

        assertThat(edge, is("-[name*..5]->"));
    }

    @Test
    public void createFixedPatternEdge() throws Exception {
        String edge = Edge.fixedPatternEdge(RIGHT, 2)
                .name("name")
                .build();

        assertThat(edge, is("-[name*2]->"));
    }

    @Test
    public void createUnboundedEdge() throws Exception {
        String edge = Edge.unboundedPatternEdge(RIGHT)
                .name("name")
                .build();

        assertThat(edge, is("-[name*]->"));
    }
}
