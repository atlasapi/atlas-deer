package org.atlasapi.neo4j.service.cypher;

import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class NodeTest {

    @Test
    public void createNode() throws Exception {
        String node = Node.node()
                .name("name")
                .label("Label")
                .property("booleanProperty", false)
                .property("numericProperty", 5.0F)
                .property("stringProperty", "value")
                .build();

        assertThat(node,
                is("(name:Label {"
                        + "booleanProperty: false, "
                        + "numericProperty: 5.0, "
                        + "stringProperty: \"value\""
                        + "})"));
    }

    @Test
    public void createNodeWithoutProperties() throws Exception {
        String node = Node.node()
                .name("name")
                .label("Label")
                .build();

        assertThat(node, is("(name:Label)"));
    }

    @Test
    public void createNodeWithoutLabel() throws Exception {
        String node = Node.node()
                .name("name")
                .property("booleanProperty", false)
                .property("numericProperty", 5.0F)
                .property("stringProperty", "value")
                .build();

        assertThat(node,
                is("(name {"
                        + "booleanProperty: false, "
                        + "numericProperty: 5.0, "
                        + "stringProperty: \"value\""
                        + "})"));
    }

    @Test
    public void createNodeWithoutName() throws Exception {
        String node = Node.node()
                .label("Label")
                .property("booleanProperty", false)
                .property("numericProperty", 5.0F)
                .property("stringProperty", "value")
                .build();

        assertThat(node,
                is("(:Label {"
                        + "booleanProperty: false, "
                        + "numericProperty: 5.0, "
                        + "stringProperty: \"value\""
                        + "})"));
    }

    @Test
    public void createMinimalNode() throws Exception {
        String node = Node.node()
                .build();

        assertThat(node, is("()"));
    }

    @Test
    public void createNodeWithPropertyParameter() throws Exception {
        String node = Node.node()
                .name("name")
                .propertyParameter("property", "parameter")
                .build();

        assertThat(node, is("(name {property: {parameter}})"));
    }
}
